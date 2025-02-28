"""
This script synchronizes perpetual futures tickers and their candle data from Bybit using the pybit library,
and stores the data into a MariaDB database.

Database details:
    - MariaDB host: 192.168.2.168
    - Database: invest
    - Tables:
        * crypto_tickers: columns: ticker, state (active/inactive)
        * candles: columns: ticker, timestamp, timeframe, o, h, l, c, volume

Scheduled tasks:
    - Every week: update the crypto_tickers table with active tickers from Bybit and delete candles for inactive tickers.
    - For each timeframe, a separate job is scheduled (e.g. daily candles run each day after the candle has closed).
"""

import logging
import mysql.connector
from datetime import datetime, timezone
from pybit.unified_trading import HTTP
from apscheduler.schedulers.blocking import BlockingScheduler
import traceback
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, wait_exponential, stop_after_attempt
import os
import zipfile
from logging.handlers import RotatingFileHandler

# --------------------------
# Database Configuration
# --------------------------
DB_HOST = '192.168.2.168'
DB_DATABASE = 'invest'
DB_USER = 'root'       # <<== Replace with your DB username
DB_PASSWORD = 'dandy'   # <<== Replace with your DB password

# --------------------------
# Logging Configuration
# --------------------------
class ZipRotatingFileHandler(RotatingFileHandler):
    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        if os.path.exists(self.baseFilename):
            # Create zip backup
            timestr = datetime.now().strftime('%Y%m%d_%H%M%S')
            zipname = f'crypto_server_{timestr}.zip'
            with zipfile.ZipFile(zipname, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(self.baseFilename, os.path.basename(self.baseFilename))
            os.remove(self.baseFilename)
        self.mode = 'w'
        self.stream = self._open()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        ZipRotatingFileHandler('crypto_server.log', maxBytes=10*1024*1024, backupCount=5),  # 10MB
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --------------------------
# Database Connection Helper
# --------------------------
def get_db_connection():
    """
    Establish and return a connection to the MariaDB database.
    """
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE
        )
        return connection
    except mysql.connector.Error as err:
        logger.error(f"Error connecting to database: {err}")
        raise

# --------------------------
# Ticker Sync Functions
# --------------------------
def update_crypto_tickers():
    """
    Downloads active perpetual futures tickers from Bybit and updates the crypto_tickers table.
    Only instruments with contractType 'LinearPerpetual' and status 'Trading' are considered.
    """
    logger.info("Downloading perpetual futures tickers from Bybit")
    session = HTTP()
    instruments = []
    cursor = None
    
    # Paginate through instruments
    while True:
        response = session.get_instruments_info(
            category="linear",
            limit=1000,
            cursor=cursor
        )
        valid_contracts = [
            item for item in response['result']['list']
            if item['contractType'] == 'LinearPerpetual' and item['status'] == 'Trading'
        ]
        instruments.extend(valid_contracts)
        cursor = response['result'].get('nextPageCursor')
        if not cursor:
            break

    # Update the crypto_tickers table in the database.
    connection = get_db_connection()
    cursor_db = connection.cursor()
    try:
        # Clear existing tickers
        cursor_db.execute("TRUNCATE TABLE crypto_tickers")
        # Insert each active ticker into the table
        for instrument in instruments:
            symbol = instrument['symbol']
            state = "active" if instrument['status'] == 'Trading' else 'inactive'
            cursor_db.execute(
                "INSERT INTO crypto_tickers (ticker, state) VALUES (%s, %s)",
                (symbol, state)
            )
        connection.commit()
        logger.info(f"Updated crypto_tickers table with {len(instruments)} active tickers.")
    except Exception as e:
        logger.error(f"Error updating crypto_tickers table: {e}")
    finally:
        cursor_db.close()
        connection.close()
    logger.info("Finished downloading perpetual futures tickers from Bybit")

def cleanup_candles():
    """
    Cleans up the candles table by deleting records for tickers that are not active.
    This ensures that the database only retains candle data for tickers currently trading.
    """
    connection = get_db_connection()
    cursor_db = connection.cursor()
    try:
        # Delete candles where the ticker is not present in the active crypto_tickers table.
        cursor_db.execute(
            "DELETE FROM candles WHERE ticker NOT IN (SELECT ticker FROM crypto_tickers)"
        )
        connection.commit()
        logger.info("Cleaned up candles table by removing inactive tickers.")
    except Exception as e:
        logger.error(f"Error cleaning up candles table: {e}")
    finally:
        cursor_db.close()
        connection.close()

def get_active_tickers_from_db():
    """
    Retrieves and returns a list of active ticker symbols from the crypto_tickers table.
    """
    connection = get_db_connection()
    cursor_db = connection.cursor()
    active_tickers = []
    try:
        cursor_db.execute("SELECT ticker FROM crypto_tickers WHERE state = 'active'")
        rows = cursor_db.fetchall()
        active_tickers = [row[0] for row in rows]
    except Exception as e:
        logger.error(f"Error fetching active tickers: {e}")
    finally:
        cursor_db.close()
        connection.close()
    return active_tickers

# --------------------------
# Candle Sync Function
# --------------------------
@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(3))
def safe_fetch_kline(session, symbol, timeframe):
    """
    Safely fetch kline data with retry logic for rate limiting
    """
    response = session.get_kline(
        symbol=symbol,
        interval=timeframe,
        limit=101
    )
    if 'ret_code' in response and response['ret_code'] == 10006:
        raise Exception("Rate limit exceeded")
    return response

def process_candles(data, connection, timeframe):
    """
    Process and store candle data for a single symbol
    """
    cursor_db = connection.cursor()
    try:
        ticker = data['result']['symbol']
        candles = data['result']['list']
        
        for candle in candles:
            timestamp_ms = int(candle[0])
            timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            o, h, l, c, volume = float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4]), float(candle[5])

            # Check if candle exists
            cursor_db.execute(
                "SELECT COUNT(*) FROM candles WHERE ticker = %s AND timestamp = %s AND timeframe = %s",
                (ticker, timestamp_dt, timeframe)
            )
            if cursor_db.fetchone()[0] > 0:
                continue

            # Insert candle
            insert_sql = """
                INSERT INTO candles (ticker, timestamp, timeframe, o, h, l, c, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor_db.execute(insert_sql, (ticker, timestamp_dt, timeframe, o, h, l, c, volume))
        
        connection.commit()
        logger.info(f"Inserted candles for {ticker} on timeframe {timeframe}")
    except Exception as e:
        logger.error(f"Error processing candles for {ticker}: {e}")
    finally:
        cursor_db.close()

def sync_candles(timeframe):
    """
    Syncs candle data using parallel processing with rate limiting
    """
    logger.info(f"Starting candle sync for timeframe {timeframe}")
    active_tickers = get_active_tickers_from_db()
    if not active_tickers:
        logger.warning("No active tickers found. Skipping candle sync.")
        return

    connection = get_db_connection()
    session = HTTP()

    try:
        # Fetch data in parallel with rate limiting
        with ThreadPoolExecutor(max_workers=5) as executor:
            fetch_tasks = {
                executor.submit(safe_fetch_kline, session, ticker, timeframe): ticker 
                for ticker in active_tickers
            }
            
            # Process results as they complete
            for future in fetch_tasks:
                try:
                    data = future.result()
                    if data and 'result' in data:
                        process_candles(data, connection, timeframe)
                except Exception as e:
                    ticker = fetch_tasks[future]
                    logger.error(f"Failed to fetch/process data for {ticker}: {e}")

    except Exception as e:
        logger.error(f"Error during candle sync: {e}")
    finally:
        connection.close()
    
    logger.info(f"Finished candle sync for timeframe {timeframe}")

def cleanup_old_candles():
    """
    Cleans up the candles table by retaining only the last 100 candles for each ticker and timeframe.
    """
    connection = get_db_connection()
    cursor_db = connection.cursor()
    try:
        # Get all unique ticker and timeframe combinations
        cursor_db.execute("SELECT DISTINCT ticker, timeframe FROM candles")
        ticker_timeframes = cursor_db.fetchall()

        for ticker, timeframe in ticker_timeframes:
            # Delete candles older than the 100 most recent ones for each ticker and timeframe
            cursor_db.execute(
                """
                DELETE FROM candles
                WHERE ticker = %s AND timeframe = %s AND timestamp NOT IN (
                    SELECT timestamp FROM (
                        SELECT timestamp FROM candles
                        WHERE ticker = %s AND timeframe = %s
                        ORDER BY timestamp DESC
                        LIMIT 100
                    ) AS subquery
                )
                """,
                (ticker, timeframe, ticker, timeframe)
            )
        connection.commit()
        logger.info("Cleaned up old candles, retaining only the last 100 candles for each ticker and timeframe.")
    except Exception as e:
        logger.error(f"Error cleaning up old candles: {e}")
    finally:
        cursor_db.close()
        connection.close()

    

# --------------------------
# Weekly Full Sync Function
# --------------------------
def weekly_sync():
    """
    Performs a weekly synchronization by updating the crypto_tickers table and cleaning up
    the candles table. This ensures that only active tickers and their corresponding candles are stored.
    """
    logger.info("Starting weekly sync of crypto tickers and candle cleanup")
    update_crypto_tickers()
    cleanup_candles()
    logger.info("Weekly sync complete")

# --------------------------
# Main Scheduler Setup
# --------------------------
def main():
    """
    Sets up and starts the scheduler to run synchronization tasks:
      - Weekly: update tickers and cleanup candles.
      - Regularly for each timeframe:
            * 'M' (minute candles): every minute (run at 10 seconds past each minute)
            * '240' (4-hour candles): every 4 hours (run at 5 minutes past the hour)
            * 'D' (daily candles): every day (run at 00:05 UTC)
            * 'W' (weekly candles): every Monday (run at 00:10 UTC)
    """
    scheduler = BlockingScheduler()

    # Schedule the weekly sync (every Monday at 01:00 UTC)
    scheduler.add_job(weekly_sync, 'cron', day_of_week='mon', hour=1, minute=0, id='weekly_sync')

    # Schedule candle sync jobs for different timeframes:
    scheduler.add_job(sync_candles, 'cron', args=['M'], day=1, hour=0, minute=1, id='sync_M')  # Monthly candles
    scheduler.add_job(sync_candles, 'cron', args=['W'], day_of_week='mon', hour=0, minute=1, id='sync_W')  # Weekly candles
    scheduler.add_job(sync_candles, 'cron', args=['D'], hour=0, minute=1, id='sync_D')    # Daily candles
    scheduler.add_job(sync_candles, 'cron', args=['240'], hour='1/4', minute=1, id='sync_240')  # 4-hour candles
    scheduler.add_job(sync_candles, 'cron', args=['60'], hour='0/1', minute=1, id='sync_60')  # 1-hour candles
    scheduler.add_job(sync_candles, 'cron', args=['15'], minute='0/15', second=10, id='sync_15')  # 15-min candles

    logger.info("Scheduler started. Running tasks 24/7.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

# --------------------------
# Script Entry Point
# --------------------------
if __name__ == '__main__':
    main()
