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
logging.basicConfig(level=logging.INFO)
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
def sync_candles(timeframe):
    """
    Syncs candle data including the most recent candle for all active tickers.
    
    Args:
        timeframe (str): The timeframe identifier ('M', '240', 'D', 'W')
    """
    logger.info(f"Starting candle sync for timeframe {timeframe}")
    active_tickers = get_active_tickers_from_db()
    if not active_tickers:
        logger.warning("No active tickers found. Skipping candle sync.")
        return

    connection = get_db_connection()
    cursor_db = connection.cursor()
    session = HTTP()

    try:
        for ticker in active_tickers:
            try:
                # Fetch the last 101 candles
                response = session.get_kline(symbol=ticker, interval=timeframe, limit=101)
                candles = response['result']['list']
                if len(candles) < 1:
                    continue

                # Process all candles including the most recent one
                for candle in candles:
                    timestamp_ms = int(candle[0])
                    timestamp_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                    o, h, l, c, volume = float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4]), float(candle[5])

                    # Check if candle already exists
                    cursor_db.execute(
                        "SELECT COUNT(*) FROM candles WHERE ticker = %s AND timestamp = %s AND timeframe = %s",
                        (ticker, timestamp_dt, timeframe)
                    )
                    if cursor_db.fetchone()[0] > 0:
                        continue

                    # Insert the candle into the database
                    insert_sql = """
                        INSERT INTO candles (ticker, timestamp, timeframe, o, h, l, c, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor_db.execute(insert_sql, (ticker, timestamp_dt, timeframe, o, h, l, c, volume))
                
                connection.commit()  # Commit after each ticker
                logger.info(f"Inserted candles for {ticker} on timeframe {timeframe}")
            except Exception as e:
                logger.error(f"Error syncing candles for {ticker} on timeframe {timeframe}: {e}")
    except Exception as e:
        logger.error(f"Error during candle sync: {e}")
    finally:
        cursor_db.close()
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

    cleanup_old_candles()

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

def analyze_support_resistance():
    """
    Analyzes candle data to identify supports and resistances across all tickers and timeframes,
    and logs when recent candles cross these levels.
    """
    logger.info("Starting support/resistance analysis")
    connection = get_db_connection()
    cursor_db = connection.cursor()
    timeframes = ['M', 'W']
    
    try:
        # Get all active tickers
        active_tickers = get_active_tickers_from_db()

        # Create a dictionary to store data for each ticker/timeframe combination
        market_data = {}

        # Structure: 
        # market_data[ticker][timeframe] = {
        #     'crosses': [],      # List of cross events
        #     'supports': [],     # List of current support levels
        #     'resistances': []   # List of current resistance levels
        # }

        # Initialize structure for each ticker and timeframe
        for ticker in active_tickers:
            market_data[ticker] = {}
            for timeframe in timeframes:
                market_data[ticker][timeframe] = {
                    'crosses': [],
                    'supports': [],
                    'resistances': []
                }
        
        for ticker in active_tickers:
            for timeframe in timeframes:
                # Get all candles for this ticker and timeframe, ordered by timestamp
                cursor_db.execute("""
                    SELECT timestamp, o, h, l, c 
                    FROM candles 
                    WHERE ticker = %s AND timeframe = %s 
                    ORDER BY timestamp DESC
                    LIMIT 100
                """, (ticker, timeframe))
                
                candles = cursor_db.fetchall()[::-1]
                if len(candles) < 4:  # Need at least 4 candles for analysis
                    continue
                
                last_candle_count = 1

                # Separate recent and historical candles
                recent_candles = candles[-last_candle_count:]  # Newest 3 candles
                historical_candles = candles[:-last_candle_count]  # All except last 3 candles
                
                # Find supports and resistances from historical candles
                supports = []
                resistances = []
                
                # Combine historical and recent candles for analysis
                all_candles = historical_candles + recent_candles
                for i in range(len(all_candles)-1, 0, -1):
                    curr_candle = all_candles[i]
                    prev_candle = all_candles[i-1]
                    
                    # Check if current candle is crossing any support/resistance
                    # Check if current candle crosses any existing support levels
                    for support in supports[:]:
                        if curr_candle[2] < support['level'] < curr_candle[3] and curr_candle not in recent_candles:
                            # Log support level cross (commented out for now)
                            #logger.info(f"""
                            #    Support crossed for {ticker} ({timeframe})
                            #    Time: {curr_candle[0]}
                            #    Support level: {support['level']} (from {support['timestamp']})
                            #    Crossing candle OHLC: {curr_candle[1]},{curr_candle[2]},{curr_candle[3]},{curr_candle[4]}
                            #""".strip())
                            supports.remove(support)
                    
                    # Check if current candle crosses any existing resistance levels  
                    for resistance in resistances[:]:
                        if curr_candle[2] < resistance['level'] < curr_candle[3] and curr_candle not in recent_candles:
                            # Log resistance level cross (commented out for now)
                            #logger.info(f"""
                            #    Resistance crossed for {ticker} ({timeframe})
                            #    Time: {curr_candle[0]}
                            #    Resistance level: {resistance['level']} (from {resistance['timestamp']})
                            #    Crossing candle OHLC: {curr_candle[1]},{curr_candle[2]},{curr_candle[3]},{curr_candle[4]}
                            #""".strip())
                            resistances.remove(resistance)
                    
                    # Check if current and previous candle create new support/resistance
                    if curr_candle[4] > prev_candle[4] and prev_candle[1] > prev_candle[4]:  # Support
                        support_level = min(curr_candle[1], prev_candle[4])
                        if support_level not in [s['level'] for s in supports]:
                            supports.append({
                                'level': support_level,
                                'timestamp': curr_candle[0]
                            })
                    
                    if curr_candle[4] < prev_candle[4] and prev_candle[1] < prev_candle[4]:  # Resistance
                        resistance_level = max(curr_candle[1], prev_candle[4])
                        if resistance_level not in [r['level'] for r in resistances]:
                            resistances.append({
                                'level': resistance_level,
                                'timestamp': curr_candle[0]
                            })
                
                # Check all candles after support/resistance was established for crosses
                def check_crosses(level_data, candles, is_support=True):
                    level_time = level_data['timestamp']
                    level = level_data['level']
                    
                    # Check historical candles (excluding 3 most recent)
                    for candle in historical_candles:
                        if candle[0] <= level_time:  # Skip candles before level was established
                            continue
                        if candle[2] > level > candle[3]:  # high > level > low
                            return False
                    return True

                # Filter out invalid supports/resistances
                supports = [s for s in supports if check_crosses(s, historical_candles, True)]
                resistances = [r for r in resistances if check_crosses(r, historical_candles, False)]
                crosses = []

                # Print supports and resistances for this ticker and timeframe
                # if supports or resistances:
                #     logger.info(f"\nSupport/Resistance levels for {ticker} ({timeframe}):")
                #     logger.info("Supports:")
                #     for support in supports:
                #         logger.info(f"  Level: {support['level']}, Established: {support['timestamp']}")
                #     logger.info("Resistances:")
                #     for resistance in resistances:
                #         logger.info(f"  Level: {resistance['level']}, Established: {resistance['timestamp']}")
                
                # Check if latest 3 candles cross any of the filtered support/resistance levels
                for candle in recent_candles:
                    for support in supports[:]: # Create a copy to safely remove during iteration
                        if candle[0] > support['timestamp'] and candle[2] >= support['level'] >= candle[3]:  # If candle crosses support
                            cross_info = {
                                'ticker': ticker,
                                'timeframe': timeframe, 
                                'type': 'support',
                                'time': candle[0],
                                'level': support['level'],
                                'established': support['timestamp'],
                                'candle': {'o': candle[1], 'h': candle[2], 'l': candle[3], 'c': candle[4]}
                            }
                            crosses.append(cross_info)
                            # logger.info(f"""
                            #     Recent support cross for {ticker} ({timeframe})
                            #     Time: {candle[0]}
                            #     Support level: {support['level']} (from {support['timestamp']})
                            #     Crossing candle OHLC: {candle[1]},{candle[2]},{candle[3]},{candle[4]}
                            # """.strip())
                            supports.remove(support)  # Remove crossed support
                            
                    for resistance in resistances[:]: # Create a copy to safely remove during iteration
                        if candle[0] > resistance['timestamp'] and candle[2] >= resistance['level'] >= candle[3]:  # If candle crosses resistance
                            cross_info = {
                                'ticker': ticker,
                                'timeframe': timeframe,
                                'type': 'resistance', 
                                'time': candle[0],
                                'level': resistance['level'],
                                'established': resistance['timestamp'],
                                'candle': {'o': candle[1], 'h': candle[2], 'l': candle[3], 'c': candle[4]}
                            }
                            crosses.append(cross_info)
                            # logger.info(f"""
                            #     Recent resistance cross for {ticker} ({timeframe})
                            #     Time: {candle[0]}
                            #     Resistance level: {resistance['level']} (from {resistance['timestamp']})
                            #     Crossing candle OHLC: {candle[1]},{candle[2]},{candle[3]},{candle[4]}
                            # """.strip())
                            resistances.remove(resistance)  # Remove crossed resistance

                    # Store the analysis results in market_data
                    market_data[ticker][timeframe]['supports'] = supports
                    market_data[ticker][timeframe]['resistances'] = resistances
                    market_data[ticker][timeframe]['crosses'] = crosses
        # Find tickers with crosses in both M and W timeframes
        for ticker in market_data:
            m_crosses = market_data[ticker]['M']['crosses']
            w_crosses = market_data[ticker]['W']['crosses']
            
            if len(m_crosses) > 0 and len(w_crosses) > 0:
                logger.info(f"\nTicker {ticker} has crosses in both M and W timeframes:")
                logger.info("Monthly crosses:")
                for cross in m_crosses:
                    logger.info(f"  {cross['type'].title()} cross at {cross['time']}, level: {cross['level']}")
                logger.info("Weekly crosses:")
                for cross in w_crosses:
                    logger.info(f"  {cross['type'].title()} cross at {cross['time']}, level: {cross['level']}")
                
    except Exception as e:
        logger.error(f"Error during support/resistance analysis: {e}")
        traceback.print_exc()
    finally:
        cursor_db.close()
        connection.close()
    
    logger.info("Completed support/resistance analysis")

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
    #scheduler.add_job(weekly_sync, 'cron', day_of_week='mon', hour=1, minute=0, id='weekly_sync')

    # Schedule candle sync jobs for different timeframes:
    #scheduler.add_job(sync_candles, 'cron', args=['M'], day=1, hour=0, minute=1, id='sync_M')  # Monthly candles
    #scheduler.add_job(sync_candles, 'cron', args=['240'], hour='*/4', minute=1, id='sync_240')  # 4-hour candles
    #scheduler.add_job(sync_candles, 'cron', args=['D'], hour=0, minute=1, id='sync_D')    # Daily candles
    #scheduler.add_job(sync_candles, 'cron', args=['W'], day_of_week='mon', hour=0, minute=1, id='sync_W')  # Weekly candles

    #weekly_sync()
    #sync_candles('M')
    #sync_candles('W')
    #sync_candles('D')
    #sync_candles('240')
    analyze_support_resistance()

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
