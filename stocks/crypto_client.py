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
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression

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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_client.log', mode='w'),
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

def get_candles_data(tickers, timeframes):
    """
    Retrieves all candle data for given tickers and timeframes from the database.
    
    Args:
        tickers (list): List of ticker symbols
        timeframes (list): List of timeframe strings
        
    Returns:
        dict: Nested dictionary with structure:
            {ticker: {timeframe: [(timestamp, o, h, l, c, volume), ...], ...}, ...}
    """
    logger.info("Loading candle data from database...")
    connection = get_db_connection()
    cursor_db = connection.cursor()
    candles_data = {}

    try:
        for ticker in tickers:
            candles_data[ticker] = {}
            for timeframe in timeframes:
                cursor_db.execute("""
                    SELECT timestamp, o, h, l, c, volume 
                    FROM candles 
                    WHERE ticker = %s AND timeframe = %s 
                    ORDER BY timestamp ASC
                """, (ticker, timeframe))
                candles_data[ticker][timeframe] = cursor_db.fetchall()
                
    except Exception as e:
        logger.error(f"Error loading candle data: {e}")
        traceback.print_exc()
    finally:
        cursor_db.close()
        connection.close()
        
    return candles_data


def analyze_support_resistance(tickers, timeframes, candles_data):
    """
    Analyzes candle data to identify supports and resistances across specified tickers and timeframes,
    and logs when recent candles cross these levels.
    
    Args:
        tickers (list): List of ticker symbols to analyze
        timeframes (list): List of timeframes to analyze 
        candles_data: Dictionary containing candle data in format:
            {ticker: {timeframe: [(timestamp, o, h, l, c, volume), ...], ...}, ...}
    """
    logger.info("Starting support/resistance analysis")
    
    # Create a dictionary to store data for each ticker/timeframe combination
    market_data = {}
    
    # Structure: 
    # market_data[ticker][timeframe] = {
    #     'crosses': [],      # List of cross events
    #     'supports': [],     # List of current support levels
    #     'resistances': []   # List of current resistance levels
    # }

    try:
        # Initialize structure for each ticker and timeframe
        for ticker in tickers:
            market_data[ticker] = {}
            for timeframe in timeframes:
                market_data[ticker][timeframe] = {
                    'crosses': [],
                    'supports': [],
                    'resistances': []
                }
        
        for ticker in tickers:
            for timeframe in timeframes:
                if ticker not in candles_data or timeframe not in candles_data[ticker]:
                    continue
                    
                candles = candles_data[ticker][timeframe][-100:]  # Get last 100 candles
                if len(candles) < 4:  # Need at least 4 candles for analysis
                    continue
                
                last_candle_count = 1
                
                # Separate recent and historical candles
                recent_candles = candles[-last_candle_count:]  # Newest candle
                historical_candles = candles[:-last_candle_count]  # All except last candle
                
                # Find supports and resistances from historical candles
                supports = []
                resistances = []
                
                # Combine historical and recent candles for analysis
                all_candles = historical_candles + recent_candles
                for i in range(len(all_candles)-1, 0, -1):
                    curr_candle = all_candles[i]
                    prev_candle = all_candles[i-1]
                    
                    # Check if current candle crosses any existing support levels
                    for support in supports[:]:
                        if curr_candle[2] < support['level'] < curr_candle[3] and curr_candle not in recent_candles:
                            supports.remove(support)
                    
                    # Check if current candle crosses any existing resistance levels  
                    for resistance in resistances[:]:
                        if curr_candle[2] < resistance['level'] < curr_candle[3] and curr_candle not in recent_candles:
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
                    
                    # Check historical candles
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
                
                # Check if latest candles cross any of the filtered support/resistance levels
                for candle in recent_candles:
                    for support in supports[:]:
                        if candle[0] > support['timestamp'] and candle[2] >= support['level'] >= candle[3]:
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
                            supports.remove(support)
                            
                    for resistance in resistances[:]:
                        if candle[0] > resistance['timestamp'] and candle[2] >= resistance['level'] >= candle[3]:
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
                            resistances.remove(resistance)

                    # Store the analysis results in market_data
                    market_data[ticker][timeframe]['supports'] = supports
                    market_data[ticker][timeframe]['resistances'] = resistances
                    market_data[ticker][timeframe]['crosses'] = crosses
        
        return market_data
                
    except Exception as e:
        logger.error(f"Error during support/resistance analysis: {e}")
        traceback.print_exc()
    
    logger.info("Completed support/resistance analysis")

def analyze_price_action(tickers, timeframes, candles_data):
    """
    Analyzes candle data to identify price action patterns (bullish/bearish continuations)
    between adjacent candles in the newest 4 candles.
    
    Args:
        tickers (list): List of ticker symbols to analyze
        timeframes (list): List of timeframe strings
        candles_data (dict): Nested dictionary with structure:
            {ticker: {timeframe: [(timestamp, o, h, l, c, volume), ...], ...}, ...}
    """
    logger.info("Starting price action analysis")
    
    try:
        price_action_data = {}

        # Initialize data structure
        for ticker in tickers:
            price_action_data[ticker] = {}
            for timeframe in timeframes:
                price_action_data[ticker][timeframe] = {
                    'patterns': []
                }

        for ticker in tickers:
            for timeframe in timeframes:
                if ticker not in candles_data or timeframe not in candles_data[ticker]:
                    continue
                    
                # Get newest 4 candles
                candles = candles_data[ticker][timeframe][-4:]
                if len(candles) < 2:  # Need at least 2 candles for analysis
                    continue

                # Analyze adjacent candles
                for i in range(len(candles)-1):
                    current_candle = candles[i]
                    next_candle = candles[i+1]
                    
                    # Check for bullish continuation
                    if next_candle[4] > current_candle[2]:  # next close > current high
                        pattern = {
                            'type': 'BULL',
                            'time_first': current_candle[0],
                            'time_second': next_candle[0],
                            'first_candle': {
                                'o': current_candle[1],
                                'h': current_candle[2],
                                'l': current_candle[3],
                                'c': current_candle[4]
                            },
                            'second_candle': {
                                'o': next_candle[1],
                                'h': next_candle[2],
                                'l': next_candle[3],
                                'c': next_candle[4]
                            }
                        }
                        price_action_data[ticker][timeframe]['patterns'].append(pattern)
                    
                    # Check for bearish continuation
                    elif next_candle[4] < current_candle[3]:  # next close < current low
                        pattern = {
                            'type': 'BEAR',
                            'time_first': current_candle[0],
                            'time_second': next_candle[0],
                            'first_candle': {
                                'o': current_candle[1],
                                'h': current_candle[2],
                                'l': current_candle[3],
                                'c': current_candle[4]
                            },
                            'second_candle': {
                                'o': next_candle[1],
                                'h': next_candle[2],
                                'l': next_candle[3],
                                'c': next_candle[4]
                            }
                        }
                        price_action_data[ticker][timeframe]['patterns'].append(pattern)

        return price_action_data

    except Exception as e:
        logger.error(f"Error during price action analysis: {e}")
        traceback.print_exc()
    
    logger.info("Completed price action analysis")



def print_situations():

    support_resistance_data = []
    price_action_data = []
    support_resistance_data = analyze_support_resistance()
    price_action_data = analyze_price_action()

    # Find tickers with crosses in both M and W timeframes
    #for ticker in support_resistance_data:
    #    m_crosses = support_resistance_data[ticker]['M']['crosses']
    #    w_crosses = support_resistance_data[ticker]['W']['crosses']
        
    #    if len(m_crosses) > 0 and len(w_crosses) > 0:
    #        logger.info(f"\nTicker {ticker} has crosses in both M and W timeframes:")
    #        logger.info("Monthly crosses:")
    #        for cross in m_crosses:
    #            logger.info(f"  {cross['type'].title()} cross at {cross['time']}, level: {cross['level']}")
    #        logger.info("Weekly crosses:")
    #        for cross in w_crosses:
    #            logger.info(f"  {cross['type'].title()} cross at {cross['time']}, level: {cross['level']}")

    high_timeframe = 'D'
    low_timeframe = '240'

    # Find tickers with support crosses in M/W and bullish patterns
    for ticker in support_resistance_data:
        m_crosses = support_resistance_data[ticker][high_timeframe]['crosses']
        w_crosses = support_resistance_data[ticker][low_timeframe]['crosses']
        
        # Check if ticker has support crosses in M or W
        m_support_crosses = [c for c in m_crosses if c['type'] == 'support']
        w_support_crosses = [c for c in w_crosses if c['type'] == 'support']
        
        # Check if ticker has bullish patterns in M or W
        m_bull_patterns = []
        w_bull_patterns = []
        if ticker in price_action_data:
            m_bull_patterns = [p for p in price_action_data[ticker][high_timeframe]['patterns'] if p['type'] == 'BULL']
            w_bull_patterns = [p for p in price_action_data[ticker][low_timeframe]['patterns'] if p['type'] == 'BULL']

        # Log if ticker has both support crosses and bullish patterns
        if (m_support_crosses and w_support_crosses) and (m_bull_patterns or w_bull_patterns):
            logger.info(f"\nSignificant signals for {ticker}:")
            
            if m_support_crosses:
                logger.info("Monthly support crosses:")
                for cross in m_support_crosses:
                    logger.info(f"  Support cross at {cross['time']}, level: {cross['level']}")
            
            if w_support_crosses:
                logger.info("Weekly support crosses:")
                for cross in w_support_crosses:
                    logger.info(f"  Support cross at {cross['time']}, level: {cross['level']}")
            
            if m_bull_patterns:
                logger.info("Monthly bullish patterns:")
                for pattern in m_bull_patterns:
                    logger.info(f"  Bull pattern between {pattern['time_first']} and {pattern['time_second']}")
            
            if w_bull_patterns:
                logger.info("Weekly bullish patterns:")
                for pattern in w_bull_patterns:
                    logger.info(f"  Bull pattern between {pattern['time_first']} and {pattern['time_second']}")

    logger.info("================================================================================================================")
    logger.info("================================================================================================================")
    logger.info("================================================================================================================")


    for ticker in support_resistance_data:
        m_crosses = support_resistance_data[ticker][high_timeframe]['crosses']
        w_crosses = support_resistance_data[ticker][low_timeframe]['crosses']
        
        # Check if ticker has support crosses in M or W
        m_resistance_crosses = [c for c in m_crosses if c['type'] == 'resistance']
        w_resistance_crosses = [c for c in w_crosses if c['type'] == 'resistance']
        
        # Check if ticker has bullish patterns in M or W
        m_bear_patterns = []
        w_bear_patterns = []
        if ticker in price_action_data:
            m_bear_patterns = [p for p in price_action_data[ticker][high_timeframe]['patterns'] if p['type'] == 'BEAR']
            w_bear_patterns = [p for p in price_action_data[ticker][low_timeframe]['patterns'] if p['type'] == 'BEAR']

        # Log if ticker has both support crosses and bullish patterns
        if (m_resistance_crosses and w_resistance_crosses) and (m_bear_patterns or w_bear_patterns):
            logger.info(f"\nSignificant signals for {ticker}:")
            
            if m_resistance_crosses:
                logger.info("Monthly resistance crosses:")
                for cross in m_resistance_crosses:
                    logger.info(f"  Resistance cross at {cross['time']}, level: {cross['level']}")
            
            if w_resistance_crosses:
                logger.info("Weekly resistance crosses:")
                for cross in w_support_crosses:
                    logger.info(f"  Resistance cross at {cross['time']}, level: {cross['level']}")
            
            if m_bear_patterns:
                logger.info("Monthly bearish patterns:")
                for pattern in m_bear_patterns:
                    logger.info(f"  Bear pattern between {pattern['time_first']} and {pattern['time_second']}")
            
            if w_bear_patterns:
                logger.info("Weekly bearish patterns:")
                for pattern in w_bear_patterns:
                    logger.info(f"  Bear pattern between {pattern['time_first']} and {pattern['time_second']}")

def print_downtrend_tickers():
    connection = get_db_connection()
    cursor_db = connection.cursor()
    active_tickers = get_active_tickers_from_db()

    logger.info("\nTickers with latest monthly close in bottom 10% but with overall uptrend:")

    for ticker in active_tickers:
        try:
            # Get all monthly candles for this ticker
            cursor_db.execute("""
                SELECT h, l, c, timestamp 
                FROM candles 
                WHERE ticker = %s AND timeframe = 'M'
                ORDER BY timestamp ASC
            """, (ticker,))
            
            candles = cursor_db.fetchall()
            if len(candles) < 6:  # Need at least 6 months of data
                continue

            # Calculate historical price range with float conversion
            all_highs = [float(c[0]) for c in candles]
            all_lows = [float(c[1]) for c in candles]
            all_closes = [float(c[2]) for c in candles]
            historical_high = max(all_highs)
            historical_low = min(all_lows)
            price_range = historical_high - historical_low
            
            # Get latest close with float conversion
            latest_close = float(candles[-1][2])
            
            # Check if latest close is in bottom 10% of range
            bottom_threshold = historical_low + (price_range * 0.1)
            
            # Check for long-term uptrend by comparing averages
            early_period = all_closes[:len(all_closes)//2]  # First half of data
            late_period = all_closes[len(all_closes)//2:-3]  # Second half excluding last 3 months
            
            early_avg = sum(early_period) / len(early_period)
            late_avg = sum(late_period) / len(late_period)
            
            # If in bottom 10% AND late average is higher than early average (uptrend)
            if latest_close <= bottom_threshold and late_avg > early_avg * 1.1:  # 10% higher for clear uptrend
                percent_from_bottom = ((latest_close - historical_low) / price_range) * 100
                uptrend_strength = ((late_avg - early_avg) / early_avg) * 100
                logger.info(f"{ticker}: {percent_from_bottom:.1f}% from bottom, {uptrend_strength:.1f}% long-term growth")
                logger.info(f"  Range: {historical_low:.4f} - {historical_high:.4f}")
                logger.info(f"  Early avg: {early_avg:.4f}, Late avg: {late_avg:.4f}")

        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")

    cursor_db.close()
    connection.close()

def find_trend_crosses(tickers, timeframes, candles_data):
    """
    Finds tickers in long-term trends that recently crossed key levels:
    - Downtrend tickers crossing resistance
    - Uptrend tickers crossing support
    """
    logger.info("\nAnalyzing trend tickers with level crosses...")
    
    # Get analysis data
    sr_data = analyze_support_resistance(tickers, timeframes, candles_data)
    pa_data = analyze_price_action(tickers, timeframes, candles_data)
    
    connection = get_db_connection()
    cursor_db = connection.cursor()
    active_tickers = get_active_tickers_from_db()

    try:
        for ticker in active_tickers:
            # Get monthly candles for trend analysis
            cursor_db.execute("""
                SELECT h, l, c, timestamp 
                FROM candles 
                WHERE ticker = %s AND timeframe = 'M'
                ORDER BY timestamp ASC
            """, (ticker,))
            
            candles = cursor_db.fetchall()
            if len(candles) < 6:  # Need at least 6 months
                continue

            # Calculate trend
            all_closes = [float(c[2]) for c in candles]
            early_period = all_closes[:len(all_closes)//2]
            late_period = all_closes[len(all_closes)//2:-2]
            
            early_avg = sum(early_period) / len(early_period)
            late_avg = sum(late_period) / len(late_period)
            
            # Get crosses from both timeframes
            m_crosses = sr_data[ticker]['M']['crosses'] if ticker in sr_data else []
            w_crosses = sr_data[ticker]['W']['crosses'] if ticker in sr_data else []

            # Check downtrend with resistance crosses
            if late_avg < early_avg * 0.9:  # Downtrend
                resistance_crosses = []
                for crosses in [m_crosses, w_crosses]:
                    for cross in crosses:
                        if cross['type'] == 'resistance':
                            resistance_crosses.append(cross)
                
                if resistance_crosses:
                    trend_strength = ((early_avg - late_avg) / early_avg) * 100
                    logger.info(f"\nDOWNTREND ticker with resistance cross: {ticker}")
                    logger.info(f"Downtrend strength: {trend_strength:.1f}%")
                    logger.info(f"Early avg: {early_avg:.4f}, Late avg: {late_avg:.4f}")
                    
                    for cross in resistance_crosses:
                        logger.info(f"Resistance cross details:")
                        logger.info(f"  Timeframe: {cross['timeframe']}")
                        logger.info(f"  Time: {cross['time']}")
                        logger.info(f"  Level: {cross['level']}")
                        logger.info(f"  Candle: O:{cross['candle']['o']} H:{cross['candle']['h']} L:{cross['candle']['l']} C:{cross['candle']['c']}")

            # Check uptrend with support crosses
            elif late_avg > early_avg * 1.1:  # Uptrend
                support_crosses = []
                for crosses in [m_crosses, w_crosses]:
                    for cross in crosses:
                        if cross['type'] == 'support':
                            support_crosses.append(cross)
                
                if support_crosses:
                    trend_strength = ((late_avg - early_avg) / early_avg) * 100
                    logger.info(f"\nUPTREND ticker with support cross: {ticker}")
                    logger.info(f"Uptrend strength: {trend_strength:.1f}%")
                    logger.info(f"Early avg: {early_avg:.4f}, Late avg: {late_avg:.4f}")
                    
                    for cross in support_crosses:
                        logger.info(f"Support cross details:")
                        logger.info(f"  Timeframe: {cross['timeframe']}")
                        logger.info(f"  Time: {cross['time']}")
                        logger.info(f"  Level: {cross['level']}")
                        logger.info(f"  Candle: O:{cross['candle']['o']} H:{cross['candle']['h']} L:{cross['candle']['l']} C:{cross['candle']['c']}")

    except Exception as e:
        logger.error(f"Error in find_trend_crosses: {e}")
        traceback.print_exc()
    finally:
        cursor_db.close()
        connection.close()

def find_aligned_patterns(tickers, timeframes, candles_data):
    """
    Finds tickers that show aligned bullish or bearish patterns across specified timeframes
    in their most recent patterns.
    """
    logger.info("\nAnalyzing tickers for aligned patterns across timeframes...")
    price_action_data = analyze_price_action(tickers, timeframes, candles_data)
    
    # Find bullish and bearish alignments
    bull_aligned = []
    bear_aligned = []
    
    for ticker in price_action_data:
        # Get latest patterns for each timeframe
        patterns_by_timeframe = {}
        for tf in timeframes:
            if tf in price_action_data[ticker]:
                patterns = price_action_data[ticker][tf]['patterns']
                if patterns:  # Only store if patterns exist
                    patterns_by_timeframe[tf] = patterns[-1]  # Get last pattern
        
        # Skip if we don't have patterns for all timeframes
        if len(patterns_by_timeframe) != len(timeframes):
            continue
            
        # Check for bullish alignment
        if all(p['type'] == 'BULL' for p in patterns_by_timeframe.values()):
            bull_aligned.append({
                'ticker': ticker,
                'patterns': patterns_by_timeframe
            })
            
        # Check for bearish alignment
        if all(p['type'] == 'BEAR' for p in patterns_by_timeframe.values()):
            bear_aligned.append({
                'ticker': ticker,
                'patterns': patterns_by_timeframe
            })
    
    # Print results
    #if bull_aligned:
    #    logger.info(f"\nTickers with bullish alignment across {'/'.join(timeframes)} timeframes:")
    #    for entry in bull_aligned:
    #        logger.info(f"\n{entry['ticker']}:")
    #        for tf in timeframes:
    #            pattern = entry['patterns'][tf]
    #            logger.info(f"{tf}: {pattern['time_first']} to {pattern['time_second']}")
    
    #if bear_aligned:
    #    logger.info(f"\nTickers with bearish alignment across {'/'.join(timeframes)} timeframes:")
    #    for entry in bear_aligned:
    #        logger.info(f"\n{entry['ticker']}:")
    #        for tf in timeframes:
    #            pattern = entry['patterns'][tf]
    #            logger.info(f"{tf}: {pattern['time_first']} to {pattern['time_second']}")

    if not (bull_aligned or bear_aligned):
        logger.info("No tickers found with aligned patterns across timeframes.")

    return [bull_aligned, bear_aligned]

def find_pattern_sequence(tickers, timeframes, candles_data):
    """
    Finds tickers with specific sequences of events in all timeframes:
    - Bullish sequence: HT support cross -> LT support cross -> LT bullish pattern
    - Bearish sequence: HT resistance cross -> LT resistance cross -> LT bearish pattern
    """
    logger.info("\nAnalyzing pattern sequences...")
    
    # Get analysis data
    sr_data = analyze_support_resistance(tickers, timeframes, candles_data)
    pa_data = analyze_price_action(tickers, timeframes, candles_data)
    
    # Define all timeframe pairs (HT, LT)
    timeframe_pairs = [
        ('M', 'W'),
        ('W', 'D'),
        ('D', '240'),
        ('240', '60'),
        ('60', '15')
    ]
    
    for HT, LT in timeframe_pairs:
        logger.info(f"\nAnalyzing {HT}/{LT} timeframe pair:")
        bullish_matches = []
        bearish_matches = []
        
        for ticker in sr_data:
            # Get crosses and patterns for this ticker
            ht_crosses = sr_data[ticker][HT]['crosses']
            lt_crosses = sr_data[ticker][LT]['crosses']
            lt_patterns = pa_data[ticker][LT]['patterns'] if ticker in pa_data else []
            
            # Check for bullish sequence
            ht_support_crosses = [c for c in ht_crosses if c['type'] == 'support']
            lt_support_crosses = [c for c in lt_crosses if c['type'] == 'support']
            lt_bull_patterns = [p for p in lt_patterns if p['type'] == 'BULL']
            
            if ht_support_crosses and lt_support_crosses and lt_bull_patterns:
                latest_ht_cross = ht_support_crosses[-1]
                latest_lt_cross = lt_support_crosses[-1]
                latest_lt_bull = lt_bull_patterns[-1]
                
                # Check sequence timing
                ht_time = latest_ht_cross['time']
                lt_time = latest_lt_cross['time']
                pattern_time = latest_lt_bull['time_second']
                
                if ht_time < lt_time < pattern_time:
                    bullish_matches.append({
                        'ticker': ticker,
                        'ht_cross': latest_ht_cross,
                        'lt_cross': latest_lt_cross,
                        'pattern': latest_lt_bull
                    })
            
            # Check for bearish sequence
            ht_resistance_crosses = [c for c in ht_crosses if c['type'] == 'resistance']
            lt_resistance_crosses = [c for c in lt_crosses if c['type'] == 'resistance']
            lt_bear_patterns = [p for p in lt_patterns if p['type'] == 'BEAR']
            
            if ht_resistance_crosses and lt_resistance_crosses and lt_bear_patterns:
                latest_ht_cross = ht_resistance_crosses[-1]
                latest_lt_cross = lt_resistance_crosses[-1]
                latest_lt_bear = lt_bear_patterns[-1]
                
                # Check sequence timing
                ht_time = latest_ht_cross['time']
                lt_time = latest_lt_cross['time']
                pattern_time = latest_lt_bear['time_second']
                
                if ht_time < lt_time < pattern_time:
                    bearish_matches.append({
                        'ticker': ticker,
                        'ht_cross': latest_ht_cross,
                        'lt_cross': latest_lt_cross,
                        'pattern': latest_lt_bear
                    })
        
        # Print results for this timeframe pair
        if bullish_matches:
            logger.info(f"\nFound {len(bullish_matches)} tickers with bullish sequence ({HT} support -> {LT} support -> {LT} bull):")
            for match in bullish_matches:
                logger.info(f"\n{match['ticker']}:")
                logger.info(f"  {HT} support cross at {match['ht_cross']['time']}, level: {match['ht_cross']['level']}")
                logger.info(f"  {LT} support cross at {match['lt_cross']['time']}, level: {match['lt_cross']['level']}")
                logger.info(f"  {LT} bullish pattern from {match['pattern']['time_first']} to {match['pattern']['time_second']}")
        
        if bearish_matches:
            logger.info(f"\nFound {len(bearish_matches)} tickers with bearish sequence ({HT} resistance -> {LT} resistance -> {LT} bear):")
            for match in bearish_matches:
                logger.info(f"\n{match['ticker']}:")
                logger.info(f"  {HT} resistance cross at {match['ht_cross']['time']}, level: {match['ht_cross']['level']}")
                logger.info(f"  {LT} resistance cross at {match['lt_cross']['time']}, level: {match['lt_cross']['level']}")
                logger.info(f"  {LT} bearish pattern from {match['pattern']['time_first']} to {match['pattern']['time_second']}")
        
        if not (bullish_matches or bearish_matches):
            logger.info(f"No tickers found matching the sequence criteria for {HT}/{LT} timeframes.")

def find_single_timeframe_sequences(tickers, timeframes, candles_data):
    """
    Finds tickers with specific sequences of events within each timeframe:
    - Bullish sequences: 
        * higher timeframe support cross -> support cross -> bullish pattern
        * higher timeframe support cross -> bullish pattern -> support cross
    - Bearish sequences:
        * higher timeframe resistance cross -> resistance cross -> bearish pattern
        * higher timeframe resistance cross -> bearish pattern -> resistance cross
    """
    logger.info("\nAnalyzing single timeframe pattern sequences...")
    
    # Map timeframes to their higher timeframe
    timeframe_map = {
        '15': '60',
        '60': '240',  
        '240': 'D',
        'D': 'W',
        'W': 'M'
    }
    
    # Create a set of all required timeframes
    all_timeframes = set(timeframes)
    for tf in timeframes:
        if tf != 'M' and tf in timeframe_map:
            all_timeframes.add(timeframe_map[tf])
    
    # Get analysis data with all required timeframes
    sr_data = analyze_support_resistance(tickers, list(all_timeframes), candles_data)
    pa_data = analyze_price_action(tickers, list(all_timeframes), candles_data)

    # Lists to store matches across all timeframes
    bullish_matches_cs = [] 
    bullish_matches_sc = []
    bearish_matches_cr = []
    bearish_matches_rc = []
    
    # Map timeframes to their higher timeframe
    timeframe_map = {
        '15': '60',
        '60': '240',  
        '240': 'D',
        'D': 'W',
        'W': 'M'
    }
        
    for timeframe in timeframes:
        # Skip monthly timeframe as it has no higher timeframe
        if timeframe == 'M':
            continue
            
        higher_timeframe = timeframe_map[timeframe]
        
        for ticker in sr_data:
            # Get crosses and patterns for current and higher timeframes
            crosses = sr_data[ticker][timeframe]['crosses']
            higher_crosses = sr_data[ticker][higher_timeframe]['crosses']
            patterns = pa_data[ticker][timeframe]['patterns'] if ticker in pa_data else []
            
            # Get filtered crosses and patterns
            support_crosses = [c for c in crosses if c['type'] == 'support']
            resistance_crosses = [c for c in crosses if c['type'] == 'resistance']
            higher_support_crosses = [c for c in higher_crosses if c['type'] == 'support']
            higher_resistance_crosses = [c for c in higher_crosses if c['type'] == 'resistance']
            bull_patterns = [p for p in patterns if p['type'] == 'BULL']
            bear_patterns = [p for p in patterns if p['type'] == 'BEAR']
            
            # Check bullish sequences
            if higher_support_crosses and support_crosses and bull_patterns:
                # Higher TF Support Cross -> Support Cross -> Pattern
                for ht_cross in higher_support_crosses:
                    ht_cross_time = ht_cross['time']
                    matching_crosses = [c for c in support_crosses if c['time'] > ht_cross_time]
                    
                    for cross in matching_crosses:
                        cross_time = cross['time']
                        matching_patterns = [p for p in bull_patterns if p['time_first'] > cross_time]
                        
                        if matching_patterns:
                            bullish_matches_cs.append({
                                'ticker': ticker,
                                'timeframe': timeframe,
                                'ht_cross': ht_cross,
                                'cross': cross,
                                'pattern': matching_patterns[0]
                            })
                
                # Higher TF Support Cross -> Pattern -> Cross
                for ht_cross in higher_support_crosses:
                    ht_cross_time = ht_cross['time']
                    matching_patterns = [p for p in bull_patterns if p['time_first'] > ht_cross_time]
                    
                    for pattern in matching_patterns:
                        pattern_time = pattern['time_second']
                        matching_crosses = [c for c in support_crosses if c['time'] > pattern_time]
                        
                        if matching_crosses:
                            bullish_matches_sc.append({
                                'ticker': ticker,
                                'timeframe': timeframe,
                                'ht_cross': ht_cross,
                                'pattern': pattern,
                                'cross': matching_crosses[0]
                            })
            
            # Check bearish sequences
            if higher_resistance_crosses and resistance_crosses and bear_patterns:
                # Higher TF Resistance Cross -> Resistance Cross -> Pattern
                for ht_cross in higher_resistance_crosses:
                    ht_cross_time = ht_cross['time']
                    matching_crosses = [c for c in resistance_crosses if c['time'] > ht_cross_time]
                    
                    for cross in matching_crosses:
                        cross_time = cross['time']
                        matching_patterns = [p for p in bear_patterns if p['time_first'] > cross_time]
                        
                        if matching_patterns:
                            bearish_matches_cr.append({
                                'ticker': ticker,
                                'timeframe': timeframe,
                                'ht_cross': ht_cross,
                                'cross': cross,
                                'pattern': matching_patterns[0]
                            })
                
                # Higher TF Resistance Cross -> Pattern -> Cross
                for ht_cross in higher_resistance_crosses:
                    ht_cross_time = ht_cross['time']
                    matching_patterns = [p for p in bear_patterns if p['time_first'] > ht_cross_time]
                    
                    for pattern in matching_patterns:
                        pattern_time = pattern['time_second']
                        matching_crosses = [c for c in resistance_crosses if c['time'] > pattern_time]
                        
                        if matching_crosses:
                            bearish_matches_rc.append({
                                'ticker': ticker,
                                'timeframe': timeframe,
                                'ht_cross': ht_cross,
                                'pattern': pattern,
                                'cross': matching_crosses[0]
                            })

    # Print all results grouped by sequence type
    if bullish_matches_cs:
        logger.info(f"\nFound {len(bullish_matches_cs)} tickers with bullish sequence (HT support cross -> support cross -> bull pattern):")
        for match in bullish_matches_cs:
            logger.info(f"\n{match['ticker']} (Timeframe: {match['timeframe']}):")
            logger.info(f"  Higher TF Support cross at {match['ht_cross']['time']}, level: {match['ht_cross']['level']}")
            logger.info(f"  Support cross at {match['cross']['time']}, level: {match['cross']['level']}")
            logger.info(f"  Bullish pattern from {match['pattern']['time_first']} to {match['pattern']['time_second']}")
            logger.info(f"  First candle: O:{match['pattern']['first_candle']['o']}, H:{match['pattern']['first_candle']['h']}, L:{match['pattern']['first_candle']['l']}, C:{match['pattern']['first_candle']['c']}")
            logger.info(f"  Second candle: O:{match['pattern']['second_candle']['o']}, H:{match['pattern']['second_candle']['h']}, L:{match['pattern']['second_candle']['l']}, C:{match['pattern']['second_candle']['c']}")
    
    if bullish_matches_sc:
        logger.info(f"\nFound {len(bullish_matches_sc)} tickers with bullish sequence (HT support cross -> bull pattern -> support cross):")
        for match in bullish_matches_sc:
            logger.info(f"\n{match['ticker']} (Timeframe: {match['timeframe']}):")
            logger.info(f"  Higher TF Support cross at {match['ht_cross']['time']}, level: {match['ht_cross']['level']}")
            logger.info(f"  Bullish pattern from {match['pattern']['time_first']} to {match['pattern']['time_second']}")
            logger.info(f"  First candle: O:{match['pattern']['first_candle']['o']}, H:{match['pattern']['first_candle']['h']}, L:{match['pattern']['first_candle']['l']}, C:{match['pattern']['first_candle']['c']}")
            logger.info(f"  Second candle: O:{match['pattern']['second_candle']['o']}, H:{match['pattern']['second_candle']['h']}, L:{match['pattern']['second_candle']['l']}, C:{match['pattern']['second_candle']['c']}")
            logger.info(f"  Support cross at {match['cross']['time']}, level: {match['cross']['level']}")
    
    if bearish_matches_cr:
        logger.info(f"\nFound {len(bearish_matches_cr)} tickers with bearish sequence (HT resistance cross -> resistance cross -> bear pattern):")
        for match in bearish_matches_cr:
            logger.info(f"\n{match['ticker']} (Timeframe: {match['timeframe']}):")
            logger.info(f"  Higher TF Resistance cross at {match['ht_cross']['time']}, level: {match['ht_cross']['level']}")
            logger.info(f"  Resistance cross at {match['cross']['time']}, level: {match['cross']['level']}")
            logger.info(f"  Bearish pattern from {match['pattern']['time_first']} to {match['pattern']['time_second']}")
            logger.info(f"  First candle: O:{match['pattern']['first_candle']['o']}, H:{match['pattern']['first_candle']['h']}, L:{match['pattern']['first_candle']['l']}, C:{match['pattern']['first_candle']['c']}")
            logger.info(f"  Second candle: O:{match['pattern']['second_candle']['o']}, H:{match['pattern']['second_candle']['h']}, L:{match['pattern']['second_candle']['l']}, C:{match['pattern']['second_candle']['c']}")

    if bearish_matches_rc:
        logger.info(f"\nFound {len(bearish_matches_rc)} tickers with bearish sequence (HT resistance cross -> bear pattern -> resistance cross):")
        for match in bearish_matches_rc:
            logger.info(f"\n{match['ticker']} (Timeframe: {match['timeframe']}):")
            logger.info(f"  Higher TF Resistance cross at {match['ht_cross']['time']}, level: {match['ht_cross']['level']}")
            logger.info(f"  Bearish pattern from {match['pattern']['time_first']} to {match['pattern']['time_second']}")
            logger.info(f"  First candle: O:{match['pattern']['first_candle']['o']}, H:{match['pattern']['first_candle']['h']}, L:{match['pattern']['first_candle']['l']}, C:{match['pattern']['first_candle']['c']}")
            logger.info(f"  Second candle: O:{match['pattern']['second_candle']['o']}, H:{match['pattern']['second_candle']['h']}, L:{match['pattern']['second_candle']['l']}, C:{match['pattern']['second_candle']['c']}")
            logger.info(f"  Resistance cross at {match['cross']['time']}, level: {match['cross']['level']}")
    
    if not (bullish_matches_cs or bullish_matches_sc or bearish_matches_cr or bearish_matches_rc):
        logger.info("No tickers found matching any sequence criteria across all timeframes.")


def find_aligned_with_crosses(tickers, timeframes, candles_data):
    """
    Checks aligned patterns for M/W timeframes and finds matching crosses in W/D timeframes.
    Returns tickers where patterns and crosses align.
    """
    logger.info("\nAnalyzing aligned patterns with crosses...")
    
    # Get aligned patterns
    main_timeframes = ['W', 'D']
    results = find_aligned_patterns(tickers, main_timeframes, candles_data)
    bull_aligned = results[0]
    bear_aligned = results[1]
    
    # Get support/resistance data
    sr_data = analyze_support_resistance(tickers, ['D', '240'], candles_data)
    
    bullish_matches = []
    bearish_matches = []
    
    # Check bull aligned tickers for support crosses
    if bull_aligned:
        for entry in bull_aligned:
            ticker = entry['ticker']
            
            # Look for support crosses in W/D timeframes
            w_crosses = sr_data[ticker]['D']['crosses'] if ticker in sr_data else []
            d_crosses = sr_data[ticker]['240']['crosses'] if ticker in sr_data else []
            
            support_crosses = []
            
            # Get support crosses
            for cross in w_crosses + d_crosses:
                if cross['type'] == 'support':
                    support_crosses.append({
                        'timeframe': cross['timeframe'],
                        'time': cross['time'],
                        'level': cross['level']
                    })
            
            if support_crosses:
                bullish_matches.append({
                    'ticker': ticker,
                    'patterns': entry['patterns'],
                    'crosses': support_crosses
                })
    
    # Check bear aligned tickers for resistance crosses
    if bear_aligned:
        for entry in bear_aligned:
            ticker = entry['ticker']
            
            # Look for resistance crosses in W/D timeframes
            w_crosses = sr_data[ticker]['D']['crosses'] if ticker in sr_data else []
            d_crosses = sr_data[ticker]['240']['crosses'] if ticker in sr_data else []
            
            resistance_crosses = []
            
            # Get resistance crosses
            for cross in w_crosses + d_crosses:
                if cross['type'] == 'resistance':
                    resistance_crosses.append({
                        'timeframe': cross['timeframe'],
                        'time': cross['time'],
                        'level': cross['level']
                    })
            
            if resistance_crosses:
                bearish_matches.append({
                    'ticker': ticker,
                    'patterns': entry['patterns'],
                    'crosses': resistance_crosses
                })
    
    # Log results
    if bullish_matches:
        logger.info(f"\nFound {len(bullish_matches)} bullish tickers with M/W pattern alignment and W/D support crosses:")
        for match in bullish_matches:
            logger.info(f"\n{match['ticker']}:")
            logger.info("Patterns:")
            for tf, pattern in match['patterns'].items():
                logger.info(f"  {tf}: {pattern['time_first']} to {pattern['time_second']}")
            logger.info("Support crosses:")
            for cross in match['crosses']:
                logger.info(f"  {cross['timeframe']}: {cross['time']} at level {cross['level']}")
    
    if bearish_matches:
        logger.info(f"\nFound {len(bearish_matches)} bearish tickers with M/W pattern alignment and W/D resistance crosses:")
        for match in bearish_matches:
            logger.info(f"\n{match['ticker']}:")
            logger.info("Patterns:")
            for tf, pattern in match['patterns'].items():
                logger.info(f"  {tf}: {pattern['time_first']} to {pattern['time_second']}")
            logger.info("Resistance crosses:")
            for cross in match['crosses']:
                logger.info(f"  {cross['timeframe']}: {cross['time']} at level {cross['level']}")
    
    return [bullish_matches, bearish_matches]

def find_monthly_crosses(tickers, timeframes, candles_data):
    """
    Finds all tickers that have crossed monthly support or resistance levels.
    """
    logger.info("\nAnalyzing monthly support/resistance crosses...")

    # Get support/resistance data for monthly timeframe
    sr_data = analyze_support_resistance(tickers, ['M'], candles_data)

    monthly_support_crosses = []
    monthly_resistance_crosses = []

    for ticker in sr_data:
        # Get monthly crosses
        crosses = sr_data[ticker]['M']['crosses'] if ticker in sr_data else []

        # Separate support and resistance crosses
        for cross in crosses:
            if cross['type'] == 'support':
                monthly_support_crosses.append({
                    'ticker': ticker,
                    'time': cross['time'],
                    'level': cross['level'],
                    'candle': cross['candle']
                })
            elif cross['type'] == 'resistance':
                monthly_resistance_crosses.append({
                    'ticker': ticker,
                    'time': cross['time'],
                    'level': cross['level'],
                    'candle': cross['candle']
                })

    # Log results
    if monthly_support_crosses:
        logger.info(f"\nFound {len(monthly_support_crosses)} tickers with monthly support crosses:")
        for cross in monthly_support_crosses:
            logger.info(f"\n{cross['ticker']}:")
            logger.info(f"  Time: {cross['time']}")
            logger.info(f"  Support level: {cross['level']}")
            logger.info(f"  Cross candle: O:{cross['candle']['o']} H:{cross['candle']['h']} L:{cross['candle']['l']} C:{cross['candle']['c']}")

    if monthly_resistance_crosses:
        logger.info(f"\nFound {len(monthly_resistance_crosses)} tickers with monthly resistance crosses:")
        for cross in monthly_resistance_crosses:
            logger.info(f"\n{cross['ticker']}:")
            logger.info(f"  Time: {cross['time']}")
            logger.info(f"  Resistance level: {cross['level']}")
            logger.info(f"  Cross candle: O:{cross['candle']['o']} H:{cross['candle']['h']} L:{cross['candle']['l']} C:{cross['candle']['c']}")

    return [monthly_support_crosses, monthly_resistance_crosses]

def analyze_ticker_metrics(tickers, timeframes, candles_data, max_prob):
    """
    Analyzes tickers/timeframes for linear regression slope, ATH discount, and SMA200 probability.
    Gets ATH directly from database monthly highs.
    Only returns results where:
    - For negative slope: price must be above SMA200
    - For positive slope: price must be below SMA200
    
    Args:
        tickers (list): List of ticker symbols
        timeframes (list): List of timeframe strings
        candles_data (dict): Nested dictionary with candle data
    """

    logger.info("\nAnalyzing ticker metrics...")
    
    results = []
    
    # Get ATH data from database for all tickers
    connection = get_db_connection()
    cursor_db = connection.cursor()
    
    # Get highest monthly highs for each ticker
    ath_data = {}
    try:
        for ticker in tickers:
            cursor_db.execute("""
                SELECT MAX(h) as ath
                FROM candles 
                WHERE ticker = %s AND timeframe = 'M'
            """, (ticker,))
            result = cursor_db.fetchone()
            if result and result[0]:
                ath_data[ticker] = float(result[0])
    except Exception as e:
        logger.error(f"Error fetching ATH data: {e}")
    finally:
        cursor_db.close()
        connection.close()
    
    for ticker in tickers:
        for timeframe in timeframes:
            if ticker not in candles_data or timeframe not in candles_data[ticker]:
                continue
                
            candles = candles_data[ticker][timeframe]
            if len(candles) < 200:  # Need at least 200 candles for SMA200
                continue
            
            # Extract close prices and convert to numpy array
            closes = np.array([float(c[4]) for c in candles])
            
            # 1. Calculate linear regression slope
            X = np.arange(len(closes)).reshape(-1, 1)
            reg = LinearRegression().fit(X, closes)
            slope = reg.coef_[0]
            
            # Calculate slope as percent change over 200 candles
            slope_percent = (slope * 200 / closes[0]) * 100
            
            # 2. Calculate ATH discount using database ATH
            current_price = closes[-1]
            if ticker in ath_data:
                ath = ath_data[ticker]
                ath_discount = ((ath - current_price) / ath) * 100
            else:
                logger.warning(f"No ATH data available for {ticker}, using timeframe highs")
                ath = np.max(closes)
                ath_discount = ((ath - current_price) / ath) * 100
            
            # 3. Calculate SMA200
            sma200 = np.convolve(closes, np.ones(200)/200, mode='valid')
            current_sma = sma200[-1]
            diffs = closes[199:] - sma200  # Differences between price and SMA200
            
            # Calculate mean and std of historical diffs for normal distribution
            diff_mean = np.mean(diffs[:-1])  # Exclude latest diff
            diff_std = np.std(diffs[:-1])
            
            # Calculate probability of latest diff
            latest_diff = diffs[-1]
            if diff_std != 0:
                z_score = (latest_diff - diff_mean) / diff_std
                probability = stats.norm.cdf(z_score) * 100
            else:
                probability = 50  # Default to 50% if std is 0

            # Only add results that meet the slope/price conditions
            if ((slope_percent < 0 and current_price > current_sma) or \
               (slope_percent > 0 and current_price < current_sma)) and \
               abs(probability) <= max_prob:
                results.append({
                    'ticker': ticker,
                    'timeframe': timeframe,
                    'slope_percent': slope_percent,
                    'ath_discount': ath_discount,
                    'sma_probability': probability,
                    'current_price': current_price,
                    'current_sma': current_sma,
                    'ath': ath
                })
    
    # Sort by SMA probability descending
    results.sort(key=lambda x: x['sma_probability'], reverse=True)
    
    # Print results
    logger.info("\nResults sorted by SMA200 probability (highest to lowest):")
    for r in results:
        logger.info(f"\n{r['ticker']} ({r['timeframe']}):")
        logger.info(f"  Price: {r['current_price']:.4f}")
        logger.info(f"  SMA200: {r['current_sma']:.4f}") 
        logger.info(f"  ATH: {r['ath']:.4f}")
        logger.info(f"  200-candle slope: {r['slope_percent']:.2f}%")
        logger.info(f"  ATH discount: {r['ath_discount']:.2f}%")
        logger.info(f"  SMA200 probability: {r['sma_probability']:.2f}%")

# --------------------------
# Main Scheduler Setup
# --------------------------
def main():

    timeframes = ['W', 'D', '240', '60', '15']
    tickers = get_active_tickers_from_db()
    #tickers = ['BNBUSDT']
    candle_data = get_candles_data(tickers, timeframes)
    
    #timeframes = ['D', '240', '60', '15']
    #timeframes = ['15']
    #print_situations()
    #print_downtrend_tickers()
    #find_trend_crosses(tickers, timeframes, candle_data)
    #find_aligned_patterns()
    #find_pattern_sequence(tickers, timeframes, candle_data)

    #find_single_timeframe_sequences(tickers, timeframes, candle_data)

    #find_monthly_crosses(tickers, timeframes, candle_data)

    #find_aligned_with_crosses(tickers, timeframes, candle_data)

    analyze_ticker_metrics(tickers, timeframes, candle_data, 2.0)

# --------------------------
# Script Entry Point
# --------------------------
if __name__ == '__main__':
    main()
