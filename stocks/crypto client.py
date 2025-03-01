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

def analyze_support_resistance(timeframes):
    """
    Analyzes candle data to identify supports and resistances across all tickers and timeframes,
    and logs when recent candles cross these levels.
    """
    logger.info("Starting support/resistance analysis")
    connection = get_db_connection()
    cursor_db = connection.cursor()
    
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
        
        return market_data
                
    except Exception as e:
        logger.error(f"Error during support/resistance analysis: {e}")
        traceback.print_exc()
    finally:
        cursor_db.close()
        connection.close()
    
    logger.info("Completed support/resistance analysis")

def analyze_price_action(timeframes):
    """
    Analyzes candle data to identify price action patterns (bullish/bearish continuations)
    between adjacent candles in the newest 4 candles.
    """
    logger.info("Starting price action analysis")
    connection = get_db_connection()
    cursor_db = connection.cursor()
    
    try:
        active_tickers = get_active_tickers_from_db()
        price_action_data = {}

        # Initialize data structure
        for ticker in active_tickers:
            price_action_data[ticker] = {}
            for timeframe in timeframes:
                price_action_data[ticker][timeframe] = {
                    'patterns': []
                }

        for ticker in active_tickers:
            for timeframe in timeframes:
                # Get newest 4 candles for this ticker and timeframe
                cursor_db.execute("""
                    SELECT timestamp, o, h, l, c 
                    FROM candles 
                    WHERE ticker = %s AND timeframe = %s 
                    ORDER BY timestamp DESC
                    LIMIT 4
                """, (ticker, timeframe))
                
                candles = cursor_db.fetchall()[::-1]  # Reverse to get chronological order
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
    finally:
        cursor_db.close()
        connection.close()
    
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

def find_trend_crosses():
    """
    Finds tickers in long-term trends that recently crossed key levels:
    - Downtrend tickers crossing resistance
    - Uptrend tickers crossing support
    """
    logger.info("\nAnalyzing trend tickers with level crosses...")
    
    # Get analysis data
    sr_data = analyze_support_resistance()
    pa_data = analyze_price_action()
    
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

def find_aligned_patterns():
    """
    Finds tickers that show aligned bullish or bearish patterns across monthly, weekly, daily and 4h timeframes
    in their most recent patterns.
    """
    logger.info("\nAnalyzing tickers for aligned patterns across timeframes...")
    price_action_data = analyze_price_action()
    
    # Find bullish alignments
    bull_aligned = []
    bear_aligned = []
    
    for ticker in price_action_data:
        # Get latest patterns for each timeframe
        m_patterns = price_action_data[ticker]['M']['patterns']
        w_patterns = price_action_data[ticker]['W']['patterns']
        d_patterns = price_action_data[ticker]['D']['patterns']
        h4_patterns = price_action_data[ticker]['240']['patterns']
        
        # Check if there are patterns in all timeframes
        if not (m_patterns and w_patterns and d_patterns and h4_patterns):
            continue
        
        # Get last pattern from each timeframe
        last_m = m_patterns[-1]
        last_w = w_patterns[-1]
        last_d = d_patterns[-1]
        last_h4 = h4_patterns[-1]
        
        # Check for bullish alignment
        if (last_m['type'] == 'BULL' and 
            last_w['type'] == 'BULL' and 
            last_d['type'] == 'BULL' and
            last_h4['type'] == 'BULL'):
            bull_aligned.append({
                'ticker': ticker,
                'monthly': last_m,
                'weekly': last_w,
                'daily': last_d,
                'h4': last_h4
            })
        
        # Check for bearish alignment
        if (last_m['type'] == 'BEAR' and 
            last_w['type'] == 'BEAR' and 
            last_d['type'] == 'BEAR' and
            last_h4['type'] == 'BEAR'):
            bear_aligned.append({
                'ticker': ticker,
                'monthly': last_m,
                'weekly': last_w,
                'daily': last_d,
                'h4': last_h4
            })
    
    # Print results
    if bull_aligned:
        logger.info("\nTickers with bullish alignment across M/W/D/4H timeframes:")
        for entry in bull_aligned:
            logger.info(f"\n{entry['ticker']}:")
            logger.info(f"Monthly: {entry['monthly']['time_first']} to {entry['monthly']['time_second']}")
            logger.info(f"Weekly: {entry['weekly']['time_first']} to {entry['weekly']['time_second']}")
            logger.info(f"Daily: {entry['daily']['time_first']} to {entry['daily']['time_second']}")
            logger.info(f"4H: {entry['h4']['time_first']} to {entry['h4']['time_second']}")
    
    if bear_aligned:
        logger.info("\nTickers with bearish alignment across M/W/D/4H timeframes:")
        for entry in bear_aligned:
            logger.info(f"\n{entry['ticker']}:")
            logger.info(f"Monthly: {entry['monthly']['time_first']} to {entry['monthly']['time_second']}")
            logger.info(f"Weekly: {entry['weekly']['time_first']} to {entry['weekly']['time_second']}")
            logger.info(f"Daily: {entry['daily']['time_first']} to {entry['daily']['time_second']}")
            logger.info(f"4H: {entry['h4']['time_first']} to {entry['h4']['time_second']}")

    if not (bull_aligned or bear_aligned):
        logger.info("No tickers found with aligned patterns across timeframes.")

def find_pattern_sequence():
    """
    Finds tickers with specific sequences of events in all timeframes:
    - Bullish sequence: HT support cross -> LT support cross -> LT bullish pattern
    - Bearish sequence: HT resistance cross -> LT resistance cross -> LT bearish pattern
    """
    logger.info("\nAnalyzing pattern sequences...")
    
    # Get analysis data
    sr_data = analyze_support_resistance()
    pa_data = analyze_price_action()
    
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

def find_single_timeframe_sequences(timeframes):
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
    
    # Get analysis data
    sr_data = analyze_support_resistance(timeframes)
    pa_data = analyze_price_action(timeframes)

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
    
    if bullish_matches_sc:
        logger.info(f"\nFound {len(bullish_matches_sc)} tickers with bullish sequence (HT support cross -> bull pattern -> support cross):")
        for match in bullish_matches_sc:
            logger.info(f"\n{match['ticker']} (Timeframe: {match['timeframe']}):")
            logger.info(f"  Higher TF Support cross at {match['ht_cross']['time']}, level: {match['ht_cross']['level']}")
            logger.info(f"  Bullish pattern from {match['pattern']['time_first']} to {match['pattern']['time_second']}")
            logger.info(f"  Support cross at {match['cross']['time']}, level: {match['cross']['level']}")
    
    if bearish_matches_cr:
        logger.info(f"\nFound {len(bearish_matches_cr)} tickers with bearish sequence (HT resistance cross -> resistance cross -> bear pattern):")
        for match in bearish_matches_cr:
            logger.info(f"\n{match['ticker']} (Timeframe: {match['timeframe']}):")
            logger.info(f"  Higher TF Resistance cross at {match['ht_cross']['time']}, level: {match['ht_cross']['level']}")
            logger.info(f"  Resistance cross at {match['cross']['time']}, level: {match['cross']['level']}")
            logger.info(f"  Bearish pattern from {match['pattern']['time_first']} to {match['pattern']['time_second']}")

    if bearish_matches_rc:
        logger.info(f"\nFound {len(bearish_matches_rc)} tickers with bearish sequence (HT resistance cross -> bear pattern -> resistance cross):")
        for match in bearish_matches_rc:
            logger.info(f"\n{match['ticker']} (Timeframe: {match['timeframe']}):")
            logger.info(f"  Higher TF Resistance cross at {match['ht_cross']['time']}, level: {match['ht_cross']['level']}")
            logger.info(f"  Bearish pattern from {match['pattern']['time_first']} to {match['pattern']['time_second']}")
            logger.info(f"  Resistance cross at {match['cross']['time']}, level: {match['cross']['level']}")
    
    if not (bullish_matches_cs or bullish_matches_sc or bearish_matches_cr or bearish_matches_rc):
        logger.info("No tickers found matching any sequence criteria across all timeframes.")

# --------------------------
# Main Scheduler Setup
# --------------------------
def main():

    timeframes = ['M', 'W', 'D', '240', '60', '15']
    #timeframes = ['15']
    #print_situations()
    #print_downtrend_tickers()
    #find_trend_crosses()
    #find_aligned_patterns()
    #find_pattern_sequence()

    find_single_timeframe_sequences(timeframes)

# --------------------------
# Script Entry Point
# --------------------------
if __name__ == '__main__':
    main()
