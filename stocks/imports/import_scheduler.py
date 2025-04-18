from stocks.data_providers.alpha_vantage import get_tickers_download, get_earnings_calendar
from stocks.exporters.ical_exporter import export_earnings
from apscheduler.schedulers.background import BackgroundScheduler
from sklearn.linear_model import LinearRegression
from stocks.db import DAO_Tickers, ROW_Tickers, DB, ROW_TickersData, DAO_TickersData, DAO_Portfolios, ROW_Portfolios, ROW_PortfolioPositions, DAO_PortfolioPositions, TICKERS_TIME_DATA__TYPE__CONST, FUNDAMENTAL_NAME__TO_TYPE__ANNUAL, FUNDAMENTAL_NAME__TO_TYPE__QUATERLY
from stocks.data_providers.fmp import FMP, FMP_Metrics, FMPException_LimitReached
import logging
from logging.handlers import RotatingFileHandler
import yfinance as yf
import pandas as pd
import math
import re
import os
import numpy as np
from scipy import stats

from datetime import date, timedelta, datetime
import traceback


# Create a custom logger
logger = logging.getLogger('import_scheduler_logger')
logger.setLevel(logging.DEBUG)  # Set minimum level of logging

# Create handlers
rotating_file_handler = RotatingFileHandler(
    'invest.log', maxBytes=10*1024*1024, backupCount=50)  # Log file that rolls over at 10MB
console_handler = logging.StreamHandler()  # Console handler

# Set level for each handler
rotating_file_handler.setLevel(logging.DEBUG)
console_handler.setLevel(logging.DEBUG)

# Create formatters and add it to handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotating_file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(rotating_file_handler)
logger.addHandler(console_handler)

logging.getLogger('yfinance').setLevel(logging.CRITICAL + 1)  # This effectively disables logging for this logger
logging.getLogger('urllib3.connectionpool').setLevel(logging.CRITICAL + 1)  # This effectively disables logging for this logger


scheduler = BackgroundScheduler()

def resetAfterSplit(input_ticker_id_list=None):
    logger.info(f"resetAfterSplit - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        tickers = []
        if (input_ticker_id_list != None):
            tickers = input_ticker_id_list

        for ticker_id in tickers:
            dao_tickers_data.delete(ticker_id, True)

            download_prices([ticker_id])
            update_ticker_target_price([ticker_id])
            update_stock_recommendations([ticker_id])
            update_stock_predictions([ticker_id])
            downloadStockOptionData([ticker_id])
            download_fundamental_statements([ticker_id])
            

            estimate_growth_stocks([ticker_id])
            calculate_price_discount([ticker_id])
            calc_valuation_ratios_stocks([ticker_id])
            calc_valuation_stocks([ticker_id])

            calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS, [ticker_id])
            calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS, [ticker_id])
            calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS, [ticker_id])
            calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS, [ticker_id])

            calc_ratio_discounts([ticker_id])


            logger.info(f"resetAfterSplit: Resetted {ticker_id}")
    except Exception as e:
        logger.error(f"resetAfterSplit - Error {e}")
        traceback.print_exc()
    logger.info(f"resetAfterSplit - End")

def notify_earnings():
    print(f"Task Notify_Earnings executed at: {datetime.datetime.now()}")
    tickers = ['GOOGL','META','AAPL','AMZN','MSFT','BRK-B','V','PLTR','NVDA','PYPL','DIS','MPW','UFPI','O','VICI','BTI','NXST','TSM','VRTX','CMCSA']

    calendar = get_earnings_calendar()

    export_earnings(tickers)

def sync_ticker_id_list():
    logger.info(f"sync_ticker_id_list - Start")
    try:
        exchange_list = ['NASDAQ', 'NYSE']
        #exchange_list = ['NASDAQ', 'NYSE', 'XETRA', 'EURONEXT', 'LSE']
        
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)

        ticker_list = []

        for exchange in exchange_list:
            exchange_ticker_list = fmp.get_symbols_list(exchange)

            if exchange_ticker_list != None:
                for exchange_ticker in exchange_ticker_list:
                    ticker_list.append(exchange_ticker['symbol'])

        db_ticker_list = dao_tickers.select_tickers_all_ids()

        for ticker_id in ticker_list:
            if ticker_id.upper() not in db_ticker_list:
                dao_tickers.insert_ticker(ticker_id.upper(), True)
                logger.info(f"sync_ticker_id_list - Added {ticker_id.upper()}")
                db_ticker_list.append(ticker_id.upper())
    except Exception as e:
        logger.error(f"sync_ticker_id_list - Error {e}")
        traceback.print_exc()
    
    logger.info(f"sync_ticker_id_list - End")

def update_ticker_profile(refresh: bool, input_ticker_id_list = None):
    logger.info(f"update_ticker_profile({refresh}) - Start")
    try:
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)

        db_ticker_list = dao_tickers.select_tickers_all_ids()
        if (input_ticker_id_list != None):
            db_ticker_list = input_ticker_id_list
        #db_ticker_list = ['AAPL', 'GOOG', 'MPW']

        counter = 0
        for ticker_id in db_ticker_list:
            counter += 1
            ticker = dao_tickers.select_ticker(ticker_id)
            if ticker == None or ticker.market_cap == None or refresh:
                profile = fmp.get_stock_profile(ticker_id)
                if len(profile) == 1:

                    if profile[0]['isFund'] == True or profile[0]['isActivelyTrading'] == False or profile[0]['isEtf'] == True:
                        dao_tickers.delete_tickers(ticker_id)
                        continue

                    dict_data = {
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__BETA: profile[0]['beta'],
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__ISIN: profile[0]['isin'],
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__NAME: profile[0]['companyName'][:100],
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__MARKET_CAP: profile[0]['mktCap'],
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__SECTOR: profile[0]['sector'],
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__INDUSTRY: profile[0]['industry'],
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE: profile[0]['price'],
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__DESCRIPTION: profile[0]['description'],
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__EXCHANGE: profile[0]['exchangeShortName'],
                    }

                    dao_tickers.update_ticker_types(ticker_id, dict_data, True)
                    logger.info(f"update_ticker_profile - {counter}/{len(db_ticker_list)}")
                else:
                    logger.error(f"Multiple or no profile for ticker {ticker_id}")
    except Exception as e:
        logger.error(f"update_ticker_profile - Error {e}")
        traceback.print_exc()
    logger.info(f"update_ticker_profile - End")

def update_earnings_calendar():

    logger.info(f"update_earnings_calendar - Start")

    try:
        
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)

        from_date = datetime.now().strftime('%Y-%m-%d')
        to_date = (datetime.now() + timedelta(days=93)).strftime('%Y-%m-%d')
        calendar = fmp.get_earnings_calendar(from_date, to_date)

        for row in calendar:
            dict_data = {
                TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__EARNINGS_DATE: row['date']
            }
            dao_tickers.update_ticker_types(row['symbol'], dict_data, True)

    except Exception as e:
        logger.error(f"update_earnings_calendar - Error {e}")
        traceback.print_exc()
    logger.info(f"update_earnings_calendar - End")    

def download_prices(input_ticker_id_list=None):
    logger.info(f"download_prices - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)
        fmp = FMP()

        tickers = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            tickers = input_ticker_id_list
        today = datetime.today().strftime("%Y-%m-%d")
        fromDay_0 = "1900-01-01"

        counter = 0
        skip = True
        for ticker_id in tickers:
            counter += 1

            last_price_result = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, 1)
            last_volume_result = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.VOLUME, 1)

            if len(last_price_result) == 0:
                fromDay = fromDay_0
            else:
                fromDay = (last_price_result[0].date  + timedelta(days=1)).strftime("%Y-%m-%d")
                if fromDay > today:
                    continue

            try:
                yf_ticker = yf.Ticker(ticker_id)
                hist = yf_ticker.history(start=fromDay, end=today)
                prices = []
                for idx, row in hist.iterrows():
                    prices.append({
                        'date': idx.strftime("%Y-%m-%d"),
                        'adjClose': row['Close'],
                        'volume': row['Volume']
                    })
            except:
                prices = None

            rows_price = []
            rows_volume = []

            date_list = []
            if prices == None:
                continue
            for record in prices:
                row = ROW_TickersData()

                date_d = datetime.strptime(record['date'], "%Y-%m-%d").date()
                if date_d in date_list:
                    continue

                row.date = date_d
                row.ticker_id = ticker_id
                row.type = TICKERS_TIME_DATA__TYPE__CONST.PRICE
                row.value = record['adjClose']
                if (row.date != None and row.value != None and (len(last_price_result) == 0 or row.date > last_price_result[-1].date)):
                    rows_price.append(row)

                if record == prices[0]:            
                    dict_data = {
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE: row.value
                    }
                    dao_tickers.update_ticker_types(ticker_id, dict_data, True)

                row = ROW_TickersData()
                row.date = date_d
                row.ticker_id = ticker_id
                row.type = TICKERS_TIME_DATA__TYPE__CONST.VOLUME
                row.value = record['volume']

                if (row.date != None and row.value != None and (len(last_volume_result) == 0 or row.date > last_volume_result[-1].date)):
                    rows_volume.append(row)
                    date_list.append(row.date)

            dao_tickers_data.bulk_insert_ticker_data(rows_price, True)
            dao_tickers_data.bulk_insert_ticker_data(rows_volume, True)
            logger.info(f"download_prices: Updated {ticker_id} {counter}/{len(tickers)} count={len(rows_price)}")
    except Exception as e:
        logger.error(f"download_prices - Error {e}")
        traceback.print_exc()
    logger.info(f"download_prices - End")

def update_ticker_target_price(input_ticker_id_list=None):
    logger.info(f"update_ticker_target_price - Start")
    try:
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)
        today = datetime.today().date()

        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            db_ticker_list = input_ticker_id_list

        counter = 0
        for ticker_id in db_ticker_list:
            counter += 1
            target = fmp.get_stock_price_target(ticker_id)
            if target != None and len(target) == 1:

                lastMonthTarget = target[0]['lastMonthAvgPriceTarget']
                lastMonthTargetCount = target[0]['lastMonth']
                lastQTarget = target[0]['lastQuarterAvgPriceTarget']
                lastQTargetCount = target[0]['lastQuarter']

                if lastMonthTargetCount + lastQTargetCount > 0:
                    target_price = ((lastMonthTarget * lastMonthTargetCount) + (lastQTarget * lastQTargetCount)) / (lastMonthTargetCount + lastQTargetCount)

                    dict_data = {
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__TARGET_PRICE: target_price
                    }

                    dao_tickers.update_ticker_types(ticker_id, dict_data, True)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__TARGET_PRICE, target_price, today)
                    logger.info(f"update_ticker_target_price {counter}/{len(db_ticker_list)}")

            else:
                logger.error(f"Multiple or no target price for ticker {ticker_id}")
    except Exception as e:
        logger.error(f"update_ticker_target_price - Error {e}")
        traceback.print_exc()
    logger.info(f"update_ticker_target_price - End")

def update_stock_recommendations(input_ticker_id_list = None):
    logger.info(f"update_stock_recommendations - Start")

    try:
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        today = datetime.today().date()

        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            db_ticker_list = input_ticker_id_list


        counter = 0
        skip = True
        for ticker_id in db_ticker_list:
            counter += 1
            if ticker_id == 'WOBDX':
                skip = False

            logger.info(f"update_stock_recommendations - {ticker_id} - {counter}/{len(db_ticker_list)}")
            try:
                recommendations = fmp.get_recommendations(ticker_id)
                if recommendations != None and len(recommendations) > 0:

                    analystRatingsStrongBuy = recommendations[0]['analystRatingsStrongBuy']
                    analystRatingsbuy = recommendations[0]['analystRatingsbuy']
                    analystRatingsHold = recommendations[0]['analystRatingsHold']
                    analystRatingsSell = recommendations[0]['analystRatingsSell']
                    analystRatingsStrongSell = recommendations[0]['analystRatingsStrongSell']
                    
                    recomm = analystRatingsStrongBuy + (analystRatingsbuy * 2) + (analystRatingsHold * 3) + (analystRatingsSell * 4) + (analystRatingsStrongSell * 5)
                    recomm_count = analystRatingsStrongBuy + analystRatingsbuy + analystRatingsHold + analystRatingsSell + analystRatingsStrongSell

                    if recomm_count > 0:

                        dict_data = {
                            TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__RECOMM_MEAN: recomm / recomm_count,
                            TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__RECOMM_COUNT: recomm_count
                        }

                        dao_tickers.update_ticker_types(ticker_id, dict_data, True)
                        dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__RECOMM_MEAN, recomm / recomm_count, today)
                else:
                    logger.error(f"Multiple or no target price for ticker {ticker_id}")
            except Exception as e:
                logger.error(f"Some error {ticker_id} {e}")
                traceback.print_exc()
                continue
    except Exception as e:
        logger.error(f"update_stock_recommendations - Error {e}")
        traceback.print_exc()
    logger.info(f"update_stock_recommendations - End")

def update_stock_predictions(input_ticker_id_list = None):
    logger.info(f"update_stock_predictions - Start")

    try:
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        today = datetime.today().date()
        current_year = today.year

        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            db_ticker_list = input_ticker_id_list

        counter = 0
        skip = True
        for ticker_id in db_ticker_list:
            counter += 1

            logger.info(f"update_stock_predictions - {ticker_id} - {counter}/{len(db_ticker_list)}")
            try:
                predictions = fmp.get_predictions(ticker_id)
                if predictions != None and len(predictions) > 0:
                    predictions.sort(key=lambda x: x['date'], reverse=False)

                    start_value_eps = None
                    end_value_eps = None
                    periods_eps = 0

                    start_value_rev = None
                    end_value_rev = None
                    periods_rev = 0

                    for prediction in predictions:
                        symbol = prediction['symbol']
                        date = datetime.strptime(prediction['date'], "%Y-%m-%d").date()
                        avg_revenue = prediction['estimatedRevenueAvg']
                        avg_eps = prediction['estimatedEpsAvg']

                        if date.year >= current_year and avg_eps > 0:
                            if start_value_eps == None:
                                start_value_eps = avg_eps
                            end_value_eps = avg_eps
                            periods_eps = periods_eps + 1

                        if date.year >= current_year and avg_revenue > 0:
                            if start_value_rev == None:
                                start_value_rev = avg_revenue
                            end_value_rev = avg_revenue
                            periods_rev = periods_rev + 1

                    periods_rev = periods_rev - 1
                    periods_eps = periods_eps - 1

                    if start_value_eps != None and end_value_eps != None and periods_eps > 0:
                        dict_data = {
                            TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PREDICT_EPS_CAGR: (end_value_eps / start_value_eps) ** (1 / periods_eps) - 1,
                            TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PREDICT_REV_CAGR: (end_value_rev / start_value_rev) ** (1 / periods_rev) - 1
                        }
                        dao_tickers.update_ticker_types(ticker_id, dict_data, True)
                else:
                    logger.error(f"Multiple or no target price for ticker {ticker_id}")
            except Exception as e:
                logger.error(f"Some error {ticker_id} {e}")
                traceback.print_exc()
                continue
    except Exception as e:
        logger.error(f"update_stock_predictions - Error {e}")
        traceback.print_exc()
    logger.info(f"update_stock_predictions - End")

def storeOptionData(ticker_id, chain):
    # Get a database connection and create a cursor
    connection = DB.get_connection_mysql()
    cursor = connection.cursor()

    # Prepare the SQL insert statement with "ON DUPLICATE KEY UPDATE"
    sql = """
        INSERT INTO options (
            ticker_id, option_id, expiration, strike, last_trade_date,
            last_price, bid, ask, `change`, volume, oi, iv, type
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            last_price = VALUES(last_price),
            bid = VALUES(bid),
            ask = VALUES(ask),
            `change` = VALUES(`change`),
            volume = VALUES(volume),
            oi = VALUES(oi),
            iv = VALUES(iv)
    """

    # Helper to extract and insert a row for a given option type
    def process_options(df, opt_type):
        # If chain expiration is available (from the chain), use it.
        # Otherwise, you may need to extract the expiration date from the option row.

        # Iterate over each row in the DataFrame
        for idx, row in df.iterrows():
            # Use the contract symbol as the option ID.
            option_id = row.get('contractSymbol')
            # Convert lastTradeDate (timestamp) to date if needed.
            last_trade = row.get('lastTradeDate')
            last_trade_date = last_trade.date() if last_trade else None

            match = re.search(r'(\d{6})', option_id)
            if match:
                date_str = match.group(1)
                expiration_date = datetime.strptime(date_str, "%y%m%d").date()
            else:
                expiration_date = None

            strike = row.get('strike')
            if strike is None or (isinstance(strike, float) and math.isnan(strike)):
                strike = 0
            last_price = row.get('lastPrice')
            if last_price is None or (isinstance(last_price, float) and math.isnan(last_price)):
                last_price = 0
            bid = row.get('bid')
            if bid is None or (isinstance(bid, float) and math.isnan(bid)):
                bid = 0
            ask = row.get('ask')
            if ask is None or (isinstance(ask, float) and math.isnan(ask)):
                ask = 0
            # Re-fetch last_price for change calculation
            last_price = row.get('lastPrice')
            if last_price is None or (isinstance(last_price, float) and math.isnan(last_price)):
                last_price = 0
            change = row.get('change')
            if change is None or (isinstance(change, float) and math.isnan(change)):
                change = 0
            else:
                change = (change / last_price) * 100 if last_price not in (None, 0) else 0
            vol_val = row.get('volume')
            volume = int(vol_val) if (vol_val is not None and not (isinstance(vol_val, float) and math.isnan(vol_val))) else 0
            oi_val = row.get('openInterest')
            oi = int(oi_val) if (oi_val is not None and not (isinstance(oi_val, float) and math.isnan(oi_val))) else 0
            iv = row.get('impliedVolatility')
            if iv is None or (isinstance(iv, float) and math.isnan(iv)):
                iv = 0

            values = (
                ticker_id,
                option_id,
                expiration_date,
                strike,
                last_trade_date,
                last_price,
                bid,
                ask,
                change,
                volume,
                oi,
                iv,
                opt_type
            )
            try:
                cursor.execute(sql, values)
                connection.commit()
            except Exception as e:
                # Log any error if needed
                print(f"Error inserting option {option_id}: {e}")

    # Process calls ('C') and puts ('P')
    if hasattr(chain, "calls") and not chain.calls.empty:
        process_options(chain.calls, 'C')
    if hasattr(chain, "puts") and not chain.puts.empty:
        process_options(chain.puts, 'P')

    connection.commit()
    cursor.close()
    connection.close()

def get_option_growth_data(chain, date: str) -> float: # [month_price, year_price]
    sum = 0
    count = 0
    for index, row in chain.calls.iterrows():
        #print(f"Strike: {row['strike']}, Bid: {row['bid']}, Ask: {row['ask']}, Open Interest: {row['openInterest']}") #row['impliedVolatility']
        if math.isnan(row['openInterest']) or math.isnan(row['strike']):
            continue
        sum += row['openInterest'] * row['strike']
        count += row['openInterest']

    if (count != 0):
        return sum/count
    
    return None
        #print(f"Avg price({ticker_id} - {date}): {sum/count}    {((sum/count) - yf_ticker.info['currentPrice']) / yf_ticker.info['currentPrice']}")

def downloadStockOptionData(input_ticker_id_list=None):

    logger.info(f"downloadStockOptionData - Start")
    try:

        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        ticker_list = dao_tickers.select_tickers_all__limited_usa_ids()
        ticker_list.append('SPY')

        if (input_ticker_id_list != None):
            ticker_list = input_ticker_id_list
        counter = 0
        for ticker_id in ticker_list:

            counter += 1
            logger.info(f"downloadStockOptionData {counter}/{len(ticker_list)}")

            pd.set_option('display.max_rows', None)
            stock = yf.Ticker(ticker_id)
            try:

                shares = stock.info.get('sharesOutstanding', None)
                price = stock.info.get('currentPrice', None)

                if shares == None:
                    logger.warning(f"Download Stock: Skipping {ticker_id}")
                    continue

                options_expirations = stock.options
                today = datetime.today().date()
                one_year_from_now = (today + timedelta(days=365)).strftime("%Y-%m-%d")
                one_month_from_now = (today + timedelta(days=30)).strftime("%Y-%m-%d")

                month_done = False
                for option_expiration in options_expirations:

                    chain = stock.option_chain(option_expiration)
                    storeOptionData(ticker_id, chain)

                    if month_done == False and (option_expiration > one_month_from_now):
                        #print("one month")
                        future_price = get_option_growth_data(chain, option_expiration)
                        if future_price != None:
                            dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_MONTH_AVG_PRICE, future_price, today)
                            #print(future_price)
                        month_done = True
                        continue
                        
                    if (option_expiration > one_year_from_now):
                        #print("one year")
                        future_price = get_option_growth_data(chain, option_expiration)
                        if future_price != None:
                            if price not in (0, None):
                                year_discount = (future_price - price) / price
                                dict_data = {
                                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__OPTION_YEAR_DISCOUNT: year_discount
                                }
                                dao_tickers.update_ticker_types(ticker_id, dict_data, True)
                            dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_YEAR_AVG_PRICE, future_price, today)
                            #print(future_price)
                        break

                #conn = DB.get_connection_mysql()
                #cursor = conn.cursor()
                #cursor.execute("DELETE FROM options WHERE expiration < CURDATE()")
                #conn.commit()
                #cursor.close()
                #conn.close()
                logger.info(f"Download Stock: Updated {ticker_id}")

            except Exception as err:
                logger.exception(f"Error updating ticker[{ticker_id}]:")
                continue
    except Exception as e:
        logger.error(f"downloadStockOptionData - Error {e}")
        traceback.print_exc()
    logger.info(f"downloadStockOptionData - End")

def download_fundamental_statements(input_ticker_id_list = None):
    logger.info(f"download_fundamental_statements - Start")
    try:
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        statement_list = fmp.get_statement_symbols_list()
        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            db_ticker_list = input_ticker_id_list
        #db_ticker_list = ['PATH', 'GOOG', 'MPW']

        skip = False
        counter = 0
        for ticker_id in db_ticker_list:
            counter += 1
            logger.info(f"download_fundamental_statements({ticker_id}) - {counter}/{len(db_ticker_list)}")
            if ticker_id not in statement_list:
                continue
            
            if skip:
                continue
            
            last_record = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, 1)

            now = date.today()
            if last_record != None and len(last_record) > 0 and (now - last_record[0].date).days < 90:
                continue

            income_statement_q_list = fmp.get_income_statement(ticker_id, True)
            #print(counter)

            if income_statement_q_list != None and len(income_statement_q_list) == 0:
                continue

            balance_sheet_statement_q_list = fmp.get_balance_sheet_statement(ticker_id, True)
            cash_flow_statement_q_list = fmp.get_cash_flow_statement(ticker_id, True)

            income_statement_a_list = fmp.get_income_statement(ticker_id, False)
            balance_sheet_statement_a_list = fmp.get_balance_sheet_statement(ticker_id, False)
            cash_flow_statement_a_list = fmp.get_cash_flow_statement(ticker_id, False)
            
            try :

                for income_statement in income_statement_a_list:
                    date_d = datetime.strptime(income_statement['date'], "%Y-%m-%d").date()
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE, income_statement['revenue'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT, income_statement['grossProfit'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA, income_statement['ebitda'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME, income_statement['netIncome'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS, income_statement['epsdiluted'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING, income_statement['weightedAverageShsOutDil'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_MARGIN, income_statement['grossProfitRatio'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA_MARGIN, income_statement['ebitdaratio'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPERATING_INCOME_MARGIN, income_statement['operatingIncomeRatio'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_MARGIN, income_statement['netIncomeRatio'], date_d)

                for income_statement in income_statement_q_list:
                    date_d = datetime.strptime(income_statement['date'], "%Y-%m-%d").date()
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, income_statement['revenue'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_Q, income_statement['grossProfit'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA_Q, income_statement['ebitda'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_Q, income_statement['netIncome'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS_Q, income_statement['epsdiluted'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING_Q, income_statement['weightedAverageShsOutDil'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_MARGIN_Q, income_statement['grossProfitRatio'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA_MARGIN_Q, income_statement['ebitdaratio'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPERATING_INCOME_MARGIN_Q, income_statement['operatingIncomeRatio'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_MARGIN_Q, income_statement['netIncomeRatio'], date_d)

                for balance_sheet in balance_sheet_statement_a_list:
                    date_d = datetime.strptime(balance_sheet['date'], "%Y-%m-%d").date()
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH, balance_sheet['cashAndShortTermInvestments'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_DEBT, balance_sheet['totalDebt'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.LONG_TERM_DEBT, balance_sheet['longTermDebt'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CURRENT_LIABILITIES, balance_sheet['totalCurrentLiabilities'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_ASSETS, balance_sheet['totalAssets'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CURRENT_ASSETS, balance_sheet['totalCurrentAssets'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_LIABILITIES, balance_sheet['totalLiabilities'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.STOCKHOLDER_EQUITY, balance_sheet['totalStockholdersEquity'], date_d)

                for balance_sheet in balance_sheet_statement_q_list:
                    date_d = datetime.strptime(balance_sheet['date'], "%Y-%m-%d").date()
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_Q, balance_sheet['cashAndShortTermInvestments'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_DEBT_Q, balance_sheet['totalDebt'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.LONG_TERM_DEBT_Q, balance_sheet['longTermDebt'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CURRENT_LIABILITIES_Q, balance_sheet['totalCurrentLiabilities'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_ASSETS_Q, balance_sheet['totalAssets'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CURRENT_ASSETS_Q, balance_sheet['totalCurrentAssets'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_LIABILITIES_Q, balance_sheet['totalLiabilities'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.STOCKHOLDER_EQUITY_Q, balance_sheet['totalStockholdersEquity'], date_d)

                for cash_flow in cash_flow_statement_a_list:
                    date_d = datetime.strptime(cash_flow['date'], "%Y-%m-%d").date()
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF, cash_flow['freeCashFlow'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION, cash_flow['operatingCashFlow'], date_d)

                for cash_flow in cash_flow_statement_q_list:
                    date_d = datetime.strptime(cash_flow['date'], "%Y-%m-%d").date()
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, cash_flow['freeCashFlow'], date_d)
                    dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION_Q, cash_flow['operatingCashFlow'], date_d)
            except Exception as e:
                logger.error(f"download_fundamental_statements - Error {e}")
                traceback.print_exc()
    except Exception as e:
        logger.error(f"download_fundamental_statements - Error {e}")
        traceback.print_exc()
    logger.info(f"download_fundamental_statements - End")

def prepare_growth_data(list: list[ROW_TickersData]) -> list[list]:

    x = []
    y = []

    for data in list:
        data : ROW_TickersData
        if data.value not in (0, None, ''):
            x.append(len(x) + 1)
            y.insert(0, data.value)   # !!!!! !!!!!! prevracene poradi

    if len(y) < 3:
        return None
    else: 
        return [x, y]
    
def prepare_growth_data_TTM(list: list[ROW_TickersData]) -> list[list]:

    x = []
    y = []

    counter = -1
    for data in list:
        data : ROW_TickersData
        counter += 1
        if counter >=3 and data.value not in (0, None, ''):
            x.append(len(x) + 1)
            y.insert(0, data.value + list[counter-1].value + list[counter-2].value + list[counter-3].value )   # !!!!! !!!!!! prevracene poradi

    if len(y) < 3:
        return None
    else: 
        return [x, y]

def predict_growth_rate(x : list[float], y : list[float]) -> list[float]:

    if x == None:
        x = []
        counter = 0
        for y_item in y:
            x.append(counter)
            counter += 1
    
    x_array = np.array(x).reshape(-1, 1)
    y_array = np.array(y)

    model = LinearRegression()
    model.fit(x_array, y_array)

    predicted_y = model.predict(x_array)

    len_x = len(x)
    r_squared = model.score(x_array, y_array) # 0-1 - 0.7 - 1.0 celkem dobry

    last_y = y_array[-1]
    if (predicted_y[-2] == 0):
        return [0, 0]
    growth_rate = ((predicted_y[-1] - predicted_y[-2]) / abs(predicted_y[-2]))

    return [growth_rate, r_squared]

def estimate_growth_stocks(input_ticker_id_list=None):
    logger.info(f"estimate_growth_stocks - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        tickers = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            tickers = input_ticker_id_list

        counter = 0
        for ticker_id in tickers:

            counter += 1
            logger.info(f"estimate_growth_stocks {counter}/{len(tickers)}")

            #ticker_id = 'AAPL'
            years_back = 5
            y_net_income_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME, years_back)
            y_revenue_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE, years_back)
            y_flow_cont_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION, years_back)
            y_ebitda_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA, years_back)
            y_fcf_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF, years_back)
            y_gross_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT, years_back)
            y_shares_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING, 3)

            quarters_back = years_back * 4 + 4
            q_net_income_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_Q, quarters_back)
            q_revenue_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, quarters_back)
            q_flow_cont_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION_Q, quarters_back)
            q_ebitda_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA_Q, quarters_back)
            q_fcf_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, quarters_back)
            q_gross_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_Q, quarters_back)

            q_shares_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING_Q, quarters_back) #2 years * 52 weeks * 5 days
            growth_shares = None

            shares_growth_per_year = None

            if len(y_revenue_list) < years_back or len(y_flow_cont_list) < years_back or len(y_fcf_list) < years_back or len(y_gross_list) < years_back:
                dict_data = {
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__GROWTH_RATE: -999,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__GROWTH_RATE_COMBINED: -999,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__GROWTH_RATE_STABILITY: -999
                }
                dao_tickers.update_ticker_types(ticker_id, dict_data, True)
                continue

            prepared_list = prepare_growth_data(q_shares_list)
            if prepared_list != None: 
                shares_growth_per_year = predict_growth_rate(prepared_list[0], prepared_list[1])[0] * 4

            #if len(y_shares_list) > 250 and isinstance(y_shares_list[0].value, (int, float)) and isinstance(y_shares_list[-1].value, (int, float) ) and y_shares_list[-1].value != 0:
            #    percent_change = (y_shares_list[0].value - y_shares_list[-1].value)/y_shares_list[-1].value
            #    shares_growth_per_year = percent_change * (52 * 5) / len(y_shares_list) # 52 weeks * 5 days
            
            if shares_growth_per_year != None:
                #print(f"Shares({ticker_id}): {shares_growth_per_year}")
                pass
            
            growth_list = []
            r_square_list = []
            
            prepared_list = prepare_growth_data_TTM(q_net_income_list)
            if prepared_list != None: 
                growth_net_income = predict_growth_rate(prepared_list[0], prepared_list[1])
                #growth_list.append(growth_net_income[0] * 4)
                #r_square_list.append(growth_net_income[1])
                #print(f"Net income({ticker_id}): {growth_net_income}")

            prepared_list = prepare_growth_data_TTM(q_revenue_list)
            if prepared_list != None: 
                growth_revenue = predict_growth_rate(prepared_list[0], prepared_list[1])
                growth_list.append(growth_revenue[0] * 4)
                r_square_list.append(growth_revenue[1])
                #print(f"Revenue({ticker_id}): {growth_revenue}")

            prepared_list = prepare_growth_data_TTM(q_flow_cont_list)
            if prepared_list != None: 
                cont_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
                growth_list.append(cont_growth[0] * 4)
                r_square_list.append(cont_growth[1])
                #print(f"Cont_FLOW({ticker_id}): {cont_growth}")

            prepared_list = prepare_growth_data_TTM(q_ebitda_list)
            if prepared_list != None: 
                ebitda_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
                #growth_list.append(ebitda_growth[0] * 4)
                #r_square_list.append(ebitda_growth[1])
                #print(f"EBITDA({ticker_id}): {ebitda_growth}")

            prepared_list = prepare_growth_data_TTM(q_fcf_list)
            if prepared_list != None: 
                fcf_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
                growth_list.append(fcf_growth[0] * 4)
                r_square_list.append(fcf_growth[1])
                #print(f"FCF({ticker_id}): {fcf_growth}")

            prepared_list = prepare_growth_data_TTM(q_gross_list)
            if prepared_list != None: 
                gross_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
                growth_list.append(gross_growth[0] * 4)
                r_square_list.append(gross_growth[1])
                #print(f"Gross({ticker_id}): {gross_growth}")

            data = {
                'Growth Rate': growth_list,  # Rustove koeficienty
                'R-squared': r_square_list  # Hodnoty R^2
            }

            if len(growth_list) != 4:
                continue

            df = pd.DataFrame(data)
            df['Weighted Growth'] = df['Growth Rate'] * df['R-squared']
            weighted_average_growth_a = df['Weighted Growth'].sum() / df['R-squared'].sum()
            if shares_growth_per_year != None:
                weighted_average_growth_a = ((1 + weighted_average_growth_a) / (1 + shares_growth_per_year) ) - 1
            #print(f"Final growth Annual({ticker.ticker_id}): {weighted_average_growth_a}")
            stability_a = df['R-squared'].sum()
            #print(f"Stability Annual({ticker_id}): {stability_a}")

            growth_list = []
            r_square_list = []

            prepared_list = prepare_growth_data(q_net_income_list)
            if prepared_list != None: 
                growth_net_income = predict_growth_rate(prepared_list[0], prepared_list[1])
                #growth_list.append(growth_net_income[0])
                #r_square_list.append(growth_net_income[1] ** 2)
                #print(f"Net income({ticker_id}): {growth_net_income}")

            prepared_list = prepare_growth_data(q_revenue_list)
            if prepared_list != None: 
                growth_revenue = predict_growth_rate(prepared_list[0], prepared_list[1])
                growth_list.append(growth_revenue[0])
                r_square_list.append(growth_revenue[1] ** 2)
                #print(f"Revenue({ticker_id}): {growth_revenue}")

            prepared_list = prepare_growth_data(q_flow_cont_list)
            if prepared_list != None: 
                cont_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
                growth_list.append(cont_growth[0])
                r_square_list.append(cont_growth[1] ** 2)
                #print(f"Cont_FLOW({ticker_id}): {cont_growth}")

            prepared_list = prepare_growth_data(q_ebitda_list)
            if prepared_list != None: 
                ebitda_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
                #growth_list.append(ebitda_growth[0])
                #r_square_list.append(ebitda_growth[1] ** 2)
                #print(f"EBITDA({ticker_id}): {ebitda_growth}")

            prepared_list = prepare_growth_data(q_fcf_list)
            if prepared_list != None: 
                fcf_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
                growth_list.append(fcf_growth[0])
                r_square_list.append(fcf_growth[1] ** 2)
                #print(f"FCF({ticker_id}): {fcf_growth}")

            prepared_list = prepare_growth_data(q_gross_list)
            if prepared_list != None: 
                gross_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
                growth_list.append(gross_growth[0])
                r_square_list.append(gross_growth[1] ** 2)
                #print(f"Gross({ticker_id}): {gross_growth}")

            if len(growth_list) != 4:
                continue

            data = {
                'Growth Rate': growth_list,  # Rustove koeficienty
                'R-squared': r_square_list  # Hodnoty R^2
            }

            # Vytvoreni DataFrame z dat
            df = pd.DataFrame(data)
            df['Weighted Growth'] = df['Growth Rate'] * df['R-squared']
            weighted_average_growth_Q = df['Weighted Growth'].sum() / df['R-squared'].sum()
            stability_q = df['R-squared'].sum()
            #print(f"Final growth Quaterly({ticker.ticker_id}): {weighted_average_growth_Q *4}")
            #print(f"Stability Quaterly({stability_q}")
            growth_rate_combined = (weighted_average_growth_Q *4* 0.2) + (weighted_average_growth_a * 0.8)
            #print(f"Final growth({ticker_id}) : {growth_rate_combined}")

            dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE__ANNUAL, weighted_average_growth_a, y_revenue_list[0].date)
            dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE__QUATERLY, weighted_average_growth_Q, q_revenue_list[0].date)
            dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE_STABILITY__ANNUAL, stability_a, y_revenue_list[0].date)
            dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE_COMBINED__ANNUAL, growth_rate_combined, y_revenue_list[0].date)
            dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE_STABILITY__QUATERLY, stability_q, q_revenue_list[0].date)

            dict_data = {
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__GROWTH_RATE: weighted_average_growth_a,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__GROWTH_RATE_COMBINED: growth_rate_combined,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__GROWTH_RATE_STABILITY: stability_a
            }

            dao_tickers.update_ticker_types(ticker_id, dict_data, True)
            logger.info(f"Updated GROWTH on {ticker_id}")
    except Exception as e:
        logger.error(f"estimate_growth_stocks - Error {e}")
        traceback.print_exc()
    logger.info(f"estimate_growth_stocks - End")

def get_price_discount_z_score(dao_tickers_data : DAO_TickersData, ticker_id:str, length: int) -> []:
    try:
        list_prices = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, length+length)
        list_volumes = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.VOLUME, length+length)

        if len(list_prices) >= length:
            prices_list = []
            volumes_list = []
            for price, volume in zip(list_prices, list_volumes):
                prices_list.append(price.value)
                volumes_list.append(volume.value)

            vwma = []
            prices = np.array(prices_list)
            volumes = np.array(volumes_list)
            for i in range(len(prices) - length + 1):
                price_slice = prices[i:i+length]
                volume_slice = volumes[i:i+length]
                vwma_value = np.sum(price_slice * volume_slice) / np.sum(volume_slice)
                vwma.append(vwma_value)
            
            if math.isnan(vwma[0]):
                return None

            prices_list = prices_list[:length+1]
            vwma_price_diffs = [price - vwma for price, vwma in zip(prices_list, vwma)]

            std = np.std(vwma_price_diffs)
            zscores = stats.zscore(vwma_price_diffs)
            probabilities = stats.norm.pdf(zscores)  # Use PDF for probability density
            #probabilities = [0]

            discount = (vwma[0] - prices_list[0])/vwma[0]

            if math.isnan(probabilities[0]):
                return [1.0, discount]
            else:
                return [probabilities[0], discount]
    finally:
        pass

def calculate_price_discount(input_ticker_id_list=None):
    logger.info(f"calculate_price_discount - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        tickers = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            tickers = input_ticker_id_list

        skip = True
        counter = 0
        for ticker_id in tickers:
            #ticker.ticker_id = 'PFG'
            #if ticker_id == 'PFG':
            #    skip = False
            #if skip:
            #    continue

            counter += 1

            result100 = get_price_discount_z_score(dao_tickers_data, ticker_id, 100)
            if result100 == None:
                continue
            prob100 = result100[0]
            discount100 = result100[1]
            

            result200 = get_price_discount_z_score(dao_tickers_data, ticker_id, 200)
            if result200 == None:
                continue
            prob200 = result200[0]
            discount200 = result200[1]

            result500 = get_price_discount_z_score(dao_tickers_data, ticker_id, 500)
            if result500 == None:
                continue
            prob500 = result500[0]
            discount500 = result500[1]

            if (discount500 == None or discount100 == None or discount200 == None or math.isnan(discount500) or math.isnan(discount100) or math.isnan(discount200)):
                continue

            if (prob500 == None or prob100 == None or prob200 == None or math.isnan(prob500) or math.isnan(prob100) or math.isnan(prob200)):
                continue

            if discount100 < 0:
                prob100 *= -1
            if discount200 < 0:
                prob200 *= -1
            if discount500 < 0:
                prob500 *= -1

            years_count = 3

            #pe_mean_stdev = dao_tickers_data.select_ticker_data_mean_stdev(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS, years_count * 365)
            #pb_mean_stdev = dao_tickers_data.select_ticker_data_mean_stdev(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS, years_count * 365)
            #pfcf_mean_stdev = dao_tickers_data.select_ticker_data_mean_stdev(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS, years_count * 365)

            #pb_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PB, 1)
            #pfcf_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, 4)

            pe_zscore = None
            pfcf_zscore = None
            pb_zscore = None


            dict_data = {
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_DISCOUNT_1: discount100,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_DISCOUNT_2: discount200,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_DISCOUNT_3: discount500,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_PROB_1: prob100,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_PROB_2: prob200,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_PROB_3: prob500
            }
            dao_tickers.update_ticker_types(ticker_id, dict_data, True)


            priceList = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, 1000)

            if len(priceList) > 30:
                maxPrice = max(price.value for price in priceList)
                lastPrice = priceList[0].value
                monthPrice = priceList[21].value
                weekPrice = priceList[5].value
                dayPrice = priceList[1].value

                athDiscount = (maxPrice - lastPrice) / maxPrice if maxPrice != 0 else 0
                monthDiscount = (monthPrice - lastPrice) / monthPrice if monthPrice != 0 else 0
                weekDiscount = (weekPrice - lastPrice) / weekPrice if weekPrice != 0 else 0
                dayDiscount = (dayPrice - lastPrice) / dayPrice if dayPrice != 0 else 0

                dict_data = {
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__ATH_DISCOUNT: athDiscount,
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__MONTH_DISCOUNT: monthDiscount,
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__WEEK_DISCOUNT: weekDiscount,
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__DAY_DISCOUNT: dayDiscount
                }
                dao_tickers.update_ticker_types(ticker_id, dict_data, True)

            logger.info(f"Discount({ticker_id}) {counter}/{len(tickers)}")
    except Exception as e:
        logger.error(f"calculate_price_discount - Error {e}")
        traceback.print_exc()
    logger.info(f"calculate_price_discount - End")

def calc_valuation_ratios_stocks(input_ticker_id_list=None):

    logger.info(f"calc_valuation_ratios_stocks - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)
        fmp = FMP()

        statement_list = fmp.get_statement_symbols_list()

        tickers = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            tickers = input_ticker_id_list

        metrics : list[FMP_Metrics] = None
        metric : FMP_Metrics = None
        todayDate = date.today()
        counter = 0
        for ticker_id in tickers:
            counter += 1
            #ticker_id = 'NVDA'

            if ticker_id not in statement_list:
                continue
            try:
                prices_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, -1)
                prices_list.sort(key=lambda x: x.date, reverse=False)
                shares_list_q = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING_Q, -1)
                shares_list_q.sort(key=lambda x: x.date, reverse=False)
                
                market_cap_list_q = []

                # PE
                eps_list_q = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS_Q, -1)
                eps_list_q.sort(key=lambda x: x.date, reverse=False)
                pe_list_q = []
                last_record = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q, 1)

                if len(eps_list_q) > 3:
                    for eps_index in range(3, len(eps_list_q)):
                        for price_index in range(0, len(prices_list)):
                            if prices_list[price_index].date >= eps_list_q[eps_index].date:
                                price = None
                                if prices_list[price_index].date == eps_list_q[eps_index].date:
                                    price = prices_list[price_index]
                                else:
                                    price = prices_list[price_index-1]
                                
                                eps_value = eps_list_q[eps_index - 3].value + eps_list_q[eps_index - 2].value + eps_list_q[eps_index - 1].value + eps_list_q[eps_index].value
                                pe = ROW_TickersData()
                                pe.type = TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q
                                pe.date = eps_list_q[eps_index].date
                                pe.ticker_id = ticker_id
                                if eps_value != 0:
                                    pe.value = price.value / eps_value
                                else:
                                    pe.value = 0
                                if len(last_record) == 0 or last_record[-1].date < pe.date:
                                    pe_list_q.append(pe)
                                break

                # PS
                revenue_list_q = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, -1)
                revenue_list_q.sort(key=lambda x: x.date, reverse=False)
                ps_list_q = []
                last_record = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q, 1)

                if len(revenue_list_q) > 3:
                    for rev_index in range(3, len(revenue_list_q)):
                        shares = 0
                        for shares_index in range(0, len(shares_list_q)):
                            if revenue_list_q[rev_index].date == shares_list_q[shares_index].date:
                                shares = shares_list_q[shares_index].value
                                break
                        
                        for price_index in range(0, len(prices_list)):
                            if prices_list[price_index].date >= revenue_list_q[rev_index].date:
                                price = None
                                if prices_list[price_index].date == revenue_list_q[rev_index].date:
                                    price = prices_list[price_index]
                                else:
                                    price = prices_list[price_index-1]
                                
                                rev_value = revenue_list_q[rev_index - 3].value + revenue_list_q[rev_index - 2].value + revenue_list_q[rev_index - 1].value + revenue_list_q[rev_index].value
                                ps = ROW_TickersData()
                                ps.type = TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q
                                ps.date = revenue_list_q[rev_index].date
                                ps.ticker_id = ticker_id
                                if rev_value != 0 and shares != 0:
                                    ps.value = price.value / (rev_value / shares)
                                else:
                                    ps.value = 0
                                if len(last_record) == 0 or last_record[-1].date < ps.date:
                                    ps_list_q.append(ps)
                                break

                # PB
                book_list_q = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.STOCKHOLDER_EQUITY_Q, -1)
                book_list_q.sort(key=lambda x: x.date, reverse=False)

                pb_list_q = []
                last_record = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q, 1)

                if len(book_list_q) > 0:
                    for pb_index in range(0, len(book_list_q)):
                        shares = 0
                        for shares_index in range(0, len(shares_list_q)):
                            if book_list_q[pb_index].date == shares_list_q[shares_index].date:
                                shares = shares_list_q[shares_index].value
                                break
                        
                        for price_index in range(0, len(prices_list)):
                            if prices_list[price_index].date >= book_list_q[pb_index].date:
                                price = None
                                if prices_list[price_index].date == book_list_q[pb_index].date:
                                    price = prices_list[price_index]
                                else:
                                    price = prices_list[price_index-1]
                                
                                book_value = book_list_q[pb_index].value
                                pb = ROW_TickersData()
                                pb.type = TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q
                                pb.date = book_list_q[pb_index].date
                                pb.ticker_id = ticker_id
                                if book_value != 0 and shares != 0:
                                    pb.value = price.value / (book_value / shares)
                                else:
                                    pb.value = 0
                                if len(last_record) == 0 or last_record[-1].date < pb.date:
                                    pb_list_q.append(pb)
                                break

                #PFCF
                fcf_list_q = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, -1)
                fcf_list_q.sort(key=lambda x: x.date, reverse=False)

                pfcf_list_q = []
                last_record = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q, 1)

                if len(fcf_list_q) > 3:
                    for fcf_index in range(3, len(fcf_list_q)):
                        shares = 0
                        for shares_index in range(0, len(shares_list_q)):
                            if fcf_list_q[fcf_index].date == shares_list_q[shares_index].date:
                                shares = shares_list_q[shares_index].value
                                break
                        
                        for price_index in range(0, len(prices_list)):
                            if prices_list[price_index].date >= fcf_list_q[fcf_index].date:
                                price = None
                                if prices_list[price_index].date == fcf_list_q[fcf_index].date:
                                    price = prices_list[price_index]
                                else:
                                    price = prices_list[price_index-1]
                                
                                fcf_value = fcf_list_q[fcf_index - 3].value + fcf_list_q[fcf_index - 2].value + fcf_list_q[fcf_index - 1].value + fcf_list_q[fcf_index].value
                                pfcf = ROW_TickersData()
                                pfcf.type = TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q
                                pfcf.date = fcf_list_q[fcf_index].date
                                pfcf.ticker_id = ticker_id
                                if fcf_value != 0  and shares != 0:
                                    pfcf.value = price.value / (fcf_value / shares)
                                else:
                                    pfcf.value = 0
                                if len(last_record) == 0 or last_record[-1].date < pfcf.date:
                                    pfcf_list_q.append(pfcf)
                                break
                
                dao_tickers_data.bulk_insert_ticker_data(pe_list_q, True)
                dao_tickers_data.bulk_insert_ticker_data(ps_list_q, True)
                dao_tickers_data.bulk_insert_ticker_data(pb_list_q, True)
                dao_tickers_data.bulk_insert_ticker_data(pfcf_list_q, True)
                

            except FMPException_LimitReached as e:
                logger.warning("Limit reached, job stopped.")
                return
            
            logger.info(f"calc_valuation_ratios_stocks ({ticker_id} - {counter}/{len(tickers)})")
    except Exception as e:
        logger.error(f"calc_valuation_ratios_stocks - Error {e}")
        traceback.print_exc()
    logger.info(f"calc_valuation_ratios_stocks - End")

def calc_valuation_stocks(input_ticker_id_list=None):

    logger.info(f"calc_valuation_stocks - Start")
    try:

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            db_ticker_list = input_ticker_id_list
        today = datetime.today().date()

        wanted_return = 0.1 # 10 %
        margin_of_safety = 0.1 # 10 %
        perp_growth = 0.03 # 3%

        safe_target_price_eps = 0
        safe_target_price_fcf = 0

        counter = 0
        for ticker_id in db_ticker_list:
            counter += 1
            logger.info(f"calc_valuation_stocks - {ticker_id} {counter}/{len(db_ticker_list)}")

            ticker = dao_tickers.select_ticker(ticker_id)
            growth = ticker.growth_rate
            cash = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_Q, 1)
            shares = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING_Q, 1)
            total_debt = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_DEBT_Q, 1)

            if len(shares) == 0 or len(cash) == 0 or len(total_debt) == 0 or growth == None or shares[0].value == 0 or ticker.growth_rate < 0:
                dict_data = {
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__EPS_VALUATION: 0,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__FCF_VALUATION: 0
                }

                dao_tickers.update_ticker_types(ticker_id, dict_data, True)
                continue

            #growth *= 0.85
            # EPS valuation
            eps_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS_Q, 4)
            if len(eps_list) == 4:
                eps_value = eps_list[0].value + eps_list[1].value + eps_list[2].value + eps_list[3].value
                pe_result = dao_tickers_data.select_ticker_data_mean_stdev(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS, 5 * 250)
                if pe_result != None:
                    pe_ratio_mean = pe_result[0]
                    if pe_ratio_mean == None:
                        continue
                    cash_per_share = 0

                    if len(cash) > 0 and len(shares) > 0 and shares[0].value > 0:
                        cash_per_share = cash[0].value / shares[0].value 


                        future_eps = eps_value * (1 + growth) ** 3
                        future_price = future_eps * pe_ratio_mean
                        target_price = future_price/((1+wanted_return) ** 3)

                        safe_target_price_eps = target_price * (1 - margin_of_safety)

            # DFCF valuation
            fcf_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, 4)
            if len(fcf_list) == 4:
                fcf_value = fcf_list[0].value + fcf_list[1].value + fcf_list[2].value + fcf_list[3].value

                future_fcf = fcf_value * (1 + growth) ** 9
                terminal_value = (future_fcf * (1 + perp_growth))/(wanted_return - perp_growth)
                terminal_value_price = terminal_value / ((1+wanted_return)**8)

                present_value_fcf = 0
                for year in range(1, 9 + 1):
                    future_fcf = fcf_value * (1 + growth) ** year
                    present_value_fcf += future_fcf / (1 + wanted_return) ** year

                total_present_value = present_value_fcf + terminal_value_price

                safe_target_price_fcf = (1 - margin_of_safety) * ((total_present_value + cash[0].value - total_debt[0].value) / shares[0].value)

            dict_data = {
                TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__EPS_VALUATION: safe_target_price_eps,
                TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__FCF_VALUATION: safe_target_price_fcf
            }

            dao_tickers.update_ticker_types(ticker_id, dict_data, True)
            #dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__EPS_VALUATION, safe_target_price_eps, today)
            #dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__FCF_VALUATION, safe_target_price_fcf, today)
    except Exception as e:
        logger.error(f"calc_valuation_stocks - Error {e}")
        traceback.print_exc()
    logger.info(f"calc_valuation_stocks - End")

def calculate_continuous_metrics(earning_metric_const: int, metric_continuous_const: int, input_ticker_id_list = None):
    logger.info(f"calculate_continuous_metrics({earning_metric_const}) - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        fmp = FMP()

        ticker_list_orig = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            ticker_list_orig = input_ticker_id_list
        statement_list = fmp.get_statement_symbols_list()
        #ticker_list_orig = ['NVDA']

        counter = 0
        for ticker_id in ticker_list_orig:
            counter += 1
            logger.info(f"calculate_continuous_metrics({earning_metric_const}) {counter}/{len(ticker_list_orig)}")
            #ticker_id = 'NVDA'
            #ticker : ROW_Tickers
            if ticker_id not in statement_list:
                continue

            #print(f"Continous Metric Ticker: {ticker_id}")

            years_back = 5
            price_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, years_back * 365)
            price_list.sort(key=lambda x: x.date, reverse=False)
            metric_list = dao_tickers_data.select_ticker_data(ticker_id, earning_metric_const, (years_back * 4) + 1)

            if earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS:
                shares_q_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING_Q, (years_back * 4) + 1)
                shares_q_list.sort(key=lambda x: x.date, reverse=False)
                for shares_q in shares_q_list:
                    metric = ROW_TickersData()
                    metric.date = shares_q.date
                    metric.value = shares_q.value
                    metric_list.append(metric)
            """
            if (earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q):
                pb_q_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q, years_back * (4 + 1))
                pb_q_list.sort(key=lambda x: x.date, reverse=False)
                for pb in pb_q_list:
                    metric = ROW_TickersData()
                    metric.date = pb.date
                    metric.value = pb.value
                    metric_list.append(metric)
                        
            if (earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q):
                ps_q_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q, years_back * (4 + 1))
                ps_q_list.sort(key=lambda x: x.date, reverse=False)
                for ps in ps_q_list:
                    metric = ROW_TickersData()
                    metric.date = ps.date
                    metric.value = ps.value
                    metric_list.append(metric)

            if (earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q):
                pe_q_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q, years_back * (4 + 1))
                pe_q_list.sort(key=lambda x: x.date, reverse=False)
                for pe in pe_q_list:
                    metric = ROW_TickersData()
                    metric.date = pe.date
                    metric.value = pe.value
                    metric_list.append(metric)

            if (earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q):
                pfcf_q_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q, years_back * (4 + 1))
                pfcf_q_list.sort(key=lambda x: x.date, reverse=False)
                for pfcf in pfcf_q_list:
                    metric = ROW_TickersData()
                    metric.date = pfcf.date
                    metric.value = pfcf.value
                    metric_list.append(metric)
            """

            metric_list.sort(key=lambda x: x.date, reverse=False)
            last_metric_continous_record = dao_tickers_data.select_ticker_data(ticker_id, metric_continuous_const, 1)

            metric_index = 0
            metric_length = len(metric_list)
            last_metric_record = None
            next_metric_record = None
            if metric_length > 0:
                next_metric_record = metric_list[metric_index]
            else:
                continue

            last_price = None

            last_metric_continous_date = None
            if len(last_metric_continous_record) > 0:
                last_metric_continous_date = last_metric_continous_record[0].date

            new_data = []

            for record in price_list:
                record : ROW_TickersData
            
                if next_metric_record != None and next_metric_record.date <= record.date:
                    last_metric_record = next_metric_record
                    last_price = record.value
                    #print(f"New last price({last_metric_record.date}): {last_price}")
                    metric_index += 1
                    if metric_length > metric_index:
                        next_metric_record = metric_list[metric_index]
                    else:
                        next_metric_record = None
                if last_metric_continous_date != None and last_metric_continous_date >= record.date:
                    continue

                if last_metric_record == None:
                    #print(f"Skipping {record.date}")
                    continue 

                if metric_continuous_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS:
                    metric = last_metric_record.value
                else:
                    metric = last_metric_record.value * (record.value / last_price)
                metric_record = ROW_TickersData()
                metric_record.date = record.date
                metric_record.ticker_id = ticker_id
                metric_record.type = metric_continuous_const
                metric_record.value = metric
                new_data.append(metric_record)
                #dao_tickers_data.store_ticker_data(metric_record.ticker_id, metric_record.type, metric_record.value, metric_record.date)
                #print(f"Metric({record.date}): {metric}")
            if (len(new_data) > 0):
                
                type_t = None
                if earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q:
                    type_t = TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PE
                if earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q:
                    type_t = TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PS
                if earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q:
                    type_t = TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PB
                if earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q:
                    type_t = TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PFCF

                if type_t != None:
                    dict_data = {
                        type_t: new_data[-1].value
                    }
                    dao_tickers.update_ticker_types(ticker_id, dict_data, True)
                dao_tickers_data.bulk_insert_ticker_data(new_data, True)
    except Exception as e:
        logger.error(f"calculate_continuous_metrics - Error {e}")
        traceback.print_exc()
    logger.info(f"calculate_continuous_metrics({earning_metric_const}) - End")

def analyze_option_sentiment(input_ticker_id_list=None, days=14, max_days_to_expiration=365, log_results=False):
    # Connect to the database

    conn = DB.get_connection_mysql()
    cursor = conn.cursor(dictionary=True)

    query = "SELECT ticker_id, price FROM tickers"
    if input_ticker_id_list:
        placeholders = ", ".join(["%s"] * len(input_ticker_id_list))
        query += " WHERE ticker_id IN (" + placeholders + ")"
        cursor.execute(query, tuple(input_ticker_id_list))
    else:
        cursor.execute(query)
    tickers_data = cursor.fetchall()

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days + 1)
    max_expiration = end_date + timedelta(days=max_days_to_expiration)

    for row in tickers_data:
        ticker = row['ticker_id']
        current_price = row['price']
        sentiments = []
        total_options_count = 0
        total_weight = 0

        for opt_type in ['P', 'C']:
            # Load option data (put or call)
            cursor.execute("""
                SELECT last_trade_date, option_id, last_price, oi, volume, strike, expiration
                FROM options
                WHERE ticker_id = %s AND type = %s AND last_trade_date BETWEEN %s AND %s AND expiration <= %s
                ORDER BY option_id, last_trade_date
            """, (ticker, opt_type, start_date, end_date, max_expiration))
            options = pd.DataFrame(cursor.fetchall())

            if options.empty or len(options.option_id.unique()) == 0:
                continue

            # Filter strikes: within ±10% range but at least 3 below and 3 above current price
            strikes_sorted = sorted(options['strike'].unique())
            lower_limit = current_price * 0.9
            upper_limit = current_price * 1.1
            near_strikes = [s for s in strikes_sorted if lower_limit <= s <= upper_limit]

            if len(near_strikes) < 6:
                continue  # not enough strikes

            center_idx = min(range(len(near_strikes)), key=lambda i: abs(near_strikes[i] - current_price))
            selected_strikes = near_strikes[max(0, center_idx - 3): center_idx + 4]
            options = options[options['strike'].isin(selected_strikes)]

            # Load historical stock prices from tickers_time_data (type = 103)
            cursor.execute("""
                SELECT date AS last_trade_date, value AS stock_price
                FROM tickers_time_data
                WHERE ticker_id = %s AND type = 103 AND date BETWEEN %s AND %s
            """, (ticker, start_date, end_date))
            stock_data = pd.DataFrame(cursor.fetchall())

            if stock_data.empty:
                continue

            # Merge options with stock prices
            merged = options.merge(stock_data, on="last_trade_date", how="left")

            for option_id in merged['option_id'].unique():
                df = merged[merged['option_id'] == option_id].sort_values("last_trade_date")
                if len(df) < 2:
                    continue

                prev, curr = df.iloc[-2], df.iloc[-1]

                delta_price = curr['last_price'] - prev['last_price']
                delta_oi = curr['oi'] - prev['oi']
                delta_stock = curr['stock_price'] - prev['stock_price']
                delta_volume = curr['volume'] - prev['volume']

                pct_price = abs(delta_price) / prev['last_price'] if prev['last_price'] else 0
                pct_oi = abs(delta_oi) / prev['oi'] if prev['oi'] else 0
                pct_stock = abs(delta_stock) / prev['stock_price'] if prev['stock_price'] else 0
                pct_volume = abs(delta_volume) / prev['volume'] if prev['volume'] else 0

                # Scoring based on percentage changes (hedge-style thresholds)
                strength = 1
                if pct_price > 0.15: strength += 1
                if pct_oi > 0.2: strength += 1
                if pct_stock > 0.01: strength += 1
                if pct_volume > 1.0: strength += 1
                if strength > 5: strength = 5

                # Sentiment classification logic with volume trend
                if opt_type == 'P':
                    if delta_price > 0 and delta_oi > 0 and delta_stock < 0 and delta_volume > 0:
                        sentiment = 0  # Bearish
                    elif delta_price < 0 and delta_oi > 0 and delta_stock > 0 and delta_volume > 0:
                        sentiment = 1  # Bullish
                    elif delta_price < 0 and delta_oi < 0 and delta_stock > 0 and delta_volume < 0:
                        sentiment = 1  # Bullish
                    elif delta_price > 0 and delta_oi < 0 and delta_stock < 0 and delta_volume < 0:
                        sentiment = 0  # Bearish
                    else:
                        continue
                elif opt_type == 'C':
                    if delta_price > 0 and delta_oi > 0 and delta_stock > 0 and delta_volume > 0:
                        sentiment = 1  # Bullish
                    elif delta_price < 0 and delta_oi > 0 and delta_stock < 0 and delta_volume > 0:
                        sentiment = 0  # Bearish
                    elif delta_price < 0 and delta_oi < 0 and delta_stock < 0 and delta_volume < 0:
                        sentiment = 0  # Bearish
                    elif delta_price > 0 and delta_oi < 0 and delta_stock > 0 and delta_volume < 0:
                        sentiment = 1  # Bullish
                    else:
                        continue

                weight = curr['oi']
                sentiments.append((sentiment, strength, weight))
                total_options_count += 1
                total_weight += weight

        if sentiments:
            df_sent = pd.DataFrame(sentiments, columns=["sentiment", "strength", "weight"])
            dominant = df_sent.groupby("sentiment").apply(lambda g: (g["strength"] * g["weight"]).sum() / g["weight"].sum()).sort_values(ascending=False)
            top = int(dominant.index[0])
            avg_strength = int(round(dominant.iloc[0]))

            cursor.execute("""
                UPDATE tickers
                SET option_sentiment = %s,
                    option_sentiment_strength = %s
                WHERE ticker_id = %s
            """, (top, avg_strength, ticker))
            conn.commit()

            if log_results:
                print(f"{ticker}: sentiment={top}, strength={avg_strength}, options={total_options_count}, total_weight={total_weight:.2f}")

    cursor.close()
    conn.close()

def calc_ratio_discounts(input_ticker_id_list=None):
    logger.info(f"calc_ratio_discounts - Start")
    try:

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
        if (input_ticker_id_list != None):
            db_ticker_list = input_ticker_id_list
        today = datetime.today().date()

        counter = 0
        for ticker_id in db_ticker_list:
            counter += 1
            logger.info(f"calc_ratio_discounts - {ticker_id} {counter}/{len(db_ticker_list)}")

            
            years = 5
            pe_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS, years * 250)
            pb_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS, years * 250)
            ps_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS, years * 250)
            pfcf_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS, years * 250)
            
            max = -9999999999
            min = 99999999999
            if len(pe_list) > 0:
                actual_value = pe_list[0].value
                for pe in pe_list:
                    if math.isnan(pe.value) or pe.value == None:
                        continue
                    if pe.value > max:
                        max = pe.value
                    if pe.value < min:
                        min = pe.value
                if abs(max) - abs(min) != 0:
                    dict_data = {
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PE_DISCOUNT: (1 - (abs(actual_value) - abs(min)) / (abs(max) - abs(min)))
                    }

                    dao_tickers.update_ticker_types(ticker_id, dict_data, True)

            max = -9999999999
            min = 99999999999
            if len(ps_list) > 0:
                actual_value = ps_list[0].value
                for ps in ps_list:
                    if math.isnan(ps.value) or ps.value == None:
                        continue
                    if ps.value > max:
                        max = ps.value
                    if ps.value < min:
                        min = ps.value
                if abs(max) - abs(min) != 0:
                    dict_data = {
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PS_DISCOUNT: (1 - (abs(actual_value) - abs(min)) / (abs(max) - abs(min)))
                    }

                    dao_tickers.update_ticker_types(ticker_id, dict_data, True)

            max = -9999999999
            min = 99999999999
            if len(pb_list) > 0:
                actual_value = pb_list[0].value
                for pb in pb_list:
                    if math.isnan(pb.value) or pb.value == None:
                        continue
                    if pb.value > max:
                        max = pb.value
                    if pb.value < min:
                        min = pb.value

                if abs(max) - abs(min) != 0:
                    dict_data = {
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PB_DISCOUNT: (1 - (abs(actual_value) - abs(min)) / (abs(max) - abs(min)))
                    }

                    dao_tickers.update_ticker_types(ticker_id, dict_data, True)

            max = -9999999999
            min = 99999999999
            if len(pfcf_list) > 0:
                actual_value = pfcf_list[0].value
                for pfcf in pfcf_list:
                    if math.isnan(pfcf.value) or pfcf.value == None:
                        continue
                    if pfcf.value > max:
                        max = pfcf.value
                    if pfcf.value < min:
                        min = pfcf.value
                if abs(max) - abs(min) != 0:
                    dict_data = {
                        TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PFCF_DISCOUNT: (1 - (abs(actual_value) - abs(min)) / (abs(max) - abs(min)))
                    }

                    dao_tickers.update_ticker_types(ticker_id, dict_data, True)

    except Exception as e:
        logger.error(f"calc_ratio_discounts - Error {e}")
        traceback.print_exc()
    logger.info(f"calc_ratio_discounts - End")

def rank_stocks():
    logger.info("Rank Stocks Job started.")
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)
  
    skip = True
    ticker_list_orig = dao_tickers.select_tickers_all()

    map = {}
    ticker_list = []
    for ticker in ticker_list_orig:
        ticker : ROW_Tickers
        if ticker.growth_rate != None and ticker.growth_rate > 0 \
        and ticker.growth_rate_stability != None and ticker.growth_rate_stability > 3 \
        and ticker.pe_valuation != None \
        and ticker.price_discount_3 != None \
        and ticker.recomm_mean != None and ticker.recomm_mean < 3:
            ticker_list.append(ticker)

    ticker_list.sort(key=lambda x: (x.price_discount_3 if x.price_discount_3 is not None else -float('inf')), reverse=True)

    counter = 0
    for ticker in ticker_list:
        ticker : ROW_Tickers
        counter += 1
        entry = map.get(ticker)
        if entry == None:
            map[ticker] = counter
        else:
            map[ticker] = counter + map[ticker]

    ticker_list.sort(key=lambda x: (x.pe_valuation if x.pe_valuation is not None else -float('inf')), reverse=True)

    counter = 0
    for ticker in ticker_list:
        ticker : ROW_Tickers
        counter += 1
        entry = map.get(ticker)
        if entry == None:
            map[ticker] = counter
        else:
            map[ticker] = counter + map[ticker]

    ticker_list.sort(key=lambda x: (x.growth_rate_stability if x.growth_rate_stability is not None else -float('inf')), reverse=True)

    counter = 0
    for ticker in ticker_list:
        ticker : ROW_Tickers
        counter += 1
        entry = map.get(ticker)
        if entry == None:
            map[ticker] = counter
        else:
            map[ticker] = counter + map[ticker]

    ticker_list.sort(key=lambda x: (x.growth_rate if x.growth_rate is not None else -float('inf')), reverse=True)

    counter = 0
    for ticker in ticker_list:
        ticker : ROW_Tickers
        counter += 1
        entry = map.get(ticker)
        if entry == None:
            map[ticker] = counter
        else:
            map[ticker] = counter + map[ticker]

    ticker_list.sort(key=lambda x: (x.recomm_mean if x.recomm_mean is not None else float('inf')), reverse=False)

    counter = 0
    for ticker in ticker_list:
        ticker : ROW_Tickers
        counter += 1
        entry = map.get(ticker)
        if entry == None:
            map[ticker] = counter
        else:
            map[ticker] = counter + map[ticker]
    
    #ticker_list.sort(key=lambda x: (x.recomm_mean if x.recomm_mean is not None else float('inf')), reverse=False)

    #counter = 0
    #for ticker in ticker_list:
    #    ticker : ROW_Tickers
    #    counter += 1
    #    entry = map.get(ticker.ticker_id)
    #    if entry == None:
    #        map[ticker.ticker_id] = counter
    #    else:
    #        map[ticker.ticker_id] = counter + map[ticker.ticker_id]
    sorted_map = sorted(map.items(), key=lambda item: item[1])

    for ticker, count in sorted_map:
        if ticker.market_cap > 1000000000:
            print(f"{ticker.ticker_id} {ticker.sector} {ticker.industry}: {count}")

    logger.info("Rank Stocks Job finished.")

def growthProbability(ticker_id: str, days: int, percent_range: int):
    """
    Calculates and prints the probability of stock price changes exceeding certain thresholds
    over a specified number of days, useful for option strategies like the Wheel.
    Also displays a text-based histogram of the changes.

    Args:
        ticker_id: The stock ticker symbol.
        days: The number of days over which to calculate the price change.
        percent_range: The maximum percentage change threshold (positive and negative)
                       to calculate probabilities for.
    """
    logger.info(f"growthProbability({ticker_id}, {days}, {percent_range}) - Start")
    connection = None
    try:
        connection = DB.get_connection_mysql()
        dao_tickers_data = DAO_TickersData(connection)

        # Fetch all historical prices for the ticker
        prices_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, -1)
        num_prices = len(prices_list)

        if not prices_list:
            logger.warning(f"No price data found for ticker {ticker_id}")
            return

        # Sort prices by date in ascending order
        prices_list.sort(key=lambda x: x.date, reverse=False)

        changes_pct = []

        if num_prices <= days:
            logger.warning(f"Insufficient price data for {ticker_id} ({num_prices} points) to calculate {days}-day changes.")
            return

        # Calculate percentage changes over the specified number of days
        for i in range(num_prices - days):
            price_start = prices_list[i].value
            price_end = prices_list[i + days].value

            # Ensure prices are valid and avoid division by zero
            if price_start is not None and price_end is not None and price_start != 0:
                change = ((price_end - price_start) / price_start) * 100
                changes_pct.append(change)

        if not changes_pct:
            logger.warning(f"Could not calculate any percentage changes for {ticker_id}.")
            return

        changes_array = np.array(changes_pct)
        total_changes = len(changes_array)
        probabilities = {}

        # Calculate probabilities for exceeding each percentage threshold in the range
        for x in range(-percent_range, percent_range + 1):
            if x == 0:
                continue  # Skip 0%

            count = 0
            if x > 0:
                # Count changes GREATER THAN OR EQUAL TO x% (Price increases by at least x%)
                count = np.sum(changes_array >= x)
            elif x < 0:
                # Count changes LESS THAN OR EQUAL TO x% (Price decreases by at least |x|%)
                count = np.sum(changes_array <= x)

            if total_changes > 0:
                prob = (count / total_changes) * 100
                probabilities[x] = prob
            else:
                probabilities[x] = 0 # Should not happen if changes_pct is not empty

        logger.info(f"Calculated Probabilities of Price Exceeding Thresholds for {ticker_id} over {days} days:")
        # Sort results numerically from negative to positive
        sorted_keys = sorted(probabilities.keys())
        for x in sorted_keys:
                # Use standard print to output directly to console/output pane
                if x > 0:
                    # Probability of price increasing by AT LEAST x%
                    print(f"Probability of change >= {x}%: {probabilities[x]:.2f}%")
                else: # x < 0
                    # Probability of price decreasing by AT LEAST |x|% (i.e., change <= x%)
                    print(f"Probability of change <= {x}%: {probabilities[x]:.2f}%")


        # Print statistics
        print(f"\n--- Statistics ---")
        print(f"Total prices loaded from DB: {num_prices}")
        print(f"Historical {days}-day periods analyzed: {total_changes}")
        if total_changes > 0:
            mean_change = np.mean(changes_array)
            std_dev = np.std(changes_array)
            std_dev_2 = 2 * std_dev
            min_change = np.min(changes_array)
            max_change = np.max(changes_array)

            print(f"Average {days}-day change: {mean_change:.2f}%")
            print(f"1st Standard Deviation of {days}-day change: {std_dev:.2f}%")
            print(f"2nd Standard Deviation of {days}-day change: {std_dev_2:.2f}%")
            print(f"Minimum {days}-day change: {min_change:.2f}%")
            print(f"Maximum {days}-day change: {max_change:.2f}%")

            # --- Text Histogram ---
            print(f"\n--- Text Histogram of {days}-day % Changes ---")
            num_bins = 100  # Adjust number of bins as needed
            try:
                # Use np.histogram to bin the data
                counts, bin_edges = np.histogram(changes_array, bins=num_bins)
                max_count = np.max(counts) if counts.size > 0 else 0
                max_asterisks = 100 # Max width of the histogram bars

                # Find bin indices for mean and standard deviations
                mean_bin = np.digitize(mean_change, bin_edges) - 1
                sd1_minus_bin = np.digitize(mean_change - std_dev, bin_edges) - 1
                sd1_plus_bin = np.digitize(mean_change + std_dev, bin_edges) - 1
                sd2_minus_bin = np.digitize(mean_change - std_dev_2, bin_edges) - 1
                sd2_plus_bin = np.digitize(mean_change + std_dev_2, bin_edges) - 1

                # Ensure indices are within valid range [0, num_bins-1]
                valid_indices = range(num_bins)
                mean_bin = mean_bin if mean_bin in valid_indices else -1 # Use -1 if outside range
                sd1_minus_bin = sd1_minus_bin if sd1_minus_bin in valid_indices else -1
                sd1_plus_bin = sd1_plus_bin if sd1_plus_bin in valid_indices else -1
                sd2_minus_bin = sd2_minus_bin if sd2_minus_bin in valid_indices else -1
                sd2_plus_bin = sd2_plus_bin if sd2_plus_bin in valid_indices else -1


                for i in range(num_bins):
                    bin_start = bin_edges[i]
                    bin_end = bin_edges[i+1]
                    count = counts[i]

                    # Scale the number of asterisks
                    if max_count > 0:
                        num_asterisks = int((count / max_count) * max_asterisks)
                    else:
                        num_asterisks = 0
                    asterisks = '*' * num_asterisks

                    # Prepare markers
                    markers = ""
                    if i == mean_bin: markers += " | MEAN"
                    if i == sd1_minus_bin: markers += " | -1SD"
                    if i == sd1_plus_bin: markers += " | +1SD"
                    if i == sd2_minus_bin: markers += " | -2SD"
                    if i == sd2_plus_bin: markers += " | +2SD"

                    # Print bin range, asterisks, and markers
                    print(f"{bin_start:>7.2f}% to {bin_end:>7.2f}% | {asterisks}{markers}")

            except Exception as hist_err:
                 logger.error(f"Error generating histogram for {ticker_id}: {hist_err}")


    except Exception as e:
        logger.error(f"growthProbability - Error processing {ticker_id}: {e}")
        traceback.print_exc()
    finally:
        # Ensure the database connection is closed
        if connection and connection.is_connected():
            connection.close()
            logger.debug("Database connection closed.")
    logger.info(f"growthProbability({ticker_id}) - End")

def start_import_schedulers():
        #scheduler.add_job(notify_earnings, 'cron', second='*/10')

        DEV_MODE = os.getenv('DEV_MODE').lower() == "true" 

        if DEV_MODE == False:
            #scheduler.add_job(sync_ticker_id_list, 'cron', day_of_week='sun', hour=3, minute=30)
            scheduler.add_job(update_ticker_profile, 'cron', day_of_week='sun', hour=3, minute=30, args=[False])
            scheduler.add_job(update_earnings_calendar, 'cron',day_of_week='sun', hour=3, minute=30)
            
            scheduler.add_job(download_prices, 'cron',day_of_week='tue-sat', hour=0, minute=30)
            scheduler.add_job(update_ticker_target_price, 'cron',day_of_week='tue-sat', hour=0, minute=30)
            scheduler.add_job(update_stock_recommendations, 'cron',day_of_week='tue-sat', hour=0, minute=30)
            scheduler.add_job(update_stock_predictions, 'cron',day_of_week='sat', hour=3, minute=30)
            scheduler.add_job(downloadStockOptionData, 'cron',day_of_week='tue-sat', hour=0, minute=30)
            scheduler.add_job(download_fundamental_statements, 'cron', day_of_week='wed,sat', hour=6, minute=30)

            scheduler.add_job(estimate_growth_stocks, 'cron',day_of_week='tue-sat', hour=12, minute=30)
            scheduler.add_job(calculate_price_discount, 'cron',day_of_week='tue-sat', hour=12, minute=30)
            scheduler.add_job(calc_valuation_ratios_stocks, 'cron',day_of_week='tue-sat', hour=12, minute=30)
            scheduler.add_job(calc_valuation_stocks, 'cron',day_of_week='tue-sat', hour=12, minute=30)

            scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=12, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS])
            scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=12, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS])
            scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=12, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS])
            scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=12, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS])
            scheduler.add_job(analyze_option_sentiment, 'cron', day_of_week='tue-sat', hour=12, minute=30)

            scheduler.add_job(calc_ratio_discounts, 'cron',day_of_week='tue-sat', hour=12, minute=30)

            scheduler.start()

            logger.info("Schedulers started v3.")
        
        if DEV_MODE == True:
            #update_ticker_profile(False)
            #update_earnings_calendar()
            #download_prices()
            #update_ticker_target_price()
            #update_stock_recommendations()
            #update_stock_predictions()
            #downloadStockOptionData()
            #download_fundamental_statements()
            #estimate_growth_stocks()
            #calculate_price_discount()
            #calc_valuation_ratios_stocks()
            #calc_valuation_stocks()
            #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS)
            #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS)
            #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS)
            #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS)
            #analyze_option_sentiment()
            #calc_ratio_discounts()
            #growthProbability("FLR", 5, 20) # Example call with AAPL, 5 days, +/- 10% range
            
            pass

        

        