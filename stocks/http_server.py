import http.server
import socketserver
import logging
from logging.handlers import RotatingFileHandler
import datetime
import yfinance as yf
import time
import mysql.connector
import requests
from lxml import html
import pandas as pd
from db import DAO_Tickers, ROW_Tickers, DB, ROW_TickersData, DAO_TickersData, TICKERS_TIME_DATA__TYPE__CONST, FUNDAMENTAL_NAME__TO_TYPE__ANNUAL, FUNDAMENTAL_NAME__TO_TYPE__QUATERLY

from data_providers.alpha_vantage import get_tickers_download, get_earnings_calendar
from apscheduler.schedulers.background import BackgroundScheduler
from exporters.ical_exporter import export_earnings




# Create a custom logger
logger = logging.getLogger('invest_logger')
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


PORT = 8080

Handler = http.server.SimpleHTTPRequestHandler
#logging.basicConfig()
#logging.getLogger('apscheduler').setLevel(logging.DEBUG)

#logging.basicConfig(filename='invest.log', filemode='w', level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')




def notify_earnings():
    print(f"Task Notify_Earnings executed at: {datetime.datetime.now()}")
    tickers = ['GOOGL','META','AAPL','AMZN','MSFT','BRK-B','V','PLTR','NVDA','PYPL','DIS','MPW','UFPI','O','VICI','BTI','NXST','TSM','VRTX','CMCSA']

    calendar = get_earnings_calendar()

    export_earnings(tickers)
    #for earning in calendar:
    #    if earning[0] in tickers:
    #        print(f'Ticker: {earning[0]} Date {earning[2]}')


def downloadStockData():

    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    #stock = yf.Ticker('AACT-U')
    
    skip = True
    ticker_list = dao_tickers.select_tickers_all()
    for ticker in ticker_list:
        ticker:ROW_Tickers

        if ticker.ticker_id == 'A':
            skip = False

        if skip:
            continue

        time.sleep(2)
        #ticker.ticker_id = 'A'
        stock = yf.Ticker(ticker.ticker_id)
        try:
            name = stock.info.get('name', None)
            sector = stock.info.get('sector', None)
            industry = stock.info.get('industry', None)
            isin = stock.isin

            shares = stock.info.get('sharesOutstanding', None)
            enterpriseValue = stock.info.get('enterpriseValue', None)
            totalRevenue = stock.info.get('totalRevenue', None)
            price = stock.info.get('currentPrice', None)
            market_cap = stock.info.get('marketCap', None)

            if shares == None:
                logger.warning(f"Skipping {ticker.ticker_id}")
                continue

            ps = None

            if totalRevenue != None and market_cap != None:
                ps = market_cap / totalRevenue


            if name != None :
                ticker.name = name
            if sector != None :
                ticker.sector = sector
            if industry != None :
                ticker.industry = industry
            if isin != None:
                ticker.isin = isin

            dict_data = {
                TICKERS_TIME_DATA__TYPE__CONST.MARKET_CAP: market_cap,
                TICKERS_TIME_DATA__TYPE__CONST.ENTERPRISE_VALUE: enterpriseValue,
                TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING: shares,
                TICKERS_TIME_DATA__TYPE__CONST.PRICE: price,
                TICKERS_TIME_DATA__TYPE__CONST.TARGET_PRICE: stock.info.get('targetMeanPrice', None),
                TICKERS_TIME_DATA__TYPE__CONST.EPS: stock.info.get('trailingEps', None),
                TICKERS_TIME_DATA__TYPE__CONST.BOOK_PER_SHARE: stock.info.get('bookValue', None),
                TICKERS_TIME_DATA__TYPE__CONST.PE: stock.info.get('trailingPE', None),
                TICKERS_TIME_DATA__TYPE__CONST.PB: stock.info.get('priceToBook', None),
                TICKERS_TIME_DATA__TYPE__CONST.BETA: stock.info.get('beta', None),
                TICKERS_TIME_DATA__TYPE__CONST.VOLUME: stock.info.get('volume', None),
                TICKERS_TIME_DATA__TYPE__CONST.VOLUME_AVG: stock.info.get('averageVolume', None),
                TICKERS_TIME_DATA__TYPE__CONST.EV_EBITDA: stock.info.get('enterpriseToEbitda', None),
                TICKERS_TIME_DATA__TYPE__CONST.RECOMM_MEAN: stock.info.get('recommendationMean', None),
                TICKERS_TIME_DATA__TYPE__CONST.RECOMM_COUNT: stock.info.get('numberOfAnalystOpinions', None),
                TICKERS_TIME_DATA__TYPE__CONST.DIV_YIELD: stock.info.get('dividendYield', None),
                TICKERS_TIME_DATA__TYPE__CONST.PAYOUT_RATIO: stock.info.get('payoutRatio', None),
                TICKERS_TIME_DATA__TYPE__CONST.GROWTH_5Y: None,
                TICKERS_TIME_DATA__TYPE__CONST.ROA: stock.info.get('returnOnAssets', None),
                TICKERS_TIME_DATA__TYPE__CONST.ROE: stock.info.get('returnOnEquity', None),
                TICKERS_TIME_DATA__TYPE__CONST.PEG: stock.info.get('pegRatio', None),
                TICKERS_TIME_DATA__TYPE__CONST.PS: ps
            }

            for type, value in dict_data.items():
                if value not in (None, 'Infinity'):
                    dao_tickers_data.store_ticker_data(ticker.ticker_id, type, value, None)
                    
            dao_tickers.update_ticker_base_data(ticker, True)
            dao_tickers.update_ticker_types(ticker, dict_data, True)

            # Store fundamental statements data - annual
            pd.set_option('display.max_rows', None)
            statements = [stock.income_stmt, stock.balance_sheet, stock.cash_flow]
            
            for valuation_name, type in FUNDAMENTAL_NAME__TO_TYPE__ANNUAL.items():
                for statement in statements:
                    found = False
                    for date in statement.columns:
                        value = statement[date].get(valuation_name, None)
                        if value != None:
                            found = True
                            dao_tickers_data.store_ticker_data(ticker.ticker_id, type, value, date)
                    if found == True:
                        break

            # Store fundamental statements data - quaterly
            statements = [stock.quarterly_income_stmt, stock.quarterly_balance_sheet, stock.quarterly_cash_flow]
            for valuation_name, type in FUNDAMENTAL_NAME__TO_TYPE__QUATERLY.items():
                for statement in statements:
                    found = False
                    for date in statement.columns:
                        value = statement[date].get(valuation_name, None)
                        if value != None:
                            found = True
                            dao_tickers_data.store_ticker_data(ticker.ticker_id, type, value, date)
                    if found == True:
                        break

            logger.info(f"Updated {ticker.ticker_id}")

            #row.sector = stock.info['sector']
            #row.industry = stock.info['industry']
            #row.isin = stock.isin
            #dao_tickers.update_ticker(row, True)
            #print(f"Ticker {row.ticker_id} updated")

            #market_cap = stock.info.get('marketCap', None)
            #if market_cap != 0:
            #    ticker.market_cap = market_cap



            #today_data = dao_tickers_data.select_ticker_data(row.ticker_id, datetime.date.today())
            #if today_data == None: 
            #    time.sleep(5)
            #    stock = yf.Ticker(row.ticker_id)
            #    row_data = ROW_TickersData(
            #        stock.info.get('payoutRatio', 0),
            #        datetime.date.today(),
            #        row.ticker_id,
            #        stock.info.get('marketCap', 0),
            #        stock.info.get('currentPrice', 0),
            #        stock.info.get('targetMeanPrice', 0),
            #        stock.info.get('trailingEps', 0),
            #        stock.info.get('sharesOutstanding', 0),
            #        stock.info.get('recommendationMean', 0),
            #        stock.recommendations_summary.strongBuy[0],
            #        stock.recommendations_summary.buy[0],
            #        stock.recommendations_summary.hold[0],
            #        stock.recommendations_summary.sell[0],
            #        stock.recommendations_summary.strongSell[0],
            #        stock.info.get('dividendYield', 0)
            #    )
            #    dao_tickers_data.insert_ticker_data(row_data, True)
            #    print(f"Ticker data {row.ticker_id} inserted")
            #else:
            #    print(f"Ticker data {row.ticker_id} skipped")
            #    continue

        except Exception as err:
            logger.exception(f"Error updating ticker[{ticker.ticker_id}]:")
            continue
           


    

            


    


if __name__ == "__main__":
    #tickerList = get_tickers_download()
    #for row in tickerList:
    #    print(f"Ticker: {row}")
    scheduler = BackgroundScheduler()
    #scheduler.add_job(my_task, 'interval', seconds=10)

    #cron
    #minute='*/5': Execute the task every 5 minutes
    #hour='0-23/2': Execute the task every 2 hours (0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22)
    #day='*/2': Execute the task every other day
    #month='*': Execute the task every month
    #day_of_week='mon-fri': Execute the task only on weekdays

    #scheduler.add_job(notify_earnings, 'cron', second='*/10')
    #scheduler.add_job(downloadStockData, 'cron', day='*/1') # day_of_week='mon-fri'
    downloadStockData()
    #scheduler.start()

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
