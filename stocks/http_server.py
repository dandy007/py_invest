import http.server
import socketserver
import logging
import datetime
import yfinance as yf
import time
import mysql.connector
from db import DAO_Tickers, ROW_Tickers, DB, ROW_TickersData, DAO_TickersData

from data_providers.alpha_vantage import get_tickers_download, get_earnings_calendar
from apscheduler.schedulers.background import BackgroundScheduler
from exporters.ical_exporter import export_earnings

PORT = 8080

Handler = http.server.SimpleHTTPRequestHandler
logging.basicConfig()
#logging.getLogger('apscheduler').setLevel(logging.DEBUG)


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

    ticker_list = dao_tickers.select_tickers_all()
    for row in ticker_list:
        row:ROW_Tickers

        #stock = yf.Ticker(row.ticker_id)
        try:
            #row.sector = stock.info['sector']
            #row.industry = stock.info['industry']
            #row.isin = stock.isin
            #dao_tickers.update_ticker(row, True)
            #print(f"Ticker {row.ticker_id} updated")

            today_data = dao_tickers_data.select_ticker_data(row.ticker_id, datetime.date.today())
            if today_data == None: 
                time.sleep(5)
                stock = yf.Ticker(row.ticker_id)
                row_data = ROW_TickersData(
                    stock.info.get('payoutRatio', 0),
                    datetime.date.today(),
                    row.ticker_id,
                    stock.info.get('marketCap', 0),
                    stock.info.get('currentPrice', 0),
                    stock.info.get('targetMeanPrice', 0),
                    stock.info.get('trailingEps', 0),
                    stock.info.get('sharesOutstanding', 0),
                    stock.info.get('recommendationMean', 0),
                    stock.recommendations_summary.strongBuy[0],
                    stock.recommendations_summary.buy[0],
                    stock.recommendations_summary.hold[0],
                    stock.recommendations_summary.sell[0],
                    stock.recommendations_summary.strongSell[0],
                    stock.info.get('dividendYield', 0)
                )
                dao_tickers_data.insert_ticker_data(row_data, True)
                print(f"Ticker data {row.ticker_id} inserted")
            else:
                print(f"Ticker data {row.ticker_id} skipped")
                continue

        except Exception as err:
            print(f"Error updating tickers[{row.ticker_id}]:", err)
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
