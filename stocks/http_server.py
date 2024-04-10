import http.server
import socketserver
import logging
import datetime

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

    scheduler.add_job(notify_earnings, 'cron', second='*/10')
    scheduler.start()

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
