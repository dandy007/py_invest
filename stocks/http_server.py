import http.server
import socketserver
import logging
from logging.handlers import RotatingFileHandler
import datetime
import yfinance as yf
import time
from datetime import date, timedelta
import mysql.connector
import requests
from lxml import html
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from db import DAO_Tickers, ROW_Tickers, DB, ROW_TickersData, DAO_TickersData, TICKERS_TIME_DATA__TYPE__CONST, FUNDAMENTAL_NAME__TO_TYPE__ANNUAL, FUNDAMENTAL_NAME__TO_TYPE__QUATERLY

from data_providers.fmp import FMP, FMP_Metrics, FMPException_LimitReached
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

def download_prices():
    logger.info("Download Prices Data Job started.")
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    tickers = dao_tickers.select_tickers_all()

    for ticker in tickers:
        ticker: ROW_Tickers

        last_price_result = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, 1)

        if len(last_price_result) == 0:
            data = yf.download(ticker.ticker_id)
        else:
            data = yf.download(ticker.ticker_id,start=last_price_result[0].date+timedelta(days=1))

        price = data['Adj Close']
        volume = data['Volume']

        rows_price = []
        rows_volume = []

        for date, adj_close in price.items():
            row = ROW_TickersData()

            if len(last_price_result) > 0 and date.date() == last_price_result[0].date:
                continue
            row.date = date.date()
            row.ticker_id = ticker.ticker_id
            row.type = TICKERS_TIME_DATA__TYPE__CONST.PRICE
            row.value = adj_close
            rows_price.append(row)
            
        for date, volume in volume.items():
            if len(last_price_result) > 0 and date.date() == last_price_result[0].date:
                continue
            row = ROW_TickersData()
            row.date = date.date()
            row.ticker_id = ticker.ticker_id
            row.type = TICKERS_TIME_DATA__TYPE__CONST.VOLUME
            row.value = volume
            rows_volume.append(row)

        dao_tickers_data.bulk_insert_ticker_data(rows_price, True)
        dao_tickers_data.bulk_insert_ticker_data(rows_volume, True)
        logger.info(f"Download Prices: Updated {ticker.ticker_id}")


def calc_valuation_stocks():
    logger.info("Calc Valuation Data Job started.")
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    tickers = dao_tickers.select_tickers_all()

    for ticker in tickers:
        info = dao_tickers.select_ticker(ticker.ticker_id)
        
        list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__ANNUAL, 5)

        if len(list_data) == 0 or info.pe == None:
            logger.warning(f"Calc Valuation: Skipping {ticker.ticker_id}")
            continue

        #list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_EV_FCF__ANNUAL, 5)
        #list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_EV_EBITDA__ANNUAL, 5)
        #list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_EV_OPER__ANNUAL, 5)
        #list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_EV_SALES__ANNUAL, 5)
        #list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__ANNUAL, 5)
        #list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PBT__ANNUAL, 5)
        #list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__ANNUAL, 5)
        #list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__ANNUAL, 5)
        #list_data = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_POCF__ANNUAL, 5)

        data_list = []
        for data in list_data:
            if data.value not in (0, None, ''):
                data_list.append(data.value)

        if len(data_list) >= 3:
            avg = sum(data_list) / len(data_list)
            discount = (avg - info.pe) / avg


        dict_data = {
                    TICKERS_TIME_DATA__TYPE__CONST.PE_VALUATION: discount
                    #TICKERS_TIME_DATA__TYPE__CONST.COMBINED_VALUATION: 2
        }
        dao_tickers.update_ticker_types(ticker, dict_data, True)
        logger.info(f"Calc Valuation: Valuation({ticker.ticker_id})")

def download_valuation_stocks():

    logger.info("Download Valuation Data Job started.")
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)
    fmp = FMP()

    tickers = dao_tickers.select_tickers_where("market_cap > 10000000000") # add condition to skip already filled - only temporary solution

    metrics : list[FMP_Metrics] = None
    metric : FMP_Metrics = None
    for ticker in tickers:
        try:
            data_exists = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__ANNUAL, 1)
            if len(data_exists) > 0:
                continue
            metrics = fmp.get_metrics(ticker.ticker_id)
        except FMPException_LimitReached as e:
            logger.warning("Limit reached, job stopped.")
            return

        for metric in metrics:
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_DEBT_EBITDA__ANNUAL, metric.debt_ebitda, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_EV_FCF__ANNUAL, metric.ev_fcf, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_EV_EBITDA__ANNUAL, metric.ev_ebitda, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_EV_OPER__ANNUAL, metric.ev_oper, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_EV_SALES__ANNUAL, metric.ev_sales, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__ANNUAL, metric.pb, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PBT__ANNUAL, metric.pbt, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__ANNUAL, metric.pe, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__ANNUAL, metric.ps, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__ANNUAL, metric.pfcf, metric.date)
            dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_POCF__ANNUAL, metric.pocf, metric.date)
        
        print(f"Download Valuation Ticker({ticker.ticker_id})")


def predict_growth_rate(x : list[float], y : list[float]) -> list[float]:

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

def estimate_growth_stocks():
    logger.info("Estimate Growth Stock Data Job started.")
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    tickers = dao_tickers.select_tickers_all()

    for ticker in tickers:
        #ticker.ticker_id = 'ABNB'
        years_back = 5
        y_eps_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS, years_back)
        y_revenue_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE, years_back)
        y_flow_cont_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION, years_back)
        y_ebitda_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA, years_back)
        y_fcf_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF, years_back)
        y_gross_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT, years_back)

        quarters_back = 8
        q_eps_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS_Q, quarters_back)
        q_revenue_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, quarters_back)
        q_flow_cont_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION_Q, quarters_back)
        q_ebitda_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA_Q, quarters_back)
        q_fcf_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, quarters_back)
        q_gross_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_Q, quarters_back)
        
        growth_list = []
        r_square_list = []
        
        prepared_list = prepare_growth_data(y_eps_list)
        if prepared_list != None: 
            growth_eps = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(growth_eps[0])
            r_square_list.append(growth_eps[1] ** 2)
            print(f"EPS({ticker.ticker_id}): {growth_eps}")

        prepared_list = prepare_growth_data(y_revenue_list)
        if prepared_list != None: 
            growth_revenue = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(growth_revenue[0])
            r_square_list.append(growth_revenue[1] ** 2)
            print(f"Revenue({ticker.ticker_id}): {growth_revenue}")

        prepared_list = prepare_growth_data(y_flow_cont_list)
        if prepared_list != None: 
            cont_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(cont_growth[0])
            r_square_list.append(cont_growth[1] ** 2)
            print(f"Cont_FLOW({ticker.ticker_id}): {cont_growth}")

        prepared_list = prepare_growth_data(y_ebitda_list)
        if prepared_list != None: 
            ebitda_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(ebitda_growth[0])
            r_square_list.append(ebitda_growth[1] ** 2)
            print(f"EBITDA({ticker.ticker_id}): {ebitda_growth}")

        prepared_list = prepare_growth_data(y_fcf_list)
        if prepared_list != None: 
            fcf_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(fcf_growth[0])
            r_square_list.append(fcf_growth[1] ** 2)
            print(f"FCF({ticker.ticker_id}): {fcf_growth}")

        prepared_list = prepare_growth_data(y_gross_list)
        if prepared_list != None: 
            gross_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(gross_growth[0])
            r_square_list.append(gross_growth[1] ** 2)
            print(f"Gross({ticker.ticker_id}): {gross_growth}")

        data = {
            'Growth Rate': growth_list,  # Rustove koeficienty
            'R-squared': r_square_list  # Hodnoty R^2
        }

        if len(growth_list) != 6:
            continue

        df = pd.DataFrame(data)
        df['Weighted Growth'] = df['Growth Rate'] * df['R-squared']
        weighted_average_growth_a = df['Weighted Growth'].sum() / df['R-squared'].sum()
        #print(f"Final growth Annual({ticker.ticker_id}): {weighted_average_growth_a}")
        stability_a = df['R-squared'].sum()
        print(f"Stability Annual({ticker.ticker_id}): {stability_a}")

        growth_list = []
        r_square_list = []

        prepared_list = prepare_growth_data(q_eps_list)
        if prepared_list != None: 
            growth_eps = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(growth_eps[0])
            r_square_list.append(growth_eps[1] ** 2)
            print(f"EPS({ticker.ticker_id}): {growth_eps}")

        prepared_list = prepare_growth_data(q_revenue_list)
        if prepared_list != None: 
            growth_revenue = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(growth_revenue[0])
            r_square_list.append(growth_revenue[1] ** 2)
            print(f"Revenue({ticker.ticker_id}): {growth_revenue}")

        prepared_list = prepare_growth_data(q_flow_cont_list)
        if prepared_list != None: 
            cont_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(cont_growth[0])
            r_square_list.append(cont_growth[1] ** 2)
            print(f"Cont_FLOW({ticker.ticker_id}): {cont_growth}")

        prepared_list = prepare_growth_data(q_ebitda_list)
        if prepared_list != None: 
            ebitda_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(ebitda_growth[0])
            r_square_list.append(ebitda_growth[1] ** 2)
            print(f"EBITDA({ticker.ticker_id}): {ebitda_growth}")

        prepared_list = prepare_growth_data(q_fcf_list)
        if prepared_list != None: 
            fcf_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(fcf_growth[0])
            r_square_list.append(fcf_growth[1] ** 2)
            print(f"FCF({ticker.ticker_id}): {fcf_growth}")

        prepared_list = prepare_growth_data(q_gross_list)
        if prepared_list != None: 
            gross_growth = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(gross_growth[0])
            r_square_list.append(gross_growth[1] ** 2)
            print(f"Gross({ticker.ticker_id}): {gross_growth}")

        if len(growth_list) != 6:
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
        growth_rate_combined = (weighted_average_growth_Q *4* 0.3) + (weighted_average_growth_a * 0.7)
        print(f"Final growth({ticker.ticker_id}) : {growth_rate_combined}")

        dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE__ANNUAL, weighted_average_growth_a, y_revenue_list[0].date)
        dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE__QUATERLY, weighted_average_growth_Q, q_revenue_list[0].date)
        dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE_STABILITY__ANNUAL, stability_a, y_revenue_list[0].date)
        dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE_COMBINED__ANNUAL, growth_rate_combined, y_revenue_list[0].date)
        dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.WEIGHTED_GROWTH_RATE_STABILITY__QUATERLY, stability_q, q_revenue_list[0].date)

        dict_data = {
                TICKERS_TIME_DATA__TYPE__CONST.GROWTH_RATE: weighted_average_growth_a,
                TICKERS_TIME_DATA__TYPE__CONST.GROWTH_RATE_COMBINED: growth_rate_combined,
                TICKERS_TIME_DATA__TYPE__CONST.GROWTH_RATE_STABILITY: stability_a
        }

        dao_tickers.update_ticker_types(ticker, dict_data, True)
        print(f"Updated GROWTH on {ticker.ticker_id}")


        
        

def notify_earnings():
    print(f"Task Notify_Earnings executed at: {datetime.datetime.now()}")
    tickers = ['GOOGL','META','AAPL','AMZN','MSFT','BRK-B','V','PLTR','NVDA','PYPL','DIS','MPW','UFPI','O','VICI','BTI','NXST','TSM','VRTX','CMCSA']

    calendar = get_earnings_calendar()

    export_earnings(tickers)
    #for earning in calendar:
    #    if earning[0] in tickers:
    #        print(f'Ticker: {earning[0]} Date {earning[2]}')

def date_days_diff(past_date_str: str, date_str : str) -> int:
    if past_date != None and date != None:
        
        past_date = datetime.strptime(past_date_str, '%Y-%m-%d')
        date = datetime.strptime(date_str, '%Y-%m-%d')
        delta = date - past_date
        return delta.days

    else:
        raise Exception("Wrong inputs.")

def downloadStockData():

    logger.info("Download Stock Data Job started.")
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

        #time.sleep(2)
        #ticker.ticker_id = 'AAPL'
        pd.set_option('display.max_rows', None)
        stock = yf.Ticker(ticker.ticker_id)
        try:
            name = stock.info.get('name', None)
            sector = stock.info.get('sector', None)
            industry = stock.info.get('industry', None)
            isin = stock.isin

            earnings_date = None
            earnings_dates = stock.calendar.get('Earnings Date', None)
            if earnings_dates != None:
                if len(earnings_dates) > 0:
                    earnings_date = earnings_dates[0]

            shares = stock.info.get('sharesOutstanding', None)
            enterpriseValue = stock.info.get('enterpriseValue', None)
            totalRevenue = stock.info.get('totalRevenue', None)
            price = stock.info.get('currentPrice', None)
            market_cap = stock.info.get('marketCap', None)

            if shares == None:
                logger.warning(f"Download Stock: Skipping {ticker.ticker_id}")
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
            if earnings_date != None:
                ticker.earnings_date = earnings_date

            dict_data = {
                TICKERS_TIME_DATA__TYPE__CONST.MARKET_CAP: market_cap,
                TICKERS_TIME_DATA__TYPE__CONST.ENTERPRISE_VALUE: enterpriseValue,
                TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING: shares,
                #TICKERS_TIME_DATA__TYPE__CONST.PRICE: price,
                TICKERS_TIME_DATA__TYPE__CONST.TARGET_PRICE: stock.info.get('targetMeanPrice', None),
                TICKERS_TIME_DATA__TYPE__CONST.EPS: stock.info.get('trailingEps', None),
                TICKERS_TIME_DATA__TYPE__CONST.BOOK_PER_SHARE: stock.info.get('bookValue', None),
                TICKERS_TIME_DATA__TYPE__CONST.PE: stock.info.get('trailingPE', None),
                TICKERS_TIME_DATA__TYPE__CONST.PB: stock.info.get('priceToBook', None),
                TICKERS_TIME_DATA__TYPE__CONST.BETA: stock.info.get('beta', None),
                #TICKERS_TIME_DATA__TYPE__CONST.VOLUME: stock.info.get('volume', None),
                #TICKERS_TIME_DATA__TYPE__CONST.VOLUME_AVG: stock.info.get('averageVolume', None),
                TICKERS_TIME_DATA__TYPE__CONST.EV_EBITDA: stock.info.get('enterpriseToEbitda', None),
                TICKERS_TIME_DATA__TYPE__CONST.RECOMM_MEAN: stock.info.get('recommendationMean', None),
                TICKERS_TIME_DATA__TYPE__CONST.RECOMM_COUNT: stock.info.get('numberOfAnalystOpinions', None),
                TICKERS_TIME_DATA__TYPE__CONST.DIV_YIELD: stock.info.get('dividendYield', None),
                TICKERS_TIME_DATA__TYPE__CONST.PAYOUT_RATIO: stock.info.get('payoutRatio', None),
                #TICKERS_TIME_DATA__TYPE__CONST.GROWTH_5Y: None,
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
            statements = [stock.income_stmt, stock.balance_sheet, stock.cash_flow]
            
            for valuation_name, type in FUNDAMENTAL_NAME__TO_TYPE__ANNUAL.items():
                for statement in statements:
                    found = False
                    for date in statement.columns:
                        value = statement[date].get(valuation_name, None)
                        if value != None:
                            found = True
                            inserted = dao_tickers_data.store_ticker_data(ticker.ticker_id, type, value, date.date())
                            if (inserted > 0):
                                logger.info(f"!!! New fundamental data ({ticker.ticker_id})")
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
                            inserted = dao_tickers_data.store_ticker_data(ticker.ticker_id, type, value, date.date())
                            if (inserted > 0):
                                logger.info(f"!!! New fundamental data ({ticker.ticker_id})")
                    if found == True:
                        break

            logger.info(f"Download Stock: Updated {ticker.ticker_id}")

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
    logger.info("Download Stock Data Job finished.")
           


    

            


    


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
    scheduler.add_job(download_valuation_stocks, 'cron', hour=0, minute=30) # every day
    scheduler.add_job(download_prices, 'cron',day_of_week='tue-sat', hour=0, minute=30) # day_of_week='mon-fri'
    scheduler.add_job(downloadStockData, 'cron',day_of_week='tue-sat', hour=1, minute=0) # day_of_week='mon-fri'
    scheduler.add_job(estimate_growth_stocks, 'cron', hour=11, minute=0) # day_of_week='mon-fri'
    scheduler.add_job(calc_valuation_stocks, 'cron', hour=11, minute=0) # every day
    #downloadStockData()
    #estimate_growth_stocks()
    #download_valuation_stocks()
    #download_prices()
    #downloadStockData()
    #calc_valuation_stocks()
    scheduler.start()
    logger.info("Schedulers started.")

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
