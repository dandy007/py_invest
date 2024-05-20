import http.server
import socketserver
import logging
from logging.handlers import RotatingFileHandler
import datetime
import yfinance as yf
import time
from datetime import date, timedelta, datetime
import mysql.connector
import requests
from lxml import html
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from stocks.db import DAO_Tickers, ROW_Tickers, DB, ROW_TickersData, DAO_TickersData, DAO_Portfolios, ROW_Portfolios, ROW_PortfolioPositions, DAO_PortfolioPositions, TICKERS_TIME_DATA__TYPE__CONST, FUNDAMENTAL_NAME__TO_TYPE__ANNUAL, FUNDAMENTAL_NAME__TO_TYPE__QUATERLY
from flask import Flask,render_template, render_template_string, request, redirect, url_for

from stocks.data_providers.fmp import FMP, FMP_Metrics, FMPException_LimitReached
from stocks.data_providers.alpha_vantage import get_tickers_download, get_earnings_calendar

from apscheduler.schedulers.background import BackgroundScheduler
from stocks.exporters.ical_exporter import export_earnings
from bokeh.plotting import figure
from bokeh.io import output_file, show
from bokeh.embed import file_html
from bokeh.resources import CDN

from stocks.frontend import ROW_WebPortfolioPosition
import plotly.graph_objects as go


app = Flask(__name__, template_folder='frontend/html', static_folder='frontend/html/static')
scheduler = BackgroundScheduler()

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

def valuate_stocks():
    logger.info("Valuate Stocks Data Job started.")
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    tickers = dao_tickers.select_tickers_all()

    for ticker in tickers:
        ticker: ROW_Tickers
        #ticker.ticker_id = "JBL"

        eps_ttm = None
        y_eps_growth_rate = None
        q8_eps_growth_rate = None
        y5avg_pe_ratio = None
        cash_share = 0


        y_eps_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS, 5)
        prepared_list = prepare_growth_data(y_eps_list)
        if prepared_list != None: 
            growth_eps = predict_growth_rate(prepared_list[0], prepared_list[1])
            y_eps_growth_rate = growth_eps[0]

        q_eps_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS_Q, 8)
        prepared_list = prepare_growth_data(q_eps_list)
        if prepared_list != None: 
            q_growth_eps = predict_growth_rate(prepared_list[0], prepared_list[1])
            q8_eps_growth_rate = q_growth_eps[0]
            if len(prepared_list[1]) >= 4:
                eps_ttm = (prepared_list[1][-1] + prepared_list[1][-2] + prepared_list[1][-3] + prepared_list[1][-4])
                if eps_ttm <= 0:
                    eps_ttm = None

        if q8_eps_growth_rate == None:
            q8_eps_growth_rate = y_eps_growth_rate

        pe_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__ANNUAL, 5)
        prepared_list = prepare_growth_data(pe_list)
        if prepared_list != None: 
            pe_sum = sum(prepared_list[1])
            pe_count = len(prepared_list[1])
            if pe_count > 3:
                y5avg_pe_ratio = pe_sum / pe_count

        cash_record = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH, 1)
        shares_record = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING, 1)
        if len(cash_record) > 0 and len(shares_record) > 0:
            cash_share = cash_record[0].value / shares_record[0].value

        margin_of_safety = 0.2
        desired_return = 0.1
        future_years_count = 3
        if y_eps_growth_rate != None and q8_eps_growth_rate != None and eps_ttm != None and y5avg_pe_ratio != None:
     
            eps_growth = (0.8 * y_eps_growth_rate) + (0.2 * q8_eps_growth_rate)

            future_eps = eps_ttm * ((1 + eps_growth) ** future_years_count)      
            future_price = future_eps * y5avg_pe_ratio
            required_buy_price = future_price / ((1 + desired_return) ** future_years_count)
            buy_price = required_buy_price * (1 - margin_of_safety) + cash_share

            eps_valuation_record = ROW_TickersData()
            eps_valuation_record.date = q_eps_list[0].date
            eps_valuation_record.ticker_id = ticker.ticker_id
            eps_valuation_record.type = TICKERS_TIME_DATA__TYPE__CONST.EPS_VALUATION
            eps_valuation_record.value = buy_price

            dao_tickers_data.store_ticker_data(eps_valuation_record.ticker_id, eps_valuation_record.type, eps_valuation_record.value, eps_valuation_record.date)
            dict_data = {
                TICKERS_TIME_DATA__TYPE__CONST.EPS_VALUATION: buy_price
            }

            dao_tickers.update_ticker_types(ticker, dict_data, True)

            print(f"{ticker.ticker_id}: {buy_price}")
        else:
            pass
            #print(f"{ticker.ticker_id}: Skipped !!!")
    logger.info("Valuate Stocks Data Job finished.")

        



def get_price_discount(dao_tickers_data : DAO_TickersData, ticker_id:str, length: int):
    #connection = DB.get_connection_mysql()
    try:
        #dao_tickers_data = DAO_TickersData(connection)

        list_prices = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, length)
        list_volumes = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.VOLUME, length)

        if len(list_prices) >= length:

            pd_list = []
            for price, volume in zip(list_prices, list_volumes):
                pd_list.insert(0,{'Date': price.date, 'Close': price.value, 'Volume': volume.value})

            df = pd.DataFrame(pd_list)

            # Vytvoreni noveho sloupce 'VWAP' s vazenymi cenami
            df['VWAP'] = df['Close'] * df['Volume']
            # Vypocet sumy VWAP a sumy Volume pro dane obdobi
            sum_vwap = df['VWAP'].rolling(window=length).sum()
            sum_volume = df['Volume'].rolling(window=length).sum()
            df['Deviation'] = df['Close'] - df['VWAP']
            #print(df['Deviation'].describe())
            # Vypocet samotneho VWMA
            vwma = sum_vwap / sum_volume
            #print(f"VWAP: {vwma.iloc[-1]}")
            #print(f"Discount({ticker_id}): {(vwma.iloc[-1] - list_prices[0].value)/vwma.iloc[-1]}")
            return (vwma.iloc[-1] - list_prices[0].value)/vwma.iloc[-1]
    finally:
        #connection.close()
        pass

def calculate_price_discount():
    logger.info("Calculate price discount Job started.")
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    tickers = dao_tickers.select_tickers_all()

    for ticker in tickers:
        #ticker.ticker_id = 'ADC'
        discount50 = get_price_discount(dao_tickers_data, ticker.ticker_id, 50)
        discount200 = get_price_discount(dao_tickers_data, ticker.ticker_id, 200)
        discount1000 = get_price_discount(dao_tickers_data, ticker.ticker_id, 1000)

        years_count = 3

        pe_mean_stdev = dao_tickers_data.select_ticker_data_mean_stdev(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS, years_count * 365)
        pb_mean_stdev = dao_tickers_data.select_ticker_data_mean_stdev(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS, years_count * 365)
        pfcf_mean_stdev = dao_tickers_data.select_ticker_data_mean_stdev(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS, years_count * 365)

        pb_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PB, 1)
        pfcf_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, 4)

        pe_zscore = None
        pfcf_zscore = None
        pb_zscore = None

        if ticker.pe != None and pe_mean_stdev != None and pe_mean_stdev[0] != None:
            #pe_zscore = (ticker.pe - pe_mean_stdev[0]) / pe_mean_stdev[1]
            pe_zscore = (ticker.pe - pe_mean_stdev[0]) / pe_mean_stdev[0]
        
        if pb_mean_stdev != None and len(pb_list) > 0 and pb_mean_stdev != None and pb_list[0].value != None and pb_mean_stdev[0] != None:
            #pb_zscore = (pb_list[0].value - pb_mean_stdev[0]) / pb_mean_stdev[1]
            pb_zscore = (pb_list[0].value - pb_mean_stdev[0]) / pb_mean_stdev[0]

        fcf_value = 0
        if pfcf_mean_stdev != None and len(pfcf_list) >= 4 and pfcf_mean_stdev[0] != None:
            for i in range(1, 5):
                if isinstance(pfcf_list[-i].value, (int, float)):
                    fcf_value += pfcf_list[-i].value
                else:
                    fcf_value = None
                    break
            if fcf_value != None:                       
                #pfcf_zscore = ((ticker.market_cap/fcf_value) - pfcf_mean_stdev[0]) / pfcf_mean_stdev[1]
                pfcf_zscore = ((ticker.market_cap/fcf_value) - pfcf_mean_stdev[0]) / pfcf_mean_stdev[0]

        dict_data = {
                TICKERS_TIME_DATA__TYPE__CONST.PRICE_DISCOUNT_1: discount50,
                TICKERS_TIME_DATA__TYPE__CONST.PRICE_DISCOUNT_2: discount200,
                TICKERS_TIME_DATA__TYPE__CONST.PRICE_DISCOUNT_3: discount1000
        }
        dao_tickers.update_ticker_types(ticker, dict_data, True)

        if pe_zscore != None:
            dict_data = {
                    TICKERS_TIME_DATA__TYPE__CONST.PE_DISCOUNT: pe_zscore
            }
            dao_tickers.update_ticker_types(ticker, dict_data, True)
        
        if pfcf_zscore != None:
            dict_data = {
                    TICKERS_TIME_DATA__TYPE__CONST.PFCF_DISCOUNT: pfcf_zscore
            }
            dao_tickers.update_ticker_types(ticker, dict_data, True)

        if pb_zscore != None:
            dict_data = {
                    TICKERS_TIME_DATA__TYPE__CONST.PB_DISCOUNT: pb_zscore
            }
            dao_tickers.update_ticker_types(ticker, dict_data, True)

        logger.info(f"Discount({ticker.ticker_id})")
    logger.info("Calculate price discount Job finished.")
        
       




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
    logger.info("Download Prices Data Job finished.")


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
    logger.info("Calc Valuation Data Job finished.")

def download_valuation_stocks():

    logger.info("Download Valuation Data Job started.")
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)
    fmp = FMP()

    tickers = dao_tickers.select_tickers_where("market_cap > 1000000000") # add condition to skip already filled - only temporary solution

    metrics : list[FMP_Metrics] = None
    metric : FMP_Metrics = None
    todayDate = date.today()
    for ticker in tickers:
        try:
            data_exists = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__ANNUAL, 1)
            

            if len(data_exists) > 0 and (todayDate - data_exists[-1].date).days > (366 + 92)  or len(data_exists) == 0:
                metrics = fmp.get_metrics(ticker.ticker_id)
                if len(metrics) == 0:
                    #dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__ANNUAL, 0.0, todayDate)
                    continue
            else:
                continue
        except FMPException_LimitReached as e:
            logger.warning("Limit reached, job stopped.")
            return

        for metric in metrics:
            if metric.date <= data_exists[0].date:
                continue
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
        
        logger.info(f"Download Valuation Ticker({ticker.ticker_id})")
    logger.info("Download Valuation Data Job finished.")


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
        #ticker.ticker_id = 'AAPL'
        years_back = 5
        y_net_income_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME, years_back)
        y_revenue_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE, years_back)
        y_flow_cont_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION, years_back)
        y_ebitda_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA, years_back)
        y_fcf_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF, years_back)
        y_gross_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT, years_back)

        quarters_back = 8
        q_net_income_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_Q, quarters_back)
        q_revenue_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, quarters_back)
        q_flow_cont_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION_Q, quarters_back)
        q_ebitda_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA_Q, quarters_back)
        q_fcf_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, quarters_back)
        q_gross_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_Q, quarters_back)

        q_shares_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS, 3 * 52 * 5) #2 years * 52 weeks * 5 days
        growth_shares = None

        shares_growth_per_year = None
        if len(q_shares_list) > 250 and isinstance(q_shares_list[0].value, (int, float)) and isinstance(q_shares_list[-1].value, (int, float) ) and q_shares_list[-1].value != 0:
            percent_change = (q_shares_list[0].value - q_shares_list[-1].value)/q_shares_list[-1].value
            shares_growth_per_year = percent_change * (52 * 5) / len(q_shares_list) # 52 weeks * 5 days
        
        if shares_growth_per_year != None:
            print(f"Shares({ticker.ticker_id}): {shares_growth_per_year}")
        
        growth_list = []
        r_square_list = []
        
        prepared_list = prepare_growth_data(y_net_income_list)
        if prepared_list != None: 
            growth_net_income = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(growth_net_income[0])
            r_square_list.append(growth_net_income[1] ** 2)
            print(f"Net income({ticker.ticker_id}): {growth_net_income}")

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
        if shares_growth_per_year != None:
            weighted_average_growth_a = ((1 + weighted_average_growth_a) / (1 + shares_growth_per_year) ) - 1
        #print(f"Final growth Annual({ticker.ticker_id}): {weighted_average_growth_a}")
        stability_a = df['R-squared'].sum()
        print(f"Stability Annual({ticker.ticker_id}): {stability_a}")

        growth_list = []
        r_square_list = []

        prepared_list = prepare_growth_data(q_net_income_list)
        if prepared_list != None: 
            growth_net_income = predict_growth_rate(prepared_list[0], prepared_list[1])
            growth_list.append(growth_net_income[0])
            r_square_list.append(growth_net_income[1] ** 2)
            print(f"Net income({ticker.ticker_id}): {growth_net_income}")

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
    logger.info("Estimate Growth Stock Data Job finished.")


        
        

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
    
    #skip = True
    ticker_list = dao_tickers.select_tickers_all()
    for ticker in ticker_list:
        ticker:ROW_Tickers

        #if ticker.ticker_id == 'A':
        #    skip = False

        #if skip:
        #    continue

        #time.sleep(2)
        #ticker.ticker_id = 'AAPL'
        pd.set_option('display.max_rows', None)
        stock = yf.Ticker(ticker.ticker_id)
        try:
            name = stock.info.get('name', None)
            sector = stock.info.get('sector', None)
            industry = stock.info.get('industry', None)
            description = stock.info.get('longBusinessSummary', None)
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
            if description != None:
                ticker.description = description #.replace("'", "")

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

            options = stock.options
            today = datetime.today().date()
            one_year_from_now = (today + timedelta(days=365)).strftime("%Y-%m-%d")
            one_month_from_now = (today + timedelta(days=30)).strftime("%Y-%m-%d")

            month_done = False
            for option in options:
                if month_done == False and (option > one_month_from_now):
                    #print("one month")
                    chain = stock.option_chain(option)
                    future_price = get_option_growth_data(chain, option)
                    if future_price != None:
                        dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_MONTH_AVG_PRICE, future_price, today)
                        #print(future_price)
                    month_done = True
                    continue
                    
                if (option > one_year_from_now):
                    #print("one year")
                    chain = stock.option_chain(option)
                    future_price = get_option_growth_data(chain, option)
                    if future_price != None:
                        if price not in (0, None):
                            year_discount = (future_price - price) / price
                            dict_data = {
                                TICKERS_TIME_DATA__TYPE__CONST.OPTION_YEAR_DISCOUNT: year_discount
                            }
                            dao_tickers.update_ticker_types(ticker, dict_data, True)
                        dao_tickers_data.store_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_YEAR_AVG_PRICE, future_price, today)
                        #print(future_price)
                    break

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

        except Exception as err:
            logger.exception(f"Error updating ticker[{ticker.ticker_id}]:")
            continue
    logger.info("Download Stock Data Job finished.")
           
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

    
def calculate_continuous_metrics(earning_metric_const: int, metric_continuous_const: int):
    logger.info("Continuous metrics Job started.")
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    ticker_list_orig = dao_tickers.select_tickers_all()

    for ticker in ticker_list_orig:
        ticker : ROW_Tickers

        print(f"Continous Metric Ticker: {ticker.ticker_id}")

        years_back = 3
        price_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, years_back * 365)
        price_list.sort(key=lambda x: x.date, reverse=False)
        metric_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, earning_metric_const, years_back)

        if earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS:
            net_earnings_q_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME, years_back)
            net_earnings_q_list.sort(key=lambda x: x.date, reverse=False)
            eps_q_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS, years_back)
            eps_q_list.sort(key=lambda x: x.date, reverse=False)
            eps_counter = 0
            for eps in eps_q_list:
                for net_earnings in net_earnings_q_list:
                    if eps.date != None and eps.date == net_earnings.date and isinstance(eps.value, (int, float)) and isinstance(net_earnings.value, (int, float)) and eps.value != 0:
                        metric = ROW_TickersData()
                        metric.date = eps.date
                        metric.value = net_earnings.value / eps.value
                        metric_list.append(metric)
                        break
                        
                    
        if (earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__ANNUAL):
            rev_q_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, years_back * (4 + 1))
            shares_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS, years_back * 300)
            rev_q_list.sort(key=lambda x: x.date, reverse=False)
            rev_counter = 0
            num_shares = None
            for rev in rev_q_list:
                rev_counter += 1
                for price in price_list:
                    if rev_counter >= 4 and (isinstance(rev.value, (int, float)) and price.date >= rev.date):
                        rev_value = 0
                        for i in range(1, 5):
                            if isinstance(rev_q_list[rev_counter-i].value, (int, float)):
                                rev_value += rev_q_list[rev_counter-i].value
                            else:
                                rev_value = None
                                break
                        if (rev_value not in (0,  None)):
                            metric = ROW_TickersData()
                            metric.date = rev.date

                            for shares in shares_list:
                                if shares.date >= price.date:
                                    num_shares = shares.value

                            if num_shares not in (None, 0):
                                metric.value = price.value / (rev_value / num_shares)
                                metric_list.append(metric)
                            break

        if (earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__ANNUAL):
            eps_q_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS_Q, years_back * (4 + 1))
            eps_q_list.sort(key=lambda x: x.date, reverse=False)
            eps_counter = 0
            for eps in eps_q_list:
                eps_counter += 1
                for price in price_list:
                    if eps_counter >= 4 and (isinstance(eps.value, (int, float)) and price.date >= eps.date):
                        eps_value = 0
                        for i in range(1, 5):
                            if isinstance(eps_q_list[eps_counter-i].value, (int, float)):
                                eps_value += eps_q_list[eps_counter-i].value
                            else:
                                eps_value = None
                                break
                        if (eps_value not in (0,  None)):
                            metric = ROW_TickersData()
                            metric.date = eps.date
                            metric.value = price.value / eps_value
                            metric_list.append(metric)
                            break

        if (earning_metric_const == TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__ANNUAL):
            fcf_q_list = dao_tickers_data.select_ticker_data(ticker.ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, years_back * (4 + 1))
            fcf_q_list.sort(key=lambda x: x.date, reverse=False)
            fcf_counter = 0
            for fcf in fcf_q_list:
                fcf_counter += 1
                for price in price_list:
                    if fcf_counter >= 4 and (isinstance(fcf.value, (int, float)) and price.date >= fcf.date):
                        fcf_value = 0
                        for i in range(1, 5):
                            if isinstance(fcf_q_list[fcf_counter-i].value, (int, float)):
                                fcf_value += fcf_q_list[fcf_counter-i].value
                            else:
                                fcf_value = None
                                break
                        if (fcf_value not in (0,  None)):
                            metric = ROW_TickersData()
                            metric.date = fcf.date
                            metric.value = price.value / fcf_value
                            #metric_list.append(metric)
                            break
            

        metric_list.sort(key=lambda x: x.date, reverse=False)
        last_metric_continous_record = dao_tickers_data.select_ticker_data(ticker.ticker_id, metric_continuous_const, 1)

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
            metric_record.ticker_id = ticker.ticker_id
            metric_record.type = metric_continuous_const
            metric_record.value = metric
            new_data.append(metric_record)
            #dao_tickers_data.store_ticker_data(metric_record.ticker_id, metric_record.type, metric_record.value, metric_record.date)
            #print(f"Metric({record.date}): {metric}")
        if (len(new_data) > 0):
            dao_tickers_data.bulk_insert_ticker_data(new_data, True)

    logger.info("Continuous metrics Job finished.")

def get_option_growth_data(chain, date: str) -> float: # [month_price, year_price]
    sum = 0
    count = 0
    for index, row in chain.calls.iterrows():
        #print(f"Strike: {row['strike']}, Bid: {row['bid']}, Ask: {row['ask']}, Open Interest: {row['openInterest']}") #row['impliedVolatility']
        sum += row['openInterest'] * row['strike']
        count += row['openInterest']

    if (count != 0):
        return sum/count
    
    return None
        #print(f"Avg price({ticker_id} - {date}): {sum/count}    {((sum/count) - yf_ticker.info['currentPrice']) / yf_ticker.info['currentPrice']}")

def polygon_load_fundaments():
    poly = POLYGON()
    #client = poly.get_polygon()

    result = poly.get_fundamentals_raw('O', POLY_CONSTANTS.TIMEFRAME_QUATERLY)
    result_1 = result.results[0]
    fin = result.results[0].financials
    print(result)

    

@app.route('/chart')
def bokeh_plot():
    plot = figure(title="Apple", x_axis_label='Date', y_axis_label='Price', width=1600, height=800)

    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)
    price_data_list = dao_tickers_data.select_ticker_data('AAPL', TICKERS_TIME_DATA__TYPE__CONST.PRICE, 500)
    price_data_list.sort(key=lambda x : x.date, reverse=False)

    date_list = []
    price_list = []

    count = 0
    for price in price_data_list:
        price : ROW_TickersData
        count +=1
        date_list.append(count)
        price_list.append(price.value)

    plot.line(date_list, price_list, line_width=1)
    
    # Embed plot into HTML via div element
    html = file_html(plot, CDN, "my plot")
    return render_template_string('<html><body>{{ plot_div | safe }}</body></html>', plot_div=html)


@app.route('/')
def main():
    jobs = scheduler.get_jobs()
    #for job in jobs:

    return render_template('index.html', jobs=jobs)

@app.route('/portfolios')
def portfolios():

    connection = DB.get_connection_mysql()
    dao_portfolios = DAO_Portfolios(connection)
    portfolios = dao_portfolios.select_all_portfolios()
    return render_template('portfolios.html', portfolios = portfolios)


@app.template_filter('percentage')
def format_percentage(value):
    if value == None:
        return '0%'
    return "{:.2%}".format(value)

@app.route('/portfolio/<int:portfolio_id>')
def portfolio(portfolio_id):

    connection = DB.get_connection_mysql()
    dao_portfolios = DAO_Portfolios(connection)
    dao_portfolio_positions = DAO_PortfolioPositions(connection)
    dao_tickers = DAO_Tickers(connection)
    portfolio = dao_portfolios.select_portfolio(portfolio_id)
    positions = dao_portfolio_positions.select_all_portfolio_positions(portfolio_id)
    web_positions = []

    for position in positions:
        position: ROW_PortfolioPositions
        ticker = dao_tickers.select_ticker(position.ticker_id)
        web_position: ROW_WebPortfolioPosition = ROW_WebPortfolioPosition(ticker, position)
        web_positions.append(web_position)

    return render_template('portfolio.html', web_positions = web_positions, portfolio = portfolio)

@app.route('/portfolios/submit_new', methods=['POST'])
def portfolios_submit_new():
    name = request.form['name']

    if (name not in (None, '')):
        connection = DB.get_connection_mysql()
        dao_portfolios = DAO_Portfolios(connection)
        p = ROW_Portfolios(name)
        dao_portfolios.insert_portfolio(p)

    return redirect(url_for('portfolios'))

@app.route('/portfolios/submit_delete', methods=['POST'])
def portfolios_submit_delete():
    id = request.form['id']

    if (id not in (None, '')):
        connection = DB.get_connection_mysql()
        dao_portfolios = DAO_Portfolios(connection)
        dao_portfolios.delete_portfolio(id)

    return redirect(url_for('portfolios'))


@app.route('/portfolio_positions/submit_new', methods=['POST'])
def portfolio_positions_submit_new():
    ticker_id = request.form['ticker_id']
    portfolio_id = request.form['portfolio_id']

    if (ticker_id not in (None, '') and portfolio_id not in (None, '')):
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_portfolio_positions = DAO_PortfolioPositions(connection)

        ticker = dao_tickers.select_ticker(ticker_id)
        if ticker != None:
            p = ROW_PortfolioPositions()
            p.portfolio_id = portfolio_id
            p.ticker_id = ticker_id
            dao_portfolio_positions.insert_portfolio_position(p)

    return redirect(url_for('portfolio',portfolio_id = portfolio_id))

@app.route('/portfolio_positions/submit_delete', methods=['POST'])
def portfolio_positions_submit_delete():
    ticker_id = request.form['ticker_id']
    portfolio_id = request.form['portfolio_id']

    if (ticker_id not in (None, '') and portfolio_id not in (None, '')):
        connection = DB.get_connection_mysql()
        dao_portfolio_positions = DAO_PortfolioPositions(connection)
        dao_portfolio_positions.delete_portfolio_position(portfolio_id=portfolio_id, ticker_id=ticker_id)

    return redirect(url_for('portfolio', portfolio_id = portfolio_id))

def prepare_chart_data(ticker_data_list: list[ROW_TickersData]):
    list_x = []
    list_y = []

    ticker_data_list.sort(key=lambda x: (x.date), reverse=True)

    for ticker_data in ticker_data_list:
        list_x.append(ticker_data.date)
        list_y.append(ticker_data.value)

    return [list_x, list_y]

def getChart(x_data, y_data_lists, line_title_list, chart_title):
    fig = go.Figure()

    counter = 0
    for y_data in y_data_lists:
        fig.add_trace(go.Scatter(x=x_data, y=y_data, mode='lines', name=line_title_list[counter]))
        counter += 1

    fig.update_layout(title=chart_title)
    fig_html = fig.to_html(full_html=False)
    return fig_html

@app.route('/ticker', methods=['POST'])
def ticker():
    ticker_ids = request.form['ticker_id']
    return ticker_id(ticker_ids)

@app.route('/ticker/<ticker_id>', methods=['GET'])
def ticker_id(ticker_id: str):

    connection = DB.get_connection_mysql()
    dao_portfolios = DAO_Portfolios(connection)
    dao_portfolio_positions = DAO_PortfolioPositions(connection)
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    days_back = 5 * 250
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, 3*250)
    prepared_chart_data__price = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TARGET_PRICE, days_back)
    prepared_chart_data__target_price = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_MONTH_AVG_PRICE, days_back)
    prepared_chart_data__option_month_price = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_YEAR_AVG_PRICE, days_back)
    prepared_chart_data__option_year_price = prepare_chart_data(data_list)


    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS, days_back)
    prepared_chart_data__pe = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS, days_back)
    prepared_chart_data__pb = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS, days_back)
    prepared_chart_data__ps = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS, days_back)
    prepared_chart_data__pfcf = prepare_chart_data(data_list)


    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS, days_back)
    prepared_chart_data__shares = prepare_chart_data(data_list)

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE, days_back)
    prepared_chart_data__revenue = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT, days_back)
    prepared_chart_data__gross = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA, days_back)
    prepared_chart_data__ebitda = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME, days_back)
    prepared_chart_data__net_income = prepare_chart_data(data_list)



    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_ASSETS, days_back)
    prepared_chart_data__assets = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_LIABILITIES, days_back)
    prepared_chart_data__liabilities = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.STOCKHOLDER_EQUITY, days_back)
    prepared_chart_data__equity = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.LONG_TERM_DEBT, days_back)
    prepared_chart_data__long_term_debt = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH, days_back)
    prepared_chart_data__cash = prepare_chart_data(data_list)



    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF, days_back)
    prepared_chart_data__fcf = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION, days_back)
    prepared_chart_data__fcf_oper = prepare_chart_data(data_list)



    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.STOCKHOLDER_EQUITY, days_back)
    prepared_chart_data__balance_sheet = prepare_chart_data(data_list)

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF, days_back)
    prepared_chart_data__free_cash_flow_statement = prepare_chart_data(data_list)

    

    ticker = dao_tickers.select_ticker(ticker_id)

    return render_template(
        'ticker.html', 
        ticker = ticker,
        plot_price = getChart(prepared_chart_data__price[0], [prepared_chart_data__price[1]], [''], 'Price'),
        plot_target_price = getChart(prepared_chart_data__target_price[0], [prepared_chart_data__target_price[1]], [''], 'Target Price'),
        
        plot_option_price_1M = getChart(prepared_chart_data__option_month_price[0], [prepared_chart_data__option_month_price[1]], [''], 'Option price 1M'),
        plot_option_price_1Y = getChart(prepared_chart_data__option_year_price[0], [prepared_chart_data__option_year_price[1]], [''], 'Option price 1Y'),

        plot_pe = getChart(prepared_chart_data__pe[0], [prepared_chart_data__pe[1]], [''], 'PE'),
        plot_pb = getChart(prepared_chart_data__pb[0], [prepared_chart_data__pb[1]], [''], 'PB'),
        plot_ps = getChart(prepared_chart_data__ps[0], [prepared_chart_data__ps[1]], [''], 'PS'),
        plot_pfcf = getChart(prepared_chart_data__pfcf[0], [prepared_chart_data__pfcf[1]], [''], 'PFCF'),

        plot_income_statement = getChart(
            prepared_chart_data__revenue[0], 
            [
                prepared_chart_data__revenue[1],
                prepared_chart_data__gross[1],
                prepared_chart_data__ebitda[1],
                prepared_chart_data__net_income[1]
            ], 
            [
                'Revenue',
                'Gross Profit',
                'EBITDA',
                'Net Income'
            ], 
            'Income statement'
        ),

        plot_balance_sheet = getChart(
            prepared_chart_data__assets[0], 
            [
                prepared_chart_data__assets[1],
                prepared_chart_data__liabilities[1],
                prepared_chart_data__equity[1],
                prepared_chart_data__long_term_debt[1],
                prepared_chart_data__cash[1]
            ], 
            [
                'Assets',
                'Liabilities',
                'Equity',
                'Long debt',
                'Cash'
            ], 
            'Balance sheet'
        ),

        plot_free_cash_flow_statement = getChart(
            prepared_chart_data__fcf_oper[0], 
            [
                prepared_chart_data__fcf_oper[1],
                prepared_chart_data__fcf[1]
            ], 
            [
                'FCF Operation',
                'FCF'
            ], 
            'FCF Statement'
        ),     
        
        plot_shares = getChart(prepared_chart_data__shares[0], [prepared_chart_data__shares[1]], [''], 'Shares outstanding')
        
    )

def sync_ticker_id_list():
    fmp = FMP()

    connection = DB.get_connection_mysql()  
    dao_tickers = DAO_Tickers(connection)
    #dao_tickers_data = DAO_TickersData(connection)

    ticker_list = fmp.get_statement_symbols_list()
    db_ticker_list = dao_tickers.select_tickers_all_ids()

    for ticker_id in ticker_list:
        if ticker_id.upper() not in db_ticker_list:
            dao_tickers.insert_ticker(ticker_id.upper(), True)
            db_ticker_list.append(ticker_id.upper())
    
    return




if __name__ == "__main__":
    
    #tickerList = get_tickers_download()
    #for row in tickerList:
    #    print(f"Ticker: {row}")
    #self.scheduler = BackgroundScheduler()
    #scheduler.add_job(my_task, 'interval', seconds=10)

    #cron
    #minute='*/5': Execute the task every 5 minutes
    #hour='0-23/2': Execute the task every 2 hours (0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22)
    #day='*/2': Execute the task every other day
    #month='*': Execute the task every month
    #day_of_week='mon-fri': Execute the task only on weekdays

    #scheduler.add_job(notify_earnings, 'cron', second='*/10')
    scheduler.add_job(download_valuation_stocks, 'cron', hour=0, minute=30,) # every day
    scheduler.add_job(download_prices, 'cron',day_of_week='tue-sat', hour=0, minute=30) # day_of_week='mon-fri'
    scheduler.add_job(downloadStockData, 'cron',day_of_week='tue-sat', hour=1, minute=0) # day_of_week='mon-fri'
    scheduler.add_job(estimate_growth_stocks, 'cron',day_of_week='tue-sat', hour=11, minute=0) # day_of_week='mon-fri'
    scheduler.add_job(calc_valuation_stocks, 'cron',day_of_week='tue-sat', hour=11, minute=0) # every day
    scheduler.add_job(valuate_stocks, 'cron',day_of_week='tue-sat', hour=11, minute=0) # every day
    scheduler.add_job(calculate_price_discount, 'cron',day_of_week='tue-sat', hour=11, minute=0) # every day
    scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=11, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__ANNUAL, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS]) # every day
    scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=11, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__ANNUAL, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS]) # every day
    scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=11, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__ANNUAL, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS]) # every day
    scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=11, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__ANNUAL, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS]) # every day
    scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=11, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS, TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS]) # every day
    
    sync_ticker_id_list()
    #downloadStockData()
    #download_valuation_stocks()
    #download_prices()
    #calc_valuation_stocks()
    #calculate_price_discount()
    #rank_stocks()
    #valuate_stocks()
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__ANNUAL, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS)
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__ANNUAL, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS)
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__ANNUAL, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS)
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS, TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS)
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__ANNUAL, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS)
    #estimate_growth_stocks()
    #polygon_load_fundaments()

    #scheduler.start()
    


    logger.info("Schedulers started.")
    #app.run(debug=True,host='0.0.0.0')
    

#with socketserver.TCPServer(("", PORT), Handler) as httpd:
#    print(f"Serving at port {PORT}")
#    httpd.serve_forever()
