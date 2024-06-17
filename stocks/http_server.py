import http.server
import socketserver
import logging
from logging.handlers import RotatingFileHandler
import datetime
import math
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
from scipy import stats
from concurrent.futures import ThreadPoolExecutor
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant

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

def calculate_price_discount():
    logger.info(f"calculate_price_discount - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        tickers = dao_tickers.select_tickers_all__limited_ids()

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

            #if ticker.pe != None and pe_mean_stdev != None and pe_mean_stdev[0] != None:
                #pe_zscore = (ticker.pe - pe_mean_stdev[0]) / pe_mean_stdev[1]
            #    pe_zscore = (ticker.pe - pe_mean_stdev[0]) / pe_mean_stdev[0]
            
            #if pb_mean_stdev != None and len(pb_list) > 0 and pb_mean_stdev != None and pb_list[0].value != None and pb_mean_stdev[0] != None:
                #pb_zscore = (pb_list[0].value - pb_mean_stdev[0]) / pb_mean_stdev[1]
            #    pb_zscore = (pb_list[0].value - pb_mean_stdev[0]) / pb_mean_stdev[0]

            #fcf_value = 0
            #if pfcf_mean_stdev != None and len(pfcf_list) >= 4 and pfcf_mean_stdev[0] != None:
            #    for i in range(1, 5):
            #        if isinstance(pfcf_list[-i].value, (int, float)):
            #            fcf_value += pfcf_list[-i].value
            #        else:
            #            fcf_value = None
            #            break
            #    if fcf_value != None:                       
                    #pfcf_zscore = ((ticker.market_cap/fcf_value) - pfcf_mean_stdev[0]) / pfcf_mean_stdev[1]
            #        pfcf_zscore = ((ticker.market_cap/fcf_value) - pfcf_mean_stdev[0]) / pfcf_mean_stdev[0]

            dict_data = {
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_DISCOUNT_1: discount100,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_DISCOUNT_2: discount200,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_DISCOUNT_3: discount500,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_PROB_1: prob100,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_PROB_2: prob200,
                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__PRICE_PROB_3: prob500
            }
            dao_tickers.update_ticker_types(ticker_id, dict_data, True)

            #if pe_zscore != None:
            #    dict_data = {
            #            TICKERS_TIME_DATA__TYPE__CONST.PE_DISCOUNT: pe_zscore
            #    }
            #    dao_tickers.update_ticker_types(ticker, dict_data, True)
            
            #if pfcf_zscore != None:
            #    dict_data = {
            #            TICKERS_TIME_DATA__TYPE__CONST.PFCF_DISCOUNT: pfcf_zscore
            #    }
            #    dao_tickers.update_ticker_types(ticker, dict_data, True)

            #if pb_zscore != None:
            #    dict_data = {
            #            TICKERS_TIME_DATA__TYPE__CONST.PB_DISCOUNT: pb_zscore
            #    }
            #    dao_tickers.update_ticker_types(ticker, dict_data, True)

            logger.info(f"Discount({ticker_id}) {counter}/{len(tickers)}")
    except Exception as e:
        logger.error(f"calculate_price_discount - Error {e}")
    logger.info(f"calculate_price_discount - End")

def download_prices():
    logger.info(f"download_prices - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)
        fmp = FMP()

        tickers = dao_tickers.select_tickers_all__limited_ids()
        today = datetime.today().strftime("%Y-%m-%d")
        fromDay_0 = "1900-01-01"

        counter = 0
        skip = True
        for ticker_id in tickers:
            #ticker_id='NVDA'
            #ticker: ROW_Tickers
            #if ticker_id == 'PFG':
            #    skip = False
            #if skip:
            #    continue
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
                prices = fmp.get_historic_prices(ticker_id, fromDay, today)
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
    logger.info(f"download_prices - End")

def calc_valuation_ratios_stocks():

    logger.info(f"calc_valuation_ratios_stocks - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)
        fmp = FMP()

        statement_list = fmp.get_statement_symbols_list()

        tickers = dao_tickers.select_tickers_all__limited_ids()

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
                # market_cap_list_q = 
                """
                for shares in shares_list_q:
                    shares: ROW_TickersData
                    price_counter = 0
                    for price in prices_list:
                        price: ROW_TickersData
                        price_counter += 1
                        if price.date >= shares.date:
                            price_value = 0
                            if price.date == shares.date:
                                price_value = price.value
                            else:
                                price_value = prices_list[price_counter-1].value

                            market_cap = ROW_TickersData()
                            market_cap.date = shares.date
                            market_cap.value = shares.value * price_value
                            market_cap_list_q.append(market_cap)
                            break
                """

                #market_cap_list_q.sort(key=lambda x: x.date, reverse=False)

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
    logger.info(f"calc_valuation_ratios_stocks - End")

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

def estimate_growth_stocks():
    logger.info(f"estimate_growth_stocks - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        tickers = dao_tickers.select_tickers_all__limited_ids()

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
                growth_list.append(growth_net_income[0] * 4)
                r_square_list.append(growth_net_income[1])
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
                growth_list.append(ebitda_growth[0] * 4)
                r_square_list.append(ebitda_growth[1])
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

            if len(growth_list) != 6:
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
                growth_list.append(growth_net_income[0])
                r_square_list.append(growth_net_income[1] ** 2)
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
                growth_list.append(ebitda_growth[0])
                r_square_list.append(ebitda_growth[1] ** 2)
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
    logger.info(f"estimate_growth_stocks - End")

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

def downloadStockOptionData():

    logger.info(f"downloadStockOptionData - Start")
    try:

        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        #stock = yf.Ticker('AACT-U')
        
        #skip = True
        ticker_list = dao_tickers.select_tickers_all__limited_usa_ids()
        counter = 0
        for ticker_id in ticker_list:
            #ticker:ROW_Tickers

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
                            dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_MONTH_AVG_PRICE, future_price, today)
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
                                    TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__OPTION_YEAR_DISCOUNT: year_discount
                                }
                                dao_tickers.update_ticker_types(ticker_id, dict_data, True)
                            dao_tickers_data.store_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_YEAR_AVG_PRICE, future_price, today)
                            #print(future_price)
                        break

                logger.info(f"Download Stock: Updated {ticker_id}")

            except Exception as err:
                logger.exception(f"Error updating ticker[{ticker_id}]:")
                continue
    except Exception as e:
        logger.error(f"downloadStockOptionData - Error {e}")
    logger.info(f"downloadStockOptionData - End")
           
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
    logger.info(f"calculate_continuous_metrics({earning_metric_const}) - Start")
    try:
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        fmp = FMP()

        ticker_list_orig = dao_tickers.select_tickers_all__limited_ids()
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
    logger.info(f"calculate_continuous_metrics({earning_metric_const}) - End")

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

def prepare_chart_data_EXTEND(ticker_data_list: list[ROW_TickersData], length: int):
    if len(ticker_data_list) == 0:
        return prepare_chart_data(ticker_data_list)
    
    len_input = len(ticker_data_list)
    for i in range(1,length - len_input):
        record = ROW_TickersData()
        record.value = ticker_data_list[-1].value
        record.date = ticker_data_list[-1].date - timedelta(days=1)
        ticker_data_list.append(record)
    return prepare_chart_data(ticker_data_list)
    

def prepare_chart_data(ticker_data_list: list[ROW_TickersData]):
    list_x = []
    list_y = []

    ticker_data_list.sort(key=lambda x: (x.date), reverse=False)

    for ticker_data in ticker_data_list:
        list_x.append(ticker_data.date)
        list_y.append(ticker_data.value)

    return [list_x, list_y]

def prepare_chart_data_TTM(ticker_data_list: list[ROW_TickersData]):
    list_x = []
    list_y = []

    ticker_data_list.sort(key=lambda x: (x.date), reverse=False)

    counter = -1
    for ticker_data in ticker_data_list:
        counter += 1
        if counter < 3:
            continue
        list_x.append(ticker_data.date)
        list_y.append(ticker_data_list[counter].value + ticker_data_list[counter-1].value + ticker_data_list[counter-2].value + ticker_data_list[counter-3].value)

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

    annual = 5
    days_back = annual * 250
    ticker = dao_tickers.select_ticker(ticker_id)
    eps_discount_row = ROW_TickersData()
    eps_discount_row.date = datetime.today()
    eps_discount_row.value = ticker.eps_valuation

    fcf_discount_row = ROW_TickersData()
    fcf_discount_row.date = datetime.today()
    fcf_discount_row.value = ticker.fcf_valuation

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, days_back)
    prepared_chart_data__price = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__TARGET_PRICE, days_back)
    prepared_chart_data__target_price = prepare_chart_data_EXTEND(data_list, days_back)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_MONTH_AVG_PRICE, days_back)
    prepared_chart_data__option_month_price = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_YEAR_AVG_PRICE, days_back)
    prepared_chart_data__option_year_price = prepare_chart_data_EXTEND(data_list, days_back)
    prepared_chart_data__eps_valuation = prepare_chart_data_EXTEND([eps_discount_row], days_back)
    prepared_chart_data__fcf_valuation = prepare_chart_data_EXTEND([fcf_discount_row], days_back)


    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS, days_back)
    prepared_chart_data__pe = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS, days_back)
    prepared_chart_data__pb = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS, days_back)
    prepared_chart_data__ps = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS, days_back)
    prepared_chart_data__pfcf = prepare_chart_data(data_list)


    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING_Q, annual * 4)
    prepared_chart_data__shares = prepare_chart_data(data_list)

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, (1 + annual) * 4)
    prepared_chart_data__revenue = prepare_chart_data_TTM(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_Q, (1 + annual) * 4)
    prepared_chart_data__gross = prepare_chart_data_TTM(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA_Q, (1 + annual) * 4)
    prepared_chart_data__ebitda = prepare_chart_data_TTM(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_Q, (1 + annual) * 4)
    prepared_chart_data__net_income = prepare_chart_data_TTM(data_list)



    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_ASSETS_Q, annual * 4)
    prepared_chart_data__assets = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_LIABILITIES_Q, annual * 4)
    prepared_chart_data__liabilities = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.STOCKHOLDER_EQUITY_Q, annual * 4)
    prepared_chart_data__equity = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.LONG_TERM_DEBT_Q, annual * 4)
    prepared_chart_data__long_term_debt = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_Q, annual * 4)
    prepared_chart_data__cash = prepare_chart_data(data_list)



    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, (1 + annual) * 4)
    prepared_chart_data__fcf = prepare_chart_data_TTM(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION_Q, (1 + annual) * 4)
    prepared_chart_data__fcf_oper = prepare_chart_data_TTM(data_list)



    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.STOCKHOLDER_EQUITY, annual)
    prepared_chart_data__balance_sheet = prepare_chart_data(data_list)

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF, annual)
    prepared_chart_data__free_cash_flow_statement = prepare_chart_data(data_list)

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_MARGIN_Q, annual * 4)
    prepared_chart_data__gross_margin = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPERATING_INCOME_MARGIN_Q, annual * 4)
    prepared_chart_data__operation_margin = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_MARGIN_Q, annual * 4)
    prepared_chart_data__net_margin = prepare_chart_data(data_list)
    

    ticker = dao_tickers.select_ticker(ticker_id)

    return render_template(
        'ticker.html', 
        ticker = ticker,
        plot_price = getChart(
            prepared_chart_data__price[0], 
            [
                prepared_chart_data__price[1],
                prepared_chart_data__target_price[1],
                prepared_chart_data__option_year_price[1],
                prepared_chart_data__eps_valuation[1],
                prepared_chart_data__fcf_valuation[1]
            ], 
            [
                f'Price ({prepared_chart_data__price[1][-1]:.2f})',
                f'Analyst target ({get_safe_value(prepared_chart_data__target_price)})',
                f'Option target ({get_safe_value(prepared_chart_data__option_year_price)})',
                f'EPS valuation ({get_safe_value(prepared_chart_data__eps_valuation)})',
                f'FCF valuation ({get_safe_value(prepared_chart_data__fcf_valuation)})'
            ], 
            'Price'
        ),
        plot_target_price = getChart(prepared_chart_data__target_price[0], [prepared_chart_data__target_price[1]], [''], f'Analysts Target Price ({get_safe_value(prepared_chart_data__target_price)})'),
        
        #plot_option_price_1M = getChart(prepared_chart_data__option_month_price[0], [prepared_chart_data__option_month_price[1]], [''], 'Option price 1M'),

        plot_margins = getChart(
            prepared_chart_data__gross_margin[0], 
            [
                prepared_chart_data__gross_margin[1],
                prepared_chart_data__operation_margin[1],
                prepared_chart_data__net_margin[1]
            ], 
            [
                f'Gross Margin ({prepared_chart_data__gross_margin[1][-1]*100:.2f}%)',
                f'Operating Margin ({prepared_chart_data__operation_margin[1][-1]*100:.2f}%)',
                f'Net Margin ({prepared_chart_data__net_margin[1][-1]*100:.2f}%)'
            ], 
            'Margins'
        ),


        plot_option_price_1Y = getChart(prepared_chart_data__option_year_price[0], [prepared_chart_data__option_year_price[1]], [''], f'Option price 1Y ({get_safe_value(prepared_chart_data__option_year_price)})'),

        plot_pe = getChart(prepared_chart_data__pe[0], [prepared_chart_data__pe[1]], [''], f'PE ({get_safe_value(prepared_chart_data__pe)})'),
        plot_pb = getChart(prepared_chart_data__pb[0], [prepared_chart_data__pb[1]], [''], f'PB ({get_safe_value(prepared_chart_data__pb)})'),
        plot_ps = getChart(prepared_chart_data__ps[0], [prepared_chart_data__ps[1]], [''], f'PS ({get_safe_value(prepared_chart_data__ps)})'),
        plot_pfcf = getChart(prepared_chart_data__pfcf[0], [prepared_chart_data__pfcf[1]], [''], f'PFCF ({get_safe_value(prepared_chart_data__pfcf)})'),

        plot_income_statement = getChart(
            prepared_chart_data__revenue[0], 
            [
                prepared_chart_data__revenue[1],
                prepared_chart_data__gross[1],
                prepared_chart_data__ebitda[1],
                prepared_chart_data__net_income[1]
            ], 
            [
                f'Revenue ({get_safe_growth_rate(prepared_chart_data__revenue)}% p.a.)',
                f'Gross Profit ({get_safe_growth_rate(prepared_chart_data__gross)}% p.a.)',
                f'EBITDA ({get_safe_growth_rate(prepared_chart_data__ebitda)}% p.a.)',
                f'Net Income ({get_safe_growth_rate(prepared_chart_data__net_income)}% p.a.)'
            ], 
            'Income statement (ttm)'
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
                f'Assets ({get_safe_growth_rate(prepared_chart_data__assets)}% p.a.)',
                f'Liabilities ({get_safe_growth_rate(prepared_chart_data__liabilities)}% p.a.)',
                f'Equity ({get_safe_growth_rate(prepared_chart_data__equity)}% p.a.)',
                f'Long debt ({get_safe_growth_rate(prepared_chart_data__long_term_debt)}% p.a.)',
                f'Cash ({get_safe_growth_rate(prepared_chart_data__cash)}% p.a.)'
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
                f'FCF Op ({get_safe_growth_rate(prepared_chart_data__fcf_oper)}% p.a.)',
                f'FCF ({get_safe_growth_rate(prepared_chart_data__fcf)}% p.a.)'
            ], 
            'FCF Statement (ttm)'
        ),     
        plot_shares = getChart(prepared_chart_data__shares[0], [prepared_chart_data__shares[1]], [''], f'Shares outstanding ({get_safe_growth_rate(prepared_chart_data__shares)}% p.a.)')
        
    )

def get_safe_value(value):
    try:
        return f'{value[1][-1]:.2f}'
    except:
        return 'NaN'

def get_safe_growth_rate(quarter_growth_data):
    try:
        return f'{predict_growth_rate(None, quarter_growth_data[1])[0]*4*100:.2f}'
    except Exception as e:
        return 'NaN'

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
    
    logger.info(f"sync_ticker_id_list - End")

def update_ticker_profile(refresh: bool):
    logger.info(f"update_ticker_profile({refresh}) - Start")
    try:
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)

        db_ticker_list = dao_tickers.select_tickers_all_ids()
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
    logger.info(f"update_ticker_profile - End")
    
def update_ticker_target_price():
    logger.info(f"update_ticker_target_price - Start")
    try:
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)
        today = datetime.today().date()

        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
        #db_ticker_list = ['AAON' ,'AAPL', 'GOOG', 'MPW']

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
    logger.info(f"update_ticker_target_price - End")

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
    logger.info(f"update_earnings_calendar - End")

def update_stock_recommendations():
    logger.info(f"update_stock_recommendations - Start")

    try:
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        #today = datetime.today().strftime("%Y-%m-%d")
        today = datetime.today().date()

        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
        #db_ticker_list = ['AAON' ,'AAPL', 'GOOG', 'MPW']


        counter = 0
        skip = True
        for ticker_id in db_ticker_list:
            counter += 1
            if ticker_id == 'WOBDX':
                skip = False
            #if skip:
            #    continue
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
                continue
    except Exception as e:
        logger.error(f"update_stock_recommendations - Error {e}")
    logger.info(f"update_stock_recommendations - End")

def update_dividends_info():
    fmp = FMP()

    connection = DB.get_connection_mysql()  
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    statement_list = fmp.get_statement_symbols_list()
    #db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
    db_ticker_list = ['AAPL']

    skip = True
    counter = 0
    for ticker_id in db_ticker_list:
        counter += 1
        if ticker_id not in statement_list:
            continue

        metrics = fmp.get_key_metrics_ttm(ticker_id)
        if len(metrics) > 0:
            pass

def download_fundamental_statements():
    logger.info(f"download_fundamental_statements - Start")
    try:
        fmp = FMP()

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        statement_list = fmp.get_statement_symbols_list()
        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
        #db_ticker_list = ['PATH', 'GOOG', 'MPW']

        skip = True
        counter = 0
        for ticker_id in db_ticker_list:
            counter += 1
            logger.info(f"download_fundamental_statements({ticker_id}) - {counter}/{len(db_ticker_list)}")
            if ticker_id not in statement_list:
                continue
            
            #if (ticker_id == "DSGX"):
            #    skip = False
            
            #if skip:
            #    continue
            
            last_record = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, 1)

            now = date.today()
            if len(last_record) > 0 and (now - last_record[0].date).days < 90:
                continue

            income_statement_q_list = fmp.get_income_statement(ticker_id, True)
            #print(counter)

            if len(income_statement_q_list) == 0:
                continue

            balance_sheet_statement_q_list = fmp.get_balance_sheet_statement(ticker_id, True)
            cash_flow_statement_q_list = fmp.get_cash_flow_statement(ticker_id, True)

            income_statement_a_list = fmp.get_income_statement(ticker_id, False)
            balance_sheet_statement_a_list = fmp.get_balance_sheet_statement(ticker_id, False)
            cash_flow_statement_a_list = fmp.get_cash_flow_statement(ticker_id, False)
            

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
    logger.info(f"download_fundamental_statements - End")

def calc_valuation_stocks():

    logger.info(f"calc_valuation_stocks - Start")
    try:

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
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

            if len(shares) == 0 or len(cash) == 0 or len(total_debt) == 0 or growth == None or shares[0].value == 0:
                continue

            growth *= 0.85
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
    logger.info(f"calc_valuation_stocks - End")

def calc_ratio_discounts():
    logger.info(f"calc_ratio_discounts - Start")
    try:

        connection = DB.get_connection_mysql()  
        dao_tickers = DAO_Tickers(connection)
        dao_tickers_data = DAO_TickersData(connection)

        db_ticker_list = dao_tickers.select_tickers_all__limited_ids()
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
    logger.info(f"calc_ratio_discounts - End")

def run_all_jobs_parallel():
    with ThreadPoolExecutor(max_workers=len(scheduler.get_jobs())) as executor:
        futures = [executor.submit(job.func, *job.args, **job.kwargs) for job in scheduler.get_jobs()]
        for future in futures:
            try:
                future.result()  # Wait for each job to complete
            except Exception as e:
                print(f"Job raised an exception: {e}")

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




    #scheduler.add_job(sync_ticker_id_list, 'cron', day_of_week='sun', hour=3, minute=30)
    scheduler.add_job(update_ticker_profile, 'cron', day_of_week='sun', hour=3, minute=30, args=[False])
    scheduler.add_job(update_earnings_calendar, 'cron',day_of_week='sun', hour=3, minute=30)
    

    scheduler.add_job(download_prices, 'cron',day_of_week='tue-sat', hour=0, minute=30)
    scheduler.add_job(update_ticker_target_price, 'cron',day_of_week='tue-sat', hour=0, minute=30)
    scheduler.add_job(update_stock_recommendations, 'cron',day_of_week='tue-sat', hour=0, minute=30)
    scheduler.add_job(downloadStockOptionData, 'cron',day_of_week='tue-sat', hour=0, minute=30)
    scheduler.add_job(download_fundamental_statements, 'cron',day_of_week='tue-sat', hour=0, minute=30)
    

    scheduler.add_job(estimate_growth_stocks, 'cron',day_of_week='tue-sat', hour=12, minute=30)
    scheduler.add_job(calculate_price_discount, 'cron',day_of_week='tue-sat', hour=12, minute=30)
    scheduler.add_job(calc_valuation_ratios_stocks, 'cron',day_of_week='tue-sat', hour=12, minute=30)
    scheduler.add_job(calc_valuation_stocks, 'cron',day_of_week='tue-sat', hour=12, minute=30)

    scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=12, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS])
    scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=12, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS])
    scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=12, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS])
    scheduler.add_job(calculate_continuous_metrics, 'cron', day_of_week='tue-sat', hour=12, minute=30, args=[TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS])

    scheduler.add_job(calc_ratio_discounts, 'cron',day_of_week='tue-sat', hour=12, minute=30)
    
    #sync_ticker_id_list()
    #update_ticker_profile(True)
    #update_earnings_calendar()
    
    #download_prices()
    #update_ticker_target_price()
    #update_stock_recommendations()
    #downloadStockOptionData()
    #download_fundamental_statements()
    
    #calc_valuation_ratios_stocks()
    #calculate_price_discount()
    #estimate_growth_stocks()
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS)
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS)
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS)
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__Q, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS)
    #calc_valuation_stocks()
    #calc_ratio_discounts()
    #calc_seasonality()

    #update_ticker_target_price()
    #update_stock_recommendations()
    #downloadStockOptionData()







    #update_dividends_info() - asi neni treba
    #calculate_continuous_metrics(TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS, TICKERS_TIME_DATA__TYPE__CONST.METRIC_SHARES__CONTINOUS)
    #rank_stocks()
    #calc_valuation_ratios_stocks()
    #valuate_stocks()

    #scheduler.start()
    #run_all_jobs_parallel()


    logger.info("Schedulers started v2.")
    app.run(debug=False,host='0.0.0.0')
    





#with socketserver.TCPServer(("", PORT), Handler) as httpd:
#    print(f"Serving at port {PORT}")
#    httpd.serve_forever()
