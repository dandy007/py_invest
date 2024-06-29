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

from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
from .trading_constants import COMMODITY_LIST, COT_LIST, SYMBOL_TO_COT, FOREX_LIST
from .ticker_analyze import ROW_TickerAnalyze

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

def get_forex_tickers(filter: list[str]):

    fmp = FMP()
    result = fmp.get_forex_list()

    final_result = []

    for item in result:
        curr1 = item['symbol'][0:3]
        curr2 = item['symbol'][3:6]

        if curr1 in filter and curr2 in filter:
            final_result.append(item['symbol'])
    return final_result

def get_comm_tickers(filter: list[str]):

    fmp = FMP()
    result = fmp.get_commodities_list()

    final_result = []

    for item in result:

        if item['symbol'] in filter:
            final_result.append(item['symbol'])
    return final_result

def fetch_candles_last(symbol_list: list[str], timeframe: str, count: int) -> list:
    num_workers = 10
    if len(symbol_list) < 10:
        num_workers = len(symbol_list)

    start_time = time.time()

    data_chunk = []
    for symbol in symbol_list:
        data_chunk.append([symbol, timeframe])


    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(process_fetch_candles, data_chunk))

    end_time = time.time()

    logger.info(end_time - start_time)

    return results

def process_fetch_candles(data_chunk):
    fmp = FMP()
    return [data_chunk ,fmp.fetch_candles(data_chunk[0], data_chunk[1], None, None)]

def fetch_cot_analysis(symbol_list: list[str]) -> list:
    num_workers = 10
    if len(symbol_list) < 10:
        num_workers = len(symbol_list)

    start_time = time.time()

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(process_fetch_cot_analysis, symbol_list))

    end_time = time.time()

    logger.info(end_time - start_time)

    result_dict = dict()
    for record in results:
        result_dict[record[0]] = record[1]

    return result_dict

def process_fetch_cot_analysis(data_chunk):
    fmp = FMP()
    return [data_chunk ,fmp.fetch_cot_analysis(data_chunk, None, None)]

def get_cot_symbol_parts(symbol: str):
    result = [None, None]
    if len(symbol) >= 3:
        result[0] = SYMBOL_TO_COT[symbol[:-3]]
        result[1] = SYMBOL_TO_COT[symbol[-3:]]
    return result


def get_price_discount_z_score(price_data : list, length: int) -> []:
    try:
        list_prices = []
        list_volumes = []
        for i in range(1, len(price_data)):
            list_prices.append(price_data[len(price_data) - i]['close'])
            list_volumes.append(price_data[len(price_data) - i]['volume'])

        if len(list_prices) >= length:
            prices_list = []
            volumes_list = []
            for price, volume in zip(list_prices, list_volumes):
                prices_list.append(price)
                volumes_list.append(volume)

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

            prices_list = prices_list[-len(vwma):]
            vwma_price_diffs = [price - vwma for price, vwma in zip(prices_list, vwma)]

            std = np.std(vwma_price_diffs)
            zscores = stats.zscore(vwma_price_diffs)
            probabilities = stats.norm.pdf(zscores)  # Use PDF for probability density
            #probabilities = [0]

            discount = (vwma[0] - prices_list[0])/vwma[0]

            if math.isnan(probabilities[-1]):
                return [1.0, vwma]
            else:
                return [probabilities[-1], vwma]
    finally:
        pass

def load_symbol_analysis():
    result = []
    all_symbols = FOREX_LIST + COMMODITY_LIST
    #all_symbols = ['EURUSD']

    cot_data = fetch_cot_analysis(COT_LIST)
    #cot_data = []
    candle_data_list = {
        'M' : fetch_candles_last(all_symbols, 'M', 0),
        'W' : fetch_candles_last(all_symbols, 'W', 0),
        'D' : fetch_candles_last(all_symbols, 'D', 0),
        '4H' : fetch_candles_last(all_symbols, '4hour', 0),  
        'H' : fetch_candles_last(all_symbols, '1hour', 0),  
        'M5' : fetch_candles_last(all_symbols, '5min', 0)
    }

    for symbol in all_symbols:
        analyze = ROW_TickerAnalyze()
        analyze.vwma_prob = [0,0]
        analyze.symbol = symbol

        
        count = -1
        for timeframe, candle_data in candle_data_list.items():
            data = None
            if timeframe == 'H':
                count = 0
            if timeframe == 'M5':
                count = 1
            for symbol_data in candle_data:
                if symbol_data[0][0] == symbol:
                    data = symbol_data[1]
                    break
    
            if data == None:
                continue

            # Technical analyze
            tech_result = 0
            data_len = len(data)
            if (timeframe in ('W', 'M', 'D', '4H', 'H', 'M5') and data_len >= 5):
                for i in range(1, 5):
                    record = data[5-i]
                    prev_record = data[6-i]
                    if record['close'] > prev_record['high']:
                        tech_result = 1
                    if record['close'] < prev_record['low']:
                        tech_result = -1

                if tech_result != 0:
                    if timeframe == 'M5':
                        analyze.engulf_5m = tech_result
                    if timeframe == 'H':
                        analyze.engulf_1h = tech_result
                    if timeframe == '4H':
                        analyze.engulf_4h = tech_result
                    if timeframe == 'D':
                        analyze.engulf_day = tech_result
                    if timeframe == 'W':
                        analyze.engulf_week = tech_result
                    if timeframe == 'M':
                        analyze.engulf_month = tech_result


            # ===================== VWMA
            if (timeframe not in ('W', 'M', 'D')):
                vwma_data100 = get_price_discount_z_score(data, 100)
                vwma_data200 = get_price_discount_z_score(data, 200)
                if vwma_data100 == None or vwma_data200 == None:
                    continue
                vwma100 = vwma_data100[1]
                vwma200 = vwma_data200[1]
                prob100 = vwma_data100[0]
                prob200 = vwma_data200[0]
                if (prob100 < 0.15 or prob200 < 0.15) and len(vwma200) > 200:
                    isBull100 = vwma100[-100] < vwma100[-1]
                    isBull200 = vwma200[-200] < vwma200[-1]
                    if (isBull100 and data[0]['close'] < vwma100[-1]) or (not isBull100 and data[0]['close'] > vwma100[-1]) or (isBull200 and data[0]['close'] < vwma200[-1]) or (not isBull200 and data[0]['close'] > vwma200[-1]):
                        analyze.vwma_prob[count] = 1
            
            # ===================== COT report
            if (timeframe in ('H')):
                symbol_parts = get_cot_symbol_parts(symbol)
                cot_parts = [None, None]
                cot_parts[0] = cot_data[symbol_parts[0]][0]
                cot_parts[1] = cot_data[symbol_parts[1]][0]

                if 'Bullish' in cot_parts[0]['marketSituation']:
                    analyze.cot_longterm += 1
                elif 'Bearish' in cot_parts[0]['marketSituation']:
                    analyze.cot_longterm -= 1
                else:
                    raise Exception()
                    # analyze.cot_longterm = None

                if 'Bullish' in cot_parts[1]['marketSituation']:
                    analyze.cot_longterm -= 1
                elif 'Bearish' in cot_parts[1]['marketSituation']:
                    analyze.cot_longterm += 1
                else:
                    raise Exception()
                    # analyze.cot_longterm = None

                
                if 'Bullish' in cot_parts[0]['marketSentiment']:
                    analyze.cot_shortterm += 1
                    if 'Increasing' in cot_parts[0]['marketSentiment']:
                        analyze.cot_shortterm += 1
                elif 'Bearish' in cot_parts[0]['marketSentiment']:
                    analyze.cot_shortterm -= 1
                    if 'Increasing' in cot_parts[0]['marketSentiment']:
                        analyze.cot_shortterm -= 1
                else:
                    raise Exception()
                
                if 'Bullish' in cot_parts[1]['marketSentiment']:
                    analyze.cot_shortterm -= 1
                    if 'Increasing' in cot_parts[1]['marketSentiment']:
                        analyze.cot_shortterm -= 1
                elif 'Bearish' in cot_parts[1]['marketSentiment']:
                    analyze.cot_shortterm += 1
                    if 'Increasing' in cot_parts[1]['marketSentiment']:
                        analyze.cot_shortterm += 1
                else:
                    raise Exception()

                analyze.total = analyze.cot_longterm + analyze.cot_shortterm + analyze.engulf_month + analyze.engulf_week + analyze.engulf_day
        result.append(analyze)
    return result


@app.route('/trading')
def trading():
    symbol_list = load_symbol_analysis()
    symbol_list : list [ROW_TickerAnalyze]
    symbol_list.sort(key=lambda x: x.total, reverse=True)
    #ticker_list.sort(key=lambda x: (x.price_discount_3 if x.price_discount_3 is not None else -float('inf')), reverse=True)
    return render_template('trading.html', symbol_list = symbol_list)


    

if __name__ == "__main__":

    #get_forex_tickers(filter_list_forex)
    #get_comm_tickers(filter_list_comm)
    #fetch_candles_last(None, None, None)

    #full_list = get_forex_tickers(filter_list_forex) + filter_list_comm
    #full_list = ['EURUSD']

    #result = fetch_candles_last(full_list, '5min', 0)

    #fmp = FMP()
    #result = fetch_cot_analysis(filter_list_cot)



    #result = load_symbol_analysis()

    #print()

    #with mp.Pool(num_workers) as pool:
    #    results = pool.map(process_data, data)

   
    app.run(debug=False,host='0.0.0.0', port=5010)
