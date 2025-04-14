from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from stocks.db.db import DB
from stocks.db.dao_tickers import DAO_Tickers
from stocks.db.dao_tickers_data import DAO_TickersData
from stocks.db.constants import TICKERS_TIME_DATA__TYPE__CONST
from fastapi.middleware.cors import CORSMiddleware
from datetime import timedelta, datetime
from stocks.db.row_tickers_data import ROW_TickersData
import logging
from logging.handlers import RotatingFileHandler
import numpy as np
from scipy import stats
import traceback
import yfinance as yf
import pandas as pd


fastApiApp = FastAPI()

# Create a custom logger
logger = logging.getLogger('api_logger')
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

# Allow requests from your Vue app running on localhost:4000.
origins = [
    "*",
    # You can add more origins if needed, or use "*" to allow all (not recommended for production).
]

fastApiApp.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # List the origins that are allowed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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



@fastApiApp.get("/options/get_expirations/{ticker_id}")
def get_expirations(ticker_id: str):
    """
    Retrieves available expiration dates for options of a given stock ticker.
    Returns the expiration dates in the following structure:

    {
        "ticker_id": "AAPL",
        "expiration_dates": [
            "2023-10-20",
            "2023-10-27",
            ...
        ]
    }

    Args:
        ticker_id: The stock ticker symbol

    Returns:
        JSON response containing the expiration dates
    """
    logger.info(f"get_expirations({ticker_id}) - Start")
    try:
        stock = yf.Ticker(ticker_id)
        expirations = stock.options
        return {
            "ticker_id": ticker_id,
            "expiration_dates": expirations
        }
    except Exception as e:
        logger.error(f"Error fetching expiration dates: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch expiration dates")

@fastApiApp.get("/options/get_chain/{ticker_id}/{expiration}/{option_type}")
def get_chain(ticker_id: str, expiration: str, option_type: str):
    """
    Retrieves option chain data for a given stock ticker, expiration date and option type.
    Returns the option chain data in the following structure:

    {
        "expiration": "2023-10-20",
        "option_type": "call",
        "option_chain": [
            {
                "strike": 150.0,
                "last_price": 5.0,
                "bid": 4.5,
                "ask": 5.5,
                "volume": 100,
                "open_interest": 200
            },
            ...
        ]
    }

    Args:
        ticker_id: The stock ticker symbol
        expiration: The expiration date of the options (YYYY-MM-DD)
        option_type: The type of option (call/put/both)

    Returns:
        JSON response containing the option chain data for all strikes
    """
    logger.info(f"get_chain({ticker_id}, {expiration}, {option_type}) - Start")
    try:
        stock = yf.Ticker(ticker_id)
        exp_date = datetime.strptime(expiration, '%Y-%m-%d').date()
        
        # Get option chain for the specified expiration
        opt = stock.option_chain(expiration)
        
        result = {
            "expiration": expiration,
            "option_type": option_type,
            "option_chain": []
        }

        # Process based on option type
        chains = []
        if option_type.lower() == 'call':
            chains = [opt.calls]
        elif option_type.lower() == 'put':
            chains = [opt.puts]
        elif option_type.lower() == 'both':
            chains = [opt.calls, opt.puts]
        
        # Process each chain
        for chain in chains:
            for _, row in chain.iterrows():
                result["option_chain"].append({
                    "strike": float(row['strike']),
                    "last_price": float(row['lastPrice']),
                    "bid": float(row['bid']),
                    "ask": float(row['ask']),
                    "volume": int(row['volume']) if not pd.isna(row['volume']) else 0,
                    "open_interest": int(row['openInterest']) if not pd.isna(row['openInterest']) else 0
                })
        
        return result

    except Exception as e:
        logger.error(f"Error fetching option chain: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch option data")

@fastApiApp.get("/options/growth_probability/{ticker_id}/{days}/{percent_range}")
def growthProbability(ticker_id: str, days: int, percent_range: int):
    """
    Calculates probabilities of stock price changes exceeding thresholds over specified days.
    Returns detailed statistics and histogram data in the following structure:

    {
        "probabilities": {
            "-10": 15.5,  # Probability of 15.5% that price decreases by 10% or more
            "10": 12.3,   # Probability of 12.3% that price increases by 10% or more
            ...
        },
        "statistics": {
            "total_prices": 1000,
            "periods_analyzed": 950,
            "mean_change": 2.5,
            "std_dev": 3.2,
            "std_dev_2": 6.4,
            "min_change": -15.2,
            "max_change": 20.1
        },
        "histogram": [
            {
                "bin_start": -20.0,
                "bin_end": -19.2,
                "count": 5,
                "markers": ["-2SD"]  # Can include: "MEAN", "-1SD", "+1SD", "-2SD", "+2SD"
            },
            ...
        ]
    }

    Args:
        ticker_id: The stock ticker symbol
        days: Number of days for price change calculation
        percent_range: Maximum percentage threshold (positive/negative) for probabilities
    """
    logger.info(f"growthProbability({ticker_id}, {days}, {percent_range}) - Start")
    result = {
        "probabilities": {},
        "statistics": {},
        "histogram": []
    }
    
    connection = None
    try:
        connection = DB.get_connection_mysql()
        dao_tickers_data = DAO_TickersData(connection)

        prices_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, 5*250)
        num_prices = len(prices_list)

        if not prices_list:
            logger.warning(f"No price data found for ticker {ticker_id}")
            return result

        prices_list.sort(key=lambda x: x.date, reverse=False)
        changes_pct = []

        if num_prices <= days:
            logger.warning(f"Insufficient price data for {ticker_id}")
            return result

        for i in range(num_prices - days):
            price_start = prices_list[i].value
            price_end = prices_list[i + days].value
            if price_start is not None and price_end is not None and price_start != 0:
                change = ((price_end - price_start) / price_start) * 100
                changes_pct.append(change)

        if not changes_pct:
            return result

        changes_array = np.array(changes_pct)
        total_changes = len(changes_array)

        # Calculate probabilities
        for x in range(-percent_range, percent_range + 1):
            if x == 0:
                continue
            count = np.sum(changes_array >= x) if x > 0 else np.sum(changes_array <= x)
            prob = (count / total_changes) * 100 if total_changes > 0 else 0
            result["probabilities"][str(x)] = round(prob, 2)

        # Calculate statistics
        result["statistics"] = {
            "total_prices": num_prices,
            "periods_analyzed": total_changes
        }

        if total_changes > 0:
            mean_change = np.mean(changes_array)
            std_dev = np.std(changes_array)
            result["statistics"].update({
                "mean_change": round(mean_change, 2),
                "std_dev": round(std_dev, 2),
                "std_dev_2": round(2 * std_dev, 2),
                "min_change": round(np.min(changes_array), 2),
                "max_change": round(np.max(changes_array), 2)
            })

            # Generate histogram
            num_bins = 100
            counts, bin_edges = np.histogram(changes_array, bins=num_bins)
            mean_bin = np.digitize(mean_change, bin_edges) - 1
            sd1_minus_bin = np.digitize(mean_change - std_dev, bin_edges) - 1
            sd1_plus_bin = np.digitize(mean_change + std_dev, bin_edges) - 1
            sd2_minus_bin = np.digitize(mean_change - std_dev * 2, bin_edges) - 1
            sd2_plus_bin = np.digitize(mean_change + std_dev * 2, bin_edges) - 1

            for i in range(num_bins):
                markers = []
                if i == mean_bin: markers.append("MEAN")
                if i == sd1_minus_bin: markers.append("-1SD")
                if i == sd1_plus_bin: markers.append("+1SD")
                if i == sd2_minus_bin: markers.append("-2SD")
                if i == sd2_plus_bin: markers.append("+2SD")

                result["histogram"].append({
                    "bin_start": round(bin_edges[i], 2),
                    "bin_end": round(bin_edges[i+1], 2),
                    "count": int(counts[i]),
                    "markers": markers
                })

    except Exception as e:
        logger.error(f"growthProbability - Error processing {ticker_id}: {e}")
        traceback.print_exc()
    finally:
        if connection and connection.is_connected():
            connection.close()
            logger.debug("Database connection closed.")
            
    logger.info(f"growthProbability({ticker_id}) - End")
    return result

@fastApiApp.get("/stock/current_price/{ticker_id}")
def get_current_price(ticker_id: str):
    """
    Retrieves the current market price for a given stock ticker.
    Returns the price data in the following structure:

    {
        "ticker_id": "AAPL",
        "price": 150.25,
        "timestamp": "2023-10-20T15:30:00"
    }

    Args:
        ticker_id: The stock ticker symbol

    Returns:
        JSON response containing current price and timestamp
    """
    logger.info(f"get_current_price({ticker_id}) - Start")
    try:
        stock = yf.Ticker(ticker_id)
        current_price = stock.info.get('regularMarketPrice')
        if current_price is None:
            raise HTTPException(status_code=404, detail="Price data not available")
            
        return {
            "ticker_id": ticker_id,
            "price": current_price,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error fetching current price: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch price data")

@fastApiApp.get("/stock/{ticker_id}")
def get_stock(ticker_id: str):
    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    annual = 5
    days_back = annual * 250
    ticker = dao_tickers.select_ticker(ticker_id)

    data = {}
    data['TICKER'] = ticker.ticker_id
    data['TICKER_NAME'] = ticker.name
    data['TICKER_DESCRIPTION'] = ticker.description


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
    #data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_MONTH_AVG_PRICE, days_back)
    #prepared_chart_data__option_month_price = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_YEAR_AVG_PRICE, days_back)
    prepared_chart_data__option_year_price = prepare_chart_data_EXTEND(data_list, days_back)
    prepared_chart_data__eps_valuation = prepare_chart_data_EXTEND([eps_discount_row], days_back)
    prepared_chart_data__fcf_valuation = prepare_chart_data_EXTEND([fcf_discount_row], days_back)


    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_MARGIN_Q, annual * 4)
    prepared_chart_data__gross_margin = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPERATING_INCOME_MARGIN_Q, annual * 4)
    prepared_chart_data__operation_margin = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_MARGIN_Q, annual * 4)
    prepared_chart_data__net_margin = prepare_chart_data(data_list)


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


    # Price chart
    data['PRICE_DATA'] = prepared_chart_data__price
    data['TARGET_PRICE_DATA'] = prepared_chart_data__target_price
    data['OPTION_PRICE_DATA'] = prepared_chart_data__option_year_price
    data['EPS_PRICE_DATA'] = prepared_chart_data__eps_valuation
    data['FCF_PRICE_DATA'] = prepared_chart_data__fcf_valuation

    # Margins chart
    data['GROSS_MARGIN'] = prepared_chart_data__gross_margin
    data['OPER_MARGIN'] = prepared_chart_data__operation_margin
    data['NET_MARGIN'] = prepared_chart_data__net_margin

    # Valuation charts - PE, PS, PB, PFCF - each have separate chart
    data['PE'] = prepared_chart_data__pe
    data['PS'] = prepared_chart_data__pb
    data['PB'] = prepared_chart_data__ps
    data['PFCF'] = prepared_chart_data__pfcf

    # Income statement chart
    data['REVENUE'] = prepared_chart_data__revenue
    data['GROSS_PROFIT'] = prepared_chart_data__gross
    data['EBITDA'] = prepared_chart_data__ebitda
    data['NET_INCOME'] = prepared_chart_data__net_income

    # Balance sheet chart
    data['ASSETS'] = prepared_chart_data__assets
    data['LIABILITIES'] = prepared_chart_data__liabilities
    data['EQUITY'] = prepared_chart_data__equity
    data['LONG_TERM_DEBT'] = prepared_chart_data__long_term_debt
    data['CASH'] = prepared_chart_data__cash

    # FCF Statement chart
    data['OPER_FCF'] = prepared_chart_data__fcf_oper
    data['FCF'] = prepared_chart_data__fcf

    # Shares outstanding chart
    data['SHARES_OUTSTANDING'] = prepared_chart_data__shares

    return data
    raise HTTPException(status_code=404, detail="Stock not found")