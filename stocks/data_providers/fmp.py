import os
import requests
from datetime import datetime

from .fmp_metrics import FMP_Metrics
from dotenv import load_dotenv

import fmpsdk

load_dotenv()

class FMPException_LimitReached(Exception):
    def __init__(self, message="Limit has been exceeded", value=None):
        self.message = message
        self.value = value
        super().__init__(self.message)

class FMP:

    def __init__(self):
        pass

    def get_statement_symbols_list(self) -> list[str]:
        return fmpsdk.financial_statement_symbol_lists(os.getenv("FMP_API_KEY"))
    
    def get_symbols_list(self, exchange):
        url = f'https://financialmodelingprep.com/api/v3/symbol/{exchange}?apikey={os.getenv("FMP_API_KEY")}'
        response = requests.get(url)

        if (response.status_code == 200):
            if 'application/json' in response.headers['Content-Type']:
                return response.json()
        return None
    
    def get_stock_profile(self, ticker_id: str):
        return fmpsdk.company_profile(os.getenv("FMP_API_KEY"), ticker_id)
    
    def get_stock_price_target(self, ticker_id: str) -> float:
        url = f'https://financialmodelingprep.com/api/v4/price-target-summary?symbol={ticker_id}&apikey={os.getenv("FMP_API_KEY")}'
        response = requests.get(url)

        if (response.status_code == 200):
            if 'application/json' in response.headers['Content-Type']:
                return response.json()
        return None

    def get_earnings_calendar(self, from_date: str, to_date: str):
        return fmpsdk.earning_calendar(os.getenv("FMP_API_KEY"), from_date, to_date)
    
    def get_recommendations(self, ticker_id: str):
        url = f'https://financialmodelingprep.com/api/v3/analyst-stock-recommendations/{ticker_id}?apikey={os.getenv("FMP_API_KEY")}'
        response = requests.get(url)

        if (response.status_code == 200):
            if 'application/json' in response.headers['Content-Type']:
                return response.json()
        return None
    
    def get_income_statement(self, ticker_id: str, quaterly: bool):
        return fmpsdk.income_statement(os.getenv("FMP_API_KEY"), ticker_id, 'quarter' if quaterly else 'annual', 100)
    
    def get_balance_sheet_statement(self, ticker_id: str, quaterly: bool):
        return fmpsdk.balance_sheet_statement(os.getenv("FMP_API_KEY"), ticker_id, 'quarter' if quaterly else 'annual', 100)
    
    def get_cash_flow_statement(self, ticker_id: str, quaterly: bool):
        return fmpsdk.cash_flow_statement(os.getenv("FMP_API_KEY"), ticker_id, 'quarter' if quaterly else 'annual', 100)
    
    def get_historic_prices(self, ticker_id: str, from_date: str, to_date: str):
        return fmpsdk.historical_price_full(os.getenv("FMP_API_KEY"), ticker_id, from_date, to_date)
    
    def get_key_metrics_ttm(self, ticker_id: str):
        return fmpsdk.key_metrics_ttm(os.getenv("FMP_API_KEY"), ticker_id, 100)
    
####################################################################################################################################################

    def get_metrics(self, ticker_id: str) -> list[FMP_Metrics]:
        url = f'https://financialmodelingprep.com/api/v3/key-metrics/{ticker_id}?period=annual&apikey={os.getenv("FMP_API_KEY")}'
        response = requests.get(url)

        if (response.status_code == 200):
            if 'application/json' in response.headers['Content-Type']:
                json = response.json()

                metricList = []

                date_format = "%Y-%m-%d"

                for record in json:
                    metrics = FMP_Metrics()
                    metrics.symbol=record['symbol']
                    metrics.date=datetime.strptime(record['date'], date_format).date()
                    metrics.pe=record['peRatio']
                    metrics.debt_ebitda=record['netDebtToEBITDA']
                    metrics.ev_ebitda=record['enterpriseValueOverEBITDA']
                    metrics.ev_fcf=record['evToFreeCashFlow']
                    metrics.ev_oper=record['evToOperatingCashFlow']
                    metrics.ev_sales=record['evToSales']
                    metrics.pb=record['pbRatio']
                    metrics.pbt=record['ptbRatio']
                    metrics.pfcf=record['pfcfRatio']
                    metrics.pocf=record['pocfratio']
                    metrics.ps=record['priceToSalesRatio']
                    metricList.append(metrics)

                return metricList
            else:
                raise Exception("The response is not json content.")
        elif response.status_code == 429:
            raise FMPException_LimitReached()
        else:
            raise Exception("Status code <> 200")
    
# TRADING ###################################################################################################################################################

    def get_forex_list(self):
        return fmpsdk.forex_list(os.getenv("FMP_API_KEY"))
    
    def get_commodities_list(self):
        return fmpsdk.commodities_list(os.getenv("FMP_API_KEY"))
    
    def fetch_candles(self, symbol: str, time_delta: str, from_date: str, to_date: str):
        return fmpsdk.historical_chart(os.getenv("FMP_API_KEY"), symbol, time_delta, from_date, to_date)
    
    def fetch_cot(self, symbol: str, from_date: str, to_date: str):
        return fmpsdk.commitment_of_traders_report(os.getenv("FMP_API_KEY"), symbol, from_date, to_date)
    
    def fetch_cot_analysis(self, symbol: str, from_date: str, to_date: str):
        return fmpsdk.commitment_of_traders_report_analysis(os.getenv("FMP_API_KEY"), symbol, from_date, to_date)


####################################################################################################################################################

if __name__ == "__main__":
    pass