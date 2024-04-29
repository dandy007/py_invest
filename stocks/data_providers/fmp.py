import os
import requests
from datetime import datetime

from .fmp_metrics import FMP_Metrics
from dotenv import load_dotenv

load_dotenv()

class FMPException_LimitReached(Exception):
    def __init__(self, message="Limit has been exceeded", value=None):
        self.message = message
        self.value = value
        super().__init__(self.message)

class FMP:

    def __init__(self):
        pass

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
    
####################################################################################################################################################

if __name__ == "__main__":
    pass