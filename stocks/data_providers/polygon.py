import os
import requests
from datetime import datetime

from polygon import RESTClient
from dotenv import load_dotenv
from .poly_fundamentals import FinancialReport, CompanyReport, FinancialItem, FinancialStatement, POLY_CONSTANTS

load_dotenv()

#class FMPException_LimitReached(Exception):
#    def __init__(self, message="Limit has been exceeded", value=None):
#        self.message = message
#        self.value = value
#        super().__init__(self.message)

class POLYGON:

    def __init__(self):
        pass

####################################################################################################################################################

    def get_polygon_client(self) -> RESTClient:
        return RESTClient(api_key=os.getenv("POLYGON_API_KEY"))
    
    def get_fundamentals_raw(self, ticker_id: str, timeframe: str) -> FinancialReport:
        url = f"https://api.polygon.io/vX/reference/financials?ticker={ticker_id}&timeframe={timeframe}&limit=100&apiKey={os.getenv('POLYGON_API_KEY')}"

        # Odeslani HTTP GET pozadavku
        response = requests.get(url)

        # Kontrola, zda byl pozadavek uspesny (status code 200)
        if response.status_code == 200:
            # Nacteni JSON data z odpovedi
            data = response.json()

            

            report = FinancialReport(results=data['results'])
            return report
        else:
            raise Exception(f"Failed to retrieve data. HTTP code: {response.status_code}")

if __name__ == "__main__":
    pass