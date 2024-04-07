import csv
import os
import requests

from dotenv import load_dotenv

load_dotenv()

def get_tickers_reader():
    url = f'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey='+os.getenv('ALPHAVANTAGE_API_KEY')
    response = requests.get(url)

    if (response.status_code == 200):
        if response.headers['Content-Type'] == 'application/x-download':
            reader = csv.reader(response.content.decode('utf-8').splitlines())
            return reader
        else:
            raise Exception("The response is not downloadable file.")
    else:
        raise Exception("Status code <> 200")

def get_tickers_download():
    return get_tickers(get_tickers_reader())

# symbol,name,exchange,assetType,ipoDate,delistingDate,status
def get_tickers(csvDataReader):
    next(csvDataReader) # skip header
    tickers = []
    for row in csvDataReader:
        tickerId = row[0]
        name = row[1]
        assetType = row[3]
        status = row[6]

        if assetType.lower() == "stock" and status.lower() == "active":
            tickers.append([tickerId, name])

    return tickers
    


if __name__ == "__main__":
    tickerList = get_tickers(get_tickers_reader())
    for row in tickerList:
        print(f"Ticker: {row[0]} Name: {row[1]}")