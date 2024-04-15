import yfinance as yf
import pandas as pd

if __name__ == "__main__":

    # Get the stock ticker
    ticker = 'NVDA'#input("Enter the stock ticker: ")

    # Get the stock data
    stock = yf.Ticker(ticker)

    # Get the EPS
    eps = stock.info['trailingEps']
    print(f"EPS: {eps}")

    # Get the target price
    target_price = stock.info['targetMeanPrice']
    print(f"Target Price: {target_price}")

    # Get the current price
    current_price = stock.info['currentPrice']
    print(f"Current Price: {current_price}")

    print(f"Shares: {stock.info['sharesOutstanding']}")
    print(f"MarketCap: {stock.info['marketCap']}")
    print(f"Isin: {stock.isin}")



    print(f"Strong Sell: {stock.recommendations_summary.strongSell[0]}")
    print(f"Sell: {stock.recommendations_summary.sell[0]}")
    print(f"Hold: {stock.recommendations_summary.hold[0]}")
    print(f"Buy: {stock.recommendations_summary.buy[0]}")
    print(f"Strong Buy: {stock.recommendations_summary.strongBuy[0]}")
    print(f"Recommendation: {stock.info['recommendationMean']}")
    print(f"Industry: {stock.info['industry']}")
    print(f"Sector: {stock.info['sector']}")
    print(f"DividendYield: {stock.info['dividendYield']}")
    print(f"PayoutRatio: {stock.info['payoutRatio']}")
    #print(f"PayoutRatio: {stock.earnings_forecasts}")
    print(f"EarningsQuarterlyGrowth: {stock.info['earningsQuarterlyGrowth']}")
    print(f"EarningsGrowth: {stock.info['earningsGrowth']}")

