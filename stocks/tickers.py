from data_providers.alpha_vantage import get_tickers_download
from db.db import get_connection
from stocks.db.dao_tickers import update_tickers

if __name__ == "__main__":
    tickers = get_tickers_download()
    
    if len(tickers) > 0:
        conn = get_connection()
        cursor = conn.cursor()

        update_tickers(cursor, tickers)
        cursor.close()
        conn.commit()
        conn.close()


    