import mysql
from datetime import date
from .row_tickers_data import ROW_TickersData
from mysql.connector.pooling import PooledMySQLConnection
from .constants import TIME_DATA_TYPE_CONST
from .db import DB

class DAO_TickersData:
    
    conn = None
    cursor = None

    def __init__(self, conn: PooledMySQLConnection):
        self.conn = conn
        self.cursor = conn.cursor()

    def execute(self, sql):
        try:
            self.cursor.execute(sql)
        except mysql.connector.Error as err:
            print("Error executing sql:", err)
            raise err
        return

    def insert_ticker_data(self, ticker_data_row: ROW_TickersData, forceCommit: bool):
        sql = f"INSERT INTO invest.tickers_data" \
        f" (payout_ratio, date, ticker_id, market_cap, current_price, target_price, eps, shares_outstanding, recommendation_mean, recommendation_strong_buy, recommendation_buy, recommendation_hold, recommendation_sell, recommendation_strong_sell, dividend_yield)" \
        f" VALUES({ticker_data_row.payout_ratio}, '{ticker_data_row.date}', '{ticker_data_row.ticker_id}', {ticker_data_row.market_cap}, {ticker_data_row.current_price}, {ticker_data_row.target_price}, {ticker_data_row.eps}, {ticker_data_row.shares_outstanding}, {ticker_data_row.recommendation_mean}, {ticker_data_row.recommendation_strong_buy}, {ticker_data_row.recommendation_buy}, {ticker_data_row.recommendation_hold}, {ticker_data_row.recommendation_sell}, {ticker_data_row.recommendation_strong_sell}, {ticker_data_row.dividend_yield})"
        self.execute(sql)
        if forceCommit:
            self.conn.commit()
        return

    def update_ticker_data(self, ticker_data_row: ROW_TickersData, force_commit: bool):
        #sql = f"update tickers_data set ticker_id='{ticker_row.ticker_id}', name='{ticker_row.name[0:99]}', industry='{ticker_row.industry[0:99]}', sector='{ticker_row.sector[0:99]}', isin='{ticker_row.isin[0:99]}' where ticker_id='{ticker_row.ticker_id}'"
        #self.execute(sql)
        #if force_commit:
        #    self.conn.commit()
        return     

    def select_tickers_all(self) -> list[ROW_TickersData] :
        try:
            self.cursor.execute("select * from tickers")
            all_db_tickers = self.cursor.fetchall()

            result = []
            for row in all_db_tickers:
                result.append(ROW_TickersData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14]))

            return result

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err         
        
    def select_ticker_data(self, ticker_id, date: date) -> ROW_TickersData :
        try:
            sql = f"select * from tickers_data where ticker_id='{ticker_id}' and date='{date}' LIMIT 1"
            self.cursor.execute(sql)
            row = self.cursor.fetchone()

            if row :
                return ROW_TickersData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14])
            else:
                return None

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err         

if __name__ == "__main__":
        conn = DB.get_connection_mysql()
        #cursor = conn.cursor()

        o = DAO_TickersData(conn)

        #DAO_Tickers.update_tickers(cursor, [["AMZN", "Amazon"],["AAPL", "Apple"]])
        #result = o.select_tickers_all()
        result = o.select_ticker("AAPL")
        #cursor.close()
        conn.commit()
        conn.close()
