import mysql
import datetime
from .row_tickers_data import ROW_TickersData
from mysql.connector.pooling import PooledMySQLConnection
from .db import DB
import math

class DAO_TickersData:

    table_name = "tickers_time_data"
    
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
        sql = f"INSERT INTO {self.table_name}" \
        f" (ticker_id, date, type, value)" \
        f" VALUES(%s, %s, %s, %s)"
        values = (ticker_data_row.ticker_id, ticker_data_row.date, ticker_data_row.type, ticker_data_row.value)
        self.cursor.execute(sql, values)
        if forceCommit:
            self.conn.commit()
        return
    
    def update_ticker_data(self, ticker_data_row: ROW_TickersData, forceCommit: bool):
        sql = f"UPDATE {self.table_name}" \
        f" SET value=%s where ticker_id=%s and type=%s and date=%s" 
        values = (ticker_data_row.value, ticker_data_row.ticker_id, ticker_data_row.type, ticker_data_row.date)
        self.cursor.execute(sql, values)
        if forceCommit:
            self.conn.commit()
        return

    def store_ticker_data(self, ticker_id, type, value, date):
        if date == None:
            date = datetime.date.today()
        if value != None and type != None:
            record = self.get_data(ticker_id, type, date)
            if  record != None:
                if value != None and value != record.value and record.date != date:
                    record.date = date
                    record.value = value
                    self.insert_ticker_data(record, True)
                
            elif value not in (None, ''):
                if math.isnan(value):
                    value = 0
                ticker_data = ROW_TickersData()
                ticker_data.ticker_id = ticker_id
                if date == None:
                    ticker_data.date = date
                else:
                    ticker_data.date = date
                ticker_data.type = type
                ticker_data.value = value
                self.insert_ticker_data(ticker_data, True)
        return     

    def get_data(self, ticker_id, type, date) -> ROW_TickersData:
        if date == None:
            sql = f"select ticker_id, date, type, value from {self.table_name} where ticker_id = %s and type = %s order by date DESC LIMIT 1"
            values = (ticker_id, type)
        else: 
            sql = f"select ticker_id, date, type, value from {self.table_name} where ticker_id = %s and type = %s and date=%s LIMIT 1"
            values = (ticker_id, type, date)
        self.cursor.execute(sql, values)
        row = self.cursor.fetchone()

        if row:
            ticker_data = ROW_TickersData()
            ticker_data.ticker_id = row[0]
            ticker_data.date = row[1]
            ticker_data.type = row[2]
            ticker_data.value = row[3]
            return ticker_data
        else:
            return None

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
    
    def select_ticker_data(self, ticker_id: str, type: int, limit: int) -> list[ROW_TickersData] :
        try:
            if limit == -1:
                limit = 1000000
            sql = f"select date, value from tickers_time_data where ticker_id='{ticker_id}' and type='{type}' order by date DESC LIMIT {limit}"
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()

            result_list = []

            for row in rows:
                data = ROW_TickersData()
                data.ticker_id = ticker_id
                data.type = type
                data.date = row[0]
                data.value = row[1]
                result_list.append(data)

            return result_list

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
