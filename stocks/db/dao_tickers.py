import mysql
from .row_tickers import ROW_Tickers
from .constants import TICKERS__TYPE_TO_COLUMN__DICT
from mysql.connector.pooling import PooledMySQLConnection

from .db import DB

class DAO_Tickers:
    
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

    def insert_ticker(self, ticker_row: ROW_Tickers):
        sql = f"insert into tickers (ticker_id, name, industry, sector, isin) VALUES ('{ticker_row.ticker_id}','{ticker_row.name[0:99]}','{ticker_row.industry[0:99]}','{ticker_row.sector[0:99]}','{ticker_row.isin[0:99]}')"
        self.execute(sql)
        return

    def update_ticker(self, ticker_row: ROW_Tickers, force_commit: bool):
        sql = f"update tickers set ticker_id='{ticker_row.ticker_id}', name='{ticker_row.name[0:99]}', industry='{ticker_row.industry[0:99]}', sector='{ticker_row.sector[0:99]}', isin='{ticker_row.isin[0:99]}' where ticker_id='{ticker_row.ticker_id}'"
        values = (ticker_row.ticker_id)
        self.execute(sql, values)
        if force_commit:
            self.conn.commit()
        return    

    def update_ticker_base_data(self, ticker_row: ROW_Tickers, force_commit: bool):
        sql = f"update tickers set name=%s, industry=%s, sector=%s, isin=%s where ticker_id = %s"
        values = (ticker_row.name, ticker_row.industry, ticker_row.sector, ticker_row.isin, ticker_row.ticker_id)
        self.cursor.execute(sql, values)
        if force_commit:
            self.conn.commit()
        return    

    def update_ticker_types(self, ticker_row: ROW_Tickers, types_dict, force_commit: bool):
        sql = f"update tickers set "
        values = []
        for type, value in types_dict.items():
                if value not in (None, '', 'Infinity'):
                    type_column = TICKERS__TYPE_TO_COLUMN__DICT.get(type, None)
                    if type_column != None:
                        if len(values) > 0:
                            sql += ','
                        sql += f" {type_column}=%s"
                        values.append(value)
        where = f" where ticker_id = %s"
        values.append(ticker_row.ticker_id)
        sql = sql + where
        self.cursor.execute(sql, values)
        if force_commit:
            self.conn.commit()
        return    

    def update_tickers(self, ticker_data_list):
    
        try:
            self.cursor.execute("select * from tickers")
            all_db_tickers = self.cursor.fetchall()

            ticker_db_dict = {}

            for ticker in all_db_tickers:
                ticker_db_dict[ticker[0]] = ticker

            for ticker in ticker_data_list:
                value = ticker_db_dict.get(ticker[0])
                if value is not None:
                    self.update_ticker(ticker)
                else:
                    self.insert_ticker(ticker)
            
            print("Record inserted successfully!")
        except mysql.connector.Error as err:
            print("Error inserting data:", err)
    
    def select_tickers_all(self) -> list[ROW_Tickers] :
        try:
            self.cursor.execute("select * from tickers")
            all_db_tickers = self.cursor.fetchall()

            result = []
            for row in all_db_tickers:
                result.append(ROW_Tickers(row[0], row[1], row[2], row[3], row[4]))

            return result

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err         
        
    def select_ticker(self, ticker_id) -> ROW_Tickers :
        try:
            sql = f"select * from tickers where ticker_id='{ticker_id}' LIMIT 1"
            self.cursor.execute(sql)
            row = self.cursor.fetchone()

            return ROW_Tickers(row[0], row[1], row[2], row[3], row[4])

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err         

if __name__ == "__main__":
        conn = DB.get_connection_mysql()
        #cursor = conn.cursor()

        o = DAO_Tickers(conn)

        #DAO_Tickers.update_tickers(cursor, [["AMZN", "Amazon"],["AAPL", "Apple"]])
        #result = o.select_tickers_all()
        result = o.select_ticker("AAPL")
        #cursor.close()
        conn.commit()
        conn.close()
