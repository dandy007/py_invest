import mysql
import math
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
        sql = f"update tickers set name=%s, industry=%s, sector=%s, isin=%s, earnings_date=%s, description=%s where ticker_id = %s"
        values = (ticker_row.name, ticker_row.industry, ticker_row.sector, ticker_row.isin, ticker_row.earnings_date, ticker_row.description, ticker_row.ticker_id)
        self.cursor.execute(sql, values)
        if force_commit:
            self.conn.commit()
        return    

    def update_ticker_types(self, ticker_row: ROW_Tickers, types_dict, force_commit: bool):
        sql = f"update tickers set "
        values = []
        for type, value in types_dict.items():
                if value not in (None, '', 'Infinity') and not math.isnan(value) :
                    type_column = TICKERS__TYPE_TO_COLUMN__DICT.get(type, None)
                    if type_column != None:
                        if len(values) > 0:
                            sql += ','
                        sql += f" {type_column}=%s"
                        values.append(value)
        if len(values) == 0:
            return
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
                ticker = DAO_Tickers.mapRow2ROW_Tickers(row)
                result.append(ticker)

            return result

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err
        
    def select_tickers_where(self, where: str) -> list[ROW_Tickers] :
        try:
            self.cursor.execute(f"select * from tickers where {where}")
            all_db_tickers = self.cursor.fetchall()

            result = []
            for row in all_db_tickers:
                result.append(DAO_Tickers.mapRow2ROW_Tickers(row))

            return result

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err         
        
    def select_ticker(self, ticker_id) -> ROW_Tickers :
        try:
            sql = f"select * from tickers where ticker_id='{ticker_id}' LIMIT 1"
            self.cursor.execute(sql)
            row = self.cursor.fetchone()

            ticker = None

            if row != None:
                ticker = DAO_Tickers.mapRow2ROW_Tickers(row)

            return ticker

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err         

    staticmethod
    def mapRow2ROW_Tickers(row) -> ROW_Tickers:
        ticker = ROW_Tickers()
        ticker.ticker_id = row[0]
        ticker.name = row[1]
        ticker.industry = row[2]
        ticker.sector = row[3]
        ticker.isin = row[4]
        ticker.market_cap = row[5]
        ticker.price = row[6]
        ticker.target_price = row[7]
        ticker.pe = row[8]
        ticker.recomm_mean = row[9]
        ticker.recomm_count = row[10]
        ticker.div_yield = row[11]
        ticker.payout_ratio = row[12]
        ticker.growth_rate = row[13]
        ticker.earnings_date = row[14]
        ticker.growth_rate_stability = row[15]
        ticker.growth_rate_comb = row[16]
        ticker.pe_valuation = row[17]
        ticker.comb_valuation = row[18]
        ticker.price_discount_1 = row[19]
        ticker.price_discount_2 = row[20]
        ticker.price_discount_3 = row[21]
        ticker.eps_valuation = row[22]
        ticker.pe_discount = row[23]
        ticker.pb_discount = row[24]
        ticker.pfcf_discount = row[25]
        ticker.option_year_discount = row[26]
        ticker.beta = row[27]
        ticker.description = row[28]
        return ticker


        
        

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
