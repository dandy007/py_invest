import mysql
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Tuple
from .row_tickers import ROW_Tickers
from .constants import TICKERS__TYPE_TO_COLUMN__DICT
from mysql.connector.pooling import PooledMySQLConnection

from .db import DB

class DAO_Tickers:

    db_name = 'tickers'
    
    conn = None
    cursor = None

    def __init__(self, conn: PooledMySQLConnection):
        self.conn = conn
        self.cursor = conn.cursor()

    def insert_ticker(self, ticker_id: str, force_commit: bool):
        sql = f"insert into {self.db_name} (ticker_id) VALUES (%s)"
        values = [ticker_id]
        self.cursor.execute(sql, values)
        if force_commit:
            self.conn.commit()
        return

    def update_ticker_types(self, ticker_id: str, types_dict, force_commit: bool):
        sql = f"update {self.db_name} set "
        values = []
        accepted_types: Tuple[type, ...] = (int, float, str, datetime, date, Decimal)
        for type, value in types_dict.items():
                if isinstance(value, accepted_types):
                    type_column = TICKERS__TYPE_TO_COLUMN__DICT.get(type, None)
                    if type_column != None:
                        if len(values) > 0:
                            sql += ','
                        sql += f" {type_column}=%s"
                        values.append(value)
        if len(values) == 0:
            return
        where = f" where ticker_id = %s"
        values.append(ticker_id)
        sql = sql + where
        self.cursor.execute(sql, values)
        if force_commit:
            self.conn.commit()
        return None

    def update_tickers(self, ticker_data_list):
    
        try:
            self.cursor.execute(f"select * from {self.db_name}")
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

    def delete_tickers(self, ticker_id: str):
    
        try:
            self.cursor.execute(f"delete from {self.db_name} where ticker_id = '{ticker_id}'")
            self.conn.commit()
            
            print("Record deleted successfully!")
        except mysql.connector.Error as err:
            print("Error delete data:", err)
    
    def select_tickers_all(self) -> list[ROW_Tickers] :
        try:
            self.cursor.execute(f"select * from {self.db_name}")
            all_db_tickers = self.cursor.fetchall()

            result = []
            for row in all_db_tickers:
                ticker = DAO_Tickers.mapRow2ROW_Tickers(row)
                result.append(ticker)

            return result

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err

    def select_tickers_all_ids(self) -> list[str] :
        try:
            self.cursor.execute(f"select ticker_id from {self.db_name}")
            all_db_tickers = self.cursor.fetchall()

            result = []
            for row in all_db_tickers:
                result.append(row[0].upper())

            return result

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err   

    def select_tickers_all__limited_ids(self) -> list[str] :
        return self.select_tickers_all__limited_usa_ids()
        try:
            self.cursor.execute(f"select ticker_id from {self.db_name} where market_cap > 100000000 and industry is not NULL and LENGTH(industry) > 0 ORDER BY market_cap DESC")
            all_db_tickers = self.cursor.fetchall()

            result = []
            for row in all_db_tickers:
                result.append(row[0].upper())

            return result

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err   
    
    def select_tickers_all__limited_usa_ids(self) -> list[str] :
        try:
            self.cursor.execute(f"select ticker_id from {self.db_name} where market_cap > 1000000000 and industry is not NULL and LENGTH(industry) > 0 and exchange in ('NYSE', 'NASDAQ') ORDER BY market_cap DESC")
            all_db_tickers = self.cursor.fetchall()

            result = []
            for row in all_db_tickers:
                result.append(row[0].upper())

            return result

        except mysql.connector.Error as err:
            print("Error selecting data:", err)    
            raise err   
        
    def select_tickers_where(self, where: str) -> list[ROW_Tickers] :
        try:
            self.cursor.execute(f"select * from {self.db_name} where {where}")
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
            sql = f"select * from {self.db_name} where ticker_id='{ticker_id}' LIMIT 1"
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
        ticker.exchange = row[1]
        ticker.name = row[2]
        ticker.description = row[3]
        ticker.industry = row[4]
        ticker.sector = row[5]
        ticker.isin = row[6]
        ticker.market_cap = row[7]
        ticker.price = row[8]
        ticker.target_price = row[9]
        ticker.pe = row[10]
        ticker.ps = row[11]
        ticker.pb = row[12]
        ticker.pfcf = row[13]
        ticker.recomm_mean = row[14]
        ticker.recomm_count = row[15]
        ticker.div_yield = row[16]
        ticker.payout_ratio = row[17]
        ticker.growth_rate = row[18]
        ticker.earnings_date = row[19]
        ticker.growth_rate_stability = row[20]
        ticker.growth_rate_comb = row[21]
        ticker.price_discount_1 = row[22]
        ticker.price_discount_2 = row[23]
        ticker.price_discount_3 = row[24]
        ticker.eps_valuation = row[25]
        ticker.fcf_valuation = row[26]
        ticker.pe_discount = row[27]
        ticker.ps_discount = row[28]
        ticker.pb_discount = row[29]
        ticker.pfcf_discount = row[30]
        ticker.option_year_discount = row[31]
        ticker.beta = row[32]
        
        if len(row) > 36:
            ticker.predict_rev_cagr = row[36]
        if len(row) > 37:
            ticker.predict_eps_cagr = row[37]
        if len(row) > 48:
            ticker.r_dcf = row[48]
        if len(row) > 49:
            ticker.fundament_change = row[49]
        if len(row) > 50:
            ticker.last_q_yoy_rev_growth = row[50]
        if len(row) > 51:
            ticker.stddev = row[51]
        if len(row) > 52:
            ticker.roe = row[52]
        if len(row) > 53:
            ticker.roa = row[53]
        if len(row) > 54:
            ticker.roic = row[54]
        if len(row) > 55:
            ticker.debt_to_equity = row[55]
        if len(row) > 56:
            ticker.net_debt_ebitda = row[56]
        if len(row) > 57:
            ticker.earnings_quality = row[57]
        if len(row) > 58:
            ticker.cash_conversion = row[58]
        if len(row) > 59:
            ticker.earnings_volatility = row[59]
        if len(row) > 60:
            ticker.ev_ebitda = row[60]
        if len(row) > 61:
            ticker.peg = row[61]
        if len(row) > 62:
            ticker.week_52_position = row[62]
        if len(row) > 63:
            ticker.volume_trend = row[63]
        if len(row) > 64:
            ticker.sentiment = row[64]
        if len(row) > 65:
            ticker.sentiment_date = row[65]
        
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
