import mysql
import math
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

    def update_ticker(self, ticker_row: ROW_Tickers, force_commit: bool):
        sql = f"update {self.db_name} set ticker_id='{ticker_row.ticker_id}', name='{ticker_row.name[0:99]}', industry='{ticker_row.industry[0:99]}', sector='{ticker_row.sector[0:99]}', isin='{ticker_row.isin[0:99]}' where ticker_id='{ticker_row.ticker_id}'"
        values = (ticker_row.ticker_id)
        self.cursor.execute(sql, values)
        if force_commit:
            self.conn.commit()
        return    

    def update_ticker_base_data(self, ticker_row: ROW_Tickers, force_commit: bool):
        sql = f"update {self.db_name} set name=%s, industry=%s, sector=%s, isin=%s, earnings_date=%s, description=%s where ticker_id = %s"
        values = (ticker_row.name, ticker_row.industry, ticker_row.sector, ticker_row.isin, ticker_row.earnings_date, ticker_row.description, ticker_row.ticker_id)
        self.cursor.execute(sql, values)
        if force_commit:
            self.conn.commit()
        return    

    def update_ticker_types(self, ticker_row: ROW_Tickers, types_dict, force_commit: bool):
        sql = f"update {self.db_name} set "
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
        ticker.pe_valuation = row[22]
        ticker.comb_valuation = row[23]
        ticker.price_discount_1 = row[24]
        ticker.price_discount_2 = row[25]
        ticker.price_discount_3 = row[26]
        ticker.eps_valuation = row[27]
        ticker.fcf_valuation = row[28]
        ticker.pe_discount = row[29]
        ticker.ps_discount = row[30]
        ticker.pb_discount = row[31]
        ticker.pfcf_discount = row[32]
        ticker.option_year_discount = row[33]
        ticker.beta = row[34]
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
