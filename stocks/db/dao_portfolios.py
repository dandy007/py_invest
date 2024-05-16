import mysql
import math
from .row_portfolios import ROW_Portfolios
from mysql.connector.pooling import PooledMySQLConnection

from .db import DB

class DAO_Portfolios:
    
    conn = None
    cursor = None

    def __init__(self, conn: PooledMySQLConnection):
        self.conn = conn
        self.cursor = conn.cursor()
    
    def insert_portfolio(self, portfolio_row: ROW_Portfolios):
        sql = f"insert into portfolios (name) VALUES (%s)"
        values = [portfolio_row.name[0:99]]
        self.cursor.execute(sql, values)
        self.conn.commit()
        return

    def update_portfolio(self, portfolio_row: ROW_Portfolios):
        sql = f"update portfolios set name=%s where portfolio_id=%s"
        values = [portfolio_row.name[0:99], portfolio_row.portfolio_id]
        self.cursor.execute(sql, values)
        self.conn.commit()
        return
    
    def delete_portfolio(self, portfolio_id: int):
        sql = f"delete from portfolios where portfolio_id=%s"
        values = [portfolio_id]
        self.cursor.execute(sql, values)
        self.conn.commit()
        return
    
    def select_portfolio(self, portfolio_id: int):
        sql = f"select * from portfolios where portfolio_id=%s"
        values = [portfolio_id]
        self.cursor.execute(sql, values)

        portfolio = self.cursor.fetchone()

        if len(portfolio) > 0:
            p = ROW_Portfolios(portfolio[1])
            p.portfolio_id = portfolio[0]
            return p

        return None

    def select_all_portfolios(self):
        sql = f"select * from portfolios"
        values = []
        self.cursor.execute(sql, values)

        all_portfolios = self.cursor.fetchall()

        result = []
        for p_data in all_portfolios:
            p = ROW_Portfolios(p_data[1])
            p.portfolio_id = p_data[0]
            result.append(p)

        return result

if __name__ == "__main__":
        conn = DB.get_connection_mysql()
        o = DAO_Portfolios(conn)
        result = o.select_ticker("AAPL")
        conn.commit()
        conn.close()
