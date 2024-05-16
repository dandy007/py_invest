import mysql
import math
from .row_portfolio_positions import ROW_PortfolioPositions
from mysql.connector.pooling import PooledMySQLConnection

from .db import DB

class DAO_PortfolioPositions:
    
    conn = None
    cursor = None

    def __init__(self, conn: PooledMySQLConnection):
        self.conn = conn
        self.cursor = conn.cursor()
    
    def insert_portfolio_position(self, portfolio_position_row: ROW_PortfolioPositions):
        sql = f"insert into portfolio_positions (portfolio_id, ticker_id) VALUES (%s, %s)"
        values = [portfolio_position_row.portfolio_id, portfolio_position_row.ticker_id]
        self.cursor.execute(sql, values)
        self.conn.commit()
        return

    """ def update_portfolio_position(self, portfolio_position_row: ROW_PortfolioPositions):
        sql = f"update portfolio_positions set name=%s where portfolio_id=%s"
        values = [portfolio_row.name[0:99], portfolio_row.portfolio_id]
        self.cursor.execute(sql, values)
        self.conn.commit()
        return """
    
    def delete_portfolio_position(self, portfolio_id: int, ticker_id: str):
        sql = f"delete from portfolio_positions where portfolio_id=%s and ticker_id=%s"
        values = [portfolio_id, ticker_id]
        self.cursor.execute(sql, values)
        self.conn.commit()
        return
    
    def select_all_portfolio_positions(self, portfolio_id: int):
        sql = f"select * from portfolio_positions where portfolio_id=%s"
        values = [portfolio_id]
        self.cursor.execute(sql, values)

        all_portfolios = self.cursor.fetchall()

        result = []
        for p_data in all_portfolios:
            p = ROW_PortfolioPositions()
            p.portfolio_id = p_data[0]
            p.ticker_id = p_data[1]
            result.append(p)

        return result

if __name__ == "__main__":
        conn = DB.get_connection_mysql()
        o = DAO_Portfolios(conn)
        result = o.select_ticker("AAPL")
        conn.commit()
        conn.close()
