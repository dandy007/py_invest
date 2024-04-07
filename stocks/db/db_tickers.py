import db

import mysql

def execute(cursor, sql):
    try:
        cursor.execute(sql)
    except mysql.connector.Error as err:
        print("Error executing sql:", err)
    return

def insert_ticker(cursor, ticker):
    sql = f"insert into tickers (ticker_id, name) VALUES ('{ticker[0]}','{ticker[1][0:99]}')"
    execute(cursor,sql)
    return

def update_ticker(cursor, ticker):
    sql = f"update tickers set ticker_id='{ticker[0]}', name='{ticker[1][0:99]}' where ticker_id='{ticker[0]}'"
    execute(cursor,sql)
    return     

def update_tickers(cursor, ticker_data_list):
  
    try:
        cursor.execute("select * from tickers")
        all_db_tickers = cursor.fetchall()

        ticker_db_dict = {}

        for ticker in all_db_tickers:
            ticker_db_dict[ticker[0]] = ticker

        for ticker in ticker_data_list:
            value = ticker_db_dict.get(ticker[0])
            if value is not None:
                 update_ticker(cursor, ticker)
            else:
                 insert_ticker(cursor, ticker)
        
        print("Record inserted successfully!")
    except mysql.connector.Error as err:
        print("Error inserting data:", err)

                

if __name__ == "__main__":
        conn = db.get_connection()
        cursor = conn.cursor()
        update_tickers(cursor, [["AMZN", "Amazon"],["AAPL", "Apple"]])
        cursor.close()
        conn.commit()
        conn.close()
