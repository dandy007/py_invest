from data_providers.alpha_vantage import get_tickers_download

import os
import pyodbc
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the environment variables
db_server = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

connection_string = f'Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{db_server},{db_port};Database={db_name};UID={db_user};PWD={db_password};Encrypt=no;TrustServerCertificate=no'

# Connect to the MSSQL database
conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};Server=tcp:dandy-stocks.database.windows.net,1433;Database=dandy-stocks;Uid=dandY165187496;Pwd=fHSdf%s64a215;Encrypt=yes;TrustServerCertificate=no;')

# Create a cursor object
cursor = conn.cursor()



if __name__ == "__main__":
    #tickerList = {"AMZN", "MSFT"} #get_tickers_download()
    tickerList = get_tickers_download()

    # Execute an INSERT statement
    insert_query = "INSERT INTO tickers (tickerId) VALUES (?)"

    for row in tickerList:
        print(f"Ticker: {row}")
        values = (row)
        cursor.execute(insert_query, values)  
        conn.commit()

    # Commit the changes
    

    # Close the connection
    conn.close()