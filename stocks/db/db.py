import os
import mysql.connector
from mysql.connector.pooling import PooledMySQLConnection

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

class DB:
    staticmethod
    def get_connection_mssql():
        return pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};Server=tcp:dandy-stocks.database.windows.net,1433;Database=dandy-stocks;Uid=dandY165187496;Pwd=fHSdf%s64a215;Encrypt=yes;TrustServerCertificate=no;')
    
    staticmethod
    def get_connection_mysql() -> PooledMySQLConnection:
        return mysql.connector.connect(
                host=db_server,
                user=db_user,
                password=db_password,
                database=db_name
                )
    
if __name__ == "__main__":
     DB.get_connection_mysql()