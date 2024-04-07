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