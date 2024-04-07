import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the environment variables
db_server = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

def get_connection():
        return mysql.connector.connect(
        host=db_server,
        user=db_user,
        password=db_password,
        database=db_name
        )

if __name__ == "__main__":
        get_connection.close()