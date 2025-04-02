from stocks.imports.import_scheduler import start_import_schedulers
from stocks.api.api import fastApiApp
import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
import uvicorn

# Load environment variables from .env file
load_dotenv()

# Create a custom logger
logger = logging.getLogger('invest_logger')
logger.setLevel(logging.DEBUG)  # Set minimum level of logging

# Create handlers
rotating_file_handler = RotatingFileHandler(
    'invest.log', maxBytes=10*1024*1024, backupCount=50)  # Log file that rolls over at 10MB
console_handler = logging.StreamHandler()  # Console handler

# Set level for each handler
rotating_file_handler.setLevel(logging.DEBUG)
console_handler.setLevel(logging.DEBUG)

# Create formatters and add it to handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotating_file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(rotating_file_handler)
logger.addHandler(console_handler)

logging.getLogger('yfinance').setLevel(logging.CRITICAL + 1)  # This effectively disables logging for this logger
logging.getLogger('urllib3.connectionpool').setLevel(logging.CRITICAL + 1)  # This effectively disables logging for this logger


if __name__ == "__main__":

    DEV_MODE = os.getenv('DEV_MODE').lower() == "true"

    start_import_schedulers() # download all sort of data and store them to db

    if DEV_MODE == False:
        pass
    
    if DEV_MODE == True:
        pass

    uvicorn.run(fastApiApp, host="0.0.0.0", port=5000)
