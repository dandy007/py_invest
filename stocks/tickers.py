import yfinance
import requests
import json

from bf4py import BF4Py
from datetime import datetime, timedelta

bf4py = BF4Py(default_isin='DE0005190003') # Default BMW