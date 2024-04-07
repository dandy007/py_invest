import unittest
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..\\..')))

from unittest.mock import patch, mock_open
from stocks_1_0.data_providers.alpha_vantage import get_tickers, get_tickers_reader

class TestTickerRetrieval(unittest.TestCase):
    @patch('requests.get')
    def test_get_tickers_reader_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.headers = {'Content-Type': 'application/x-download'}
        mock_get.return_value.content = b'Ticker,Name\nAAPL,Apple Inc.\nMSFT,Microsoft Corporation\n'

        reader = get_tickers_reader()
        self.assertIsNotNone(reader)
        self.assertEqual(['AAPL', 'MSFT'], get_tickers(reader))

    @patch('requests.get')
    def test_get_tickers_reader_non_downloadable(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.headers = {'Content-Type': 'text/html'}
        mock_get.return_value.content = b'<html><body>This is not a CSV file.</body></html>'

        with self.assertRaises(Exception) as context:
            get_tickers_reader()
        self.assertEqual("The response is not downloadable file.", str(context.exception))

    @patch('requests.get')
    def test_get_tickers_reader_non_200_status_code(self, mock_get):
        mock_get.return_value.status_code = 404
        mock_get.return_value.content = b'Not Found'

        with self.assertRaises(Exception) as context:
            get_tickers_reader()
        self.assertEqual("Status code <> 200", str(context.exception))

    def test_get_tickers_success(self):
        ticker_list = [[],['AAPL'],['MSFT'],['AMZN']]
        iterator = iter(ticker_list)

        result_list = get_tickers(iterator)
        self.assertIsNotNone(result_list)
        self.assertEqual(['AAPL', 'MSFT', 'AMZN'], result_list)

if __name__ == '__main__':
    unittest.main()