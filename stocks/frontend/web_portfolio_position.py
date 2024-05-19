from stocks.db.row_portfolio_positions import ROW_PortfolioPositions
from stocks.db.row_tickers import ROW_Tickers

class ROW_WebPortfolioPosition:

    db_ticker : ROW_Tickers = None
    db_position : ROW_PortfolioPositions = None

    ticker_id = None

    def __init__(self, db_ticker: ROW_Tickers, db_position: ROW_PortfolioPositions):
        self.db_ticker = db_ticker
        self.db_position = db_position
