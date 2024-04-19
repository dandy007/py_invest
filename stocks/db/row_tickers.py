class ROW_Tickers:

    ticker_id = None
    name = None
    industry = None
    sector = None
    isin = None
    market_cap = None
    price = None
    target_price = None
    pe = None
    recomm_mean = None
    recomm_avg = None
    div_yield = None
    payout_ratio = None
    growth_rate_5y = None

    def __init__(self, ticker_id, name, industry, sector, isin):
        self.ticker_id = ticker_id
        self.name = name
        self.industry = industry
        self.sector = sector
        self.isin = isin