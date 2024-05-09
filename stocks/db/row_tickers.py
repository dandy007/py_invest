class ROW_Tickers:

    ticker_id = None
    name = None
    description = None
    industry = None
    sector = None
    isin = None
    market_cap = None
    price = None
    target_price = None
    pe = None
    recomm_mean = None
    recomm_count = None
    div_yield = None
    payout_ratio = None
    growth_rate = None
    earnings_date = None
    growth_rate_stability = None
    growth_rate_comb = None
    pe_valuation = None
    comb_valuation = None
    price_discount_1 = None
    price_discount_2 = None
    price_discount_3 = None


    def __init__(self, ticker_id, name, industry, sector, isin):
        self.ticker_id = ticker_id
        self.name = name
        self.industry = industry
        self.sector = sector
        self.isin = isin