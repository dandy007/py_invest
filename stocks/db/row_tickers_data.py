from datetime import date

class ROW_TickersData:

    def __init__(self, payout_ratio, date: date, ticker_id, market_cap, current_price, target_price, eps, shares_outstanding, recommendation_mean, recommendation_strong_buy, recommendation_buy, recommendation_hold, recommendation_sell, recommendation_strong_sell, dividend_yield):
        self.ticker_id = ticker_id
        self.date = date
        self.payout_ratio = payout_ratio
        self.market_cap = market_cap
        self.current_price = current_price
        self.target_price = target_price
        self.eps = eps
        self.shares_outstanding = shares_outstanding
        self.recommendation_mean = recommendation_mean
        self.recommendation_strong_buy = recommendation_strong_buy
        self.recommendation_buy = recommendation_buy
        self.recommendation_hold = recommendation_hold
        self.recommendation_sell = recommendation_sell
        self.recommendation_strong_sell = recommendation_strong_sell
        self.dividend_yield = dividend_yield

