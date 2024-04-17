"""
- market_cap - 0
- enterprise_value - 1
- shares_outstanding - 2
- current_price - 3
- target_price - 4
- eps - 5
- book_per_share - 6
- pe - 7
- pb - 8
- beta - 9
- volume - 10
- avg volume - 11
- ev/ebitda - 12
- recomm_mean - 13
- recomm_avg - 14
- div_yield - 15
- payout_ratio - 16
- next 5y growth - 17
- ROA - 18
- ROE - 19
- peg - 20
- ps - 21
"""

class TICKERS_TIME_DATA__TYPE__CONST:
    MARKET_CAP = 0
    ENTERPRISE_VALUE = 1
    SHARES_OUTSTANDING = 2
    PRICE = 3
    TARGET_PRICE = 4
    EPS = 5
    BOOK_PER_SHARE = 6
    PE = 7
    PB = 8
    BETA = 9
    VOLUME = 10
    VOLUME_AVG = 11
    EV_EBITDA = 12
    RECOMM_MEAN = 13
    RECOMM_AVG = 14
    DIV_YIELD = 15
    PAYOUT_RATIO = 16
    GROWTH_5Y = 17
    ROA = 18
    ROE = 19
    PEG = 20
    PS = 21

TICKERS__TYPE_TO_COLUMN__DICT = {
    TICKERS_TIME_DATA__TYPE__CONST.MARKET_CAP : "market_cap",
    TICKERS_TIME_DATA__TYPE__CONST.PRICE : "price",
    TICKERS_TIME_DATA__TYPE__CONST.TARGET_PRICE : "target_price",
    TICKERS_TIME_DATA__TYPE__CONST.PE : "pe",
    TICKERS_TIME_DATA__TYPE__CONST.RECOMM_MEAN : "recomm_mean",
    TICKERS_TIME_DATA__TYPE__CONST.RECOMM_AVG : "recomm_avg",
    TICKERS_TIME_DATA__TYPE__CONST.DIV_YIELD : "div_yield",
    TICKERS_TIME_DATA__TYPE__CONST.PAYOUT_RATIO : "payout_ratio",
    TICKERS_TIME_DATA__TYPE__CONST.GROWTH_5Y : "growth_rate_5y"
}
