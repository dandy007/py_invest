class ROW_TickerAnalyze:

    symbol = None
    points = 0
    cot_longterm = 0
    cot_shortterm = 0
    seasonality = 0
    vwma_prob = [0,0] # vwma, ... 
    engulf_5m = 0
    engulf_1h = 0
    engulf_4h = 0
    engulf_day = 0
    engulf_week = 0
    engulf_month = 0
    total = 0

    def __init__(self) -> None:
        vwma_prob = [0,0]
        pass