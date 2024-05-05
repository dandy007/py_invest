from typing import List

class POLY_CONSTANTS:

    TIMEFRAME_QUATERLY = "quarterly"
    TIMEFRAME_ANNUAL = "annual"
    FISCAL_PERIOD__Q1 = "Q1"
    FISCAL_PERIOD__Q2 = "Q2"
    FISCAL_PERIOD__Q3 = "Q3"
    FISCAL_PERIOD__Q4 = "Q4"
    FISCAL_PERIOD__FISCAL_YEAR = "FY"

    FIELD__VALUE = 'value'
    CASH_FLOW__NET_CASH_FINANCING = 'net_cash_flow_from_financing_activities'
    CASH_FLOW__NET_CASH_FLOW = 'net_cash_flow'
    CASH_FLOW__NET_CASH_OPERATING = 'net_cash_flow_from_operating_activities'
    CASH_FLOW__NET_CASH_INVESTING = 'net_cash_flow_from_investing_activities'

    BALANCE_SHEET__ = ''
    BALANCE_SHEET__ = 'current_assets'
    BALANCE_SHEET__ = 'liabilities'
    BALANCE_SHEET__ = 'current_liabilities'
    BALANCE_SHEET__ = 'equity'

class CashFlow:
    def __init__(self):
        self.net_cash_flow = None
        self.net_cash_flow_financing = None
        self.net_cash_flow_operating = None
        self.net_cash_flow_investing = None

class BalanceSheet:
    def __init__(self):
        self.liabilities = None
        self.current_assets = None
        self.assets = None
        self.equity = None
        self.current_liabilities = None

class FinancialStatement:
    def __init__(self, financial_items):
        self.income_statement = None
        self.balance_sheet = None
        self.cash_flow_statement = None
        for key, items in financial_items['financials'].items():


            if key == 'income_statement':
                self.income_statement = items
            elif key == 'balance_sheet':
                self.balance_sheet = BalanceSheet()


            elif key == 'cash_flow_statement':
                self.cash_flow_statement = CashFlow()
                self.cash_flow_statement.net_cash_flow = items[POLY_CONSTANTS.CASH_FLOW__NET_CASH_FLOW][POLY_CONSTANTS.FIELD__VALUE]
                self.cash_flow_statement.net_cash_flow_investing = items[POLY_CONSTANTS.CASH_FLOW__NET_CASH_FINANCING][POLY_CONSTANTS.FIELD__VALUE]
                self.cash_flow_statement.net_cash_flow_operating = items[POLY_CONSTANTS.CASH_FLOW__NET_CASH_OPERATING][POLY_CONSTANTS.FIELD__VALUE]
                self.cash_flow_statement.net_cash_flow_investing = items[POLY_CONSTANTS.CASH_FLOW__NET_CASH_INVESTING][POLY_CONSTANTS.FIELD__VALUE]
            else :
                continue

class CompanyReport:
    def __init__(self, id, start_date, end_date, timeframe, fiscal_period, fiscal_year, cik, sic, tickers, company_name, **financials):
        self.id = id
        self.start_date = start_date
        self.end_date = end_date
        self.timeframe = timeframe
        self.fiscal_period = fiscal_period
        self.fiscal_year = fiscal_year
        self.cik = cik
        self.sic = sic
        self.tickers = tickers
        self.company_name = company_name
        self.financials = FinancialStatement(financials)

class FinancialReport:
    def __init__(self, results: List[dict]):
        self.results = [CompanyReport(**result) for result in results]