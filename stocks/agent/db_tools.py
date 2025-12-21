"""
Database Tools for the DB Analysis Agent.
Provides tool definitions and execution functions for querying the database.
"""
import json
from typing import Any, Optional
from stocks.db.db import DB
from stocks.db.dao_tickers import DAO_Tickers
from stocks.db.dao_tickers_data import DAO_TickersData
from stocks.db.constants import TICKERS_TIME_DATA__TYPE__CONST

# Tool definitions for OpenRouter function calling
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_tickers",
            "description": "Query the tickers table to get stock information. Returns data like ticker_id, name, price, PE, growth_rate, market_cap, etc.",
            "parameters": {
                "type": "object",
                "properties": {
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Columns to select. Available: ticker_id, name, exchange, industry, sector, market_cap, price, target_price, pe, ps, pb, pfcf, div_yield, growth_rate, beta, roe, roa, roic, debt_to_equity, ev_ebitda, peg, r_dcf, stddev"
                    },
                    "where": {
                        "type": "string",
                        "description": "SQL WHERE clause without 'WHERE' keyword. Example: 'pe < 15 AND growth_rate > 0.1'"
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Column to order by with optional DESC/ASC. Example: 'market_cap DESC'"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return",
                        "default": 20
                    }
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_ticker_details",
            "description": "Get detailed information about a specific ticker/stock including all available metrics",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker_id": {
                        "type": "string",
                        "description": "The ticker symbol, e.g., 'AAPL', 'MSFT', 'NVDA'"
                    }
                },
                "required": ["ticker_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_time_series",
            "description": "Query historical time series data for a ticker. Use this to analyze trends over time like revenue, earnings, cash flow, margins etc.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker_id": {
                        "type": "string",
                        "description": "The ticker symbol"
                    },
                    "data_type": {
                        "type": "string",
                        "description": "Type of data to query",
                        "enum": ["revenue", "revenue_q", "net_income", "net_income_q", "fcf", "fcf_q", 
                                 "eps", "eps_q", "price", "pe", "ps", "pb", "ebitda", "ebitda_q",
                                 "gross_profit_margin_q", "net_income_margin_q", "fcf_margin_q"]
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of historical data points to return",
                        "default": 10
                    }
                },
                "required": ["ticker_id", "data_type"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_tickers",
            "description": "Search for tickers by name, industry, or sector",
            "parameters": {
                "type": "object",
                "properties": {
                    "search_term": {
                        "type": "string",
                        "description": "Term to search for in ticker name, industry, or sector"
                    },
                    "field": {
                        "type": "string",
                        "description": "Field to search in",
                        "enum": ["name", "industry", "sector", "all"],
                        "default": "all"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results",
                        "default": 20
                    }
                },
                "required": ["search_term"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "compare_tickers",
            "description": "Compare multiple tickers side by side on key metrics",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of ticker symbols to compare"
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Metrics to compare. Available: price, pe, ps, pb, growth_rate, market_cap, div_yield, roe, roic, debt_to_equity"
                    }
                },
                "required": ["ticker_ids"]
            }
        }
    }
]

# Mapping from data_type string to constant
DATA_TYPE_MAP = {
    "revenue": TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE,
    "revenue_q": TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q,
    "net_income": TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME,
    "net_income_q": TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_Q,
    "fcf": TICKERS_TIME_DATA__TYPE__CONST.FCF,
    "fcf_q": TICKERS_TIME_DATA__TYPE__CONST.FCF_Q,
    "eps": TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS,
    "eps_q": TICKERS_TIME_DATA__TYPE__CONST.BASIC_EPS_Q,
    "price": TICKERS_TIME_DATA__TYPE__CONST.PRICE,
    "pe": TICKERS_TIME_DATA__TYPE__CONST.PE,
    "ps": TICKERS_TIME_DATA__TYPE__CONST.PS,
    "pb": TICKERS_TIME_DATA__TYPE__CONST.PB,
    "ebitda": TICKERS_TIME_DATA__TYPE__CONST.EBITDA,
    "ebitda_q": TICKERS_TIME_DATA__TYPE__CONST.EBITDA_Q,
    "gross_profit_margin_q": TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_MARGIN_Q,
    "net_income_margin_q": TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_MARGIN_Q,
    "fcf_margin_q": TICKERS_TIME_DATA__TYPE__CONST.FCF_MARGIN_Q,
}


class DBTools:
    """Executor for database tools."""
    
    def __init__(self):
        self.conn = None
        self.dao_tickers = None
        self.dao_data = None
    
    def _ensure_connection(self):
        """Ensure database connection is established."""
        if self.conn is None:
            self.conn = DB.get_connection_mysql()
            self.dao_tickers = DAO_Tickers(self.conn)
            self.dao_data = DAO_TickersData(self.conn)
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def execute_tool(self, tool_name: str, arguments: dict) -> str:
        """Execute a tool by name with given arguments."""
        self._ensure_connection()
        
        try:
            if tool_name == "query_tickers":
                return self._query_tickers(**arguments)
            elif tool_name == "get_ticker_details":
                return self._get_ticker_details(**arguments)
            elif tool_name == "query_time_series":
                return self._query_time_series(**arguments)
            elif tool_name == "search_tickers":
                return self._search_tickers(**arguments)
            elif tool_name == "compare_tickers":
                return self._compare_tickers(**arguments)
            else:
                return json.dumps({"error": f"Unknown tool: {tool_name}"})
        except Exception as e:
            return json.dumps({"error": str(e)})
    
    def _query_tickers(self, columns: list = None, where: str = None, 
                       order_by: str = None, limit: int = 20) -> str:
        """Query tickers table."""
        # Build column list
        if columns:
            col_str = ", ".join(columns)
        else:
            col_str = "ticker_id, name, price, pe, growth_rate, market_cap"
        
        # Build query
        sql = f"SELECT {col_str} FROM tickers"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        sql += f" LIMIT {limit}"
        
        self.dao_tickers.cursor.execute(sql)
        rows = self.dao_tickers.cursor.fetchall()
        
        # Get column names
        col_names = columns if columns else ["ticker_id", "name", "price", "pe", "growth_rate", "market_cap"]
        
        results = []
        for row in rows:
            result = {}
            for i, col in enumerate(col_names):
                val = row[i]
                # Handle decimal/float formatting
                if isinstance(val, (int, float)) and val is not None:
                    result[col] = round(float(val), 4) if isinstance(val, float) else val
                else:
                    result[col] = val
            results.append(result)
        
        return json.dumps({"count": len(results), "data": results}, ensure_ascii=False)
    
    def _get_ticker_details(self, ticker_id: str) -> str:
        """Get detailed ticker information."""
        ticker = self.dao_tickers.select_ticker(ticker_id.upper())
        
        if ticker is None:
            return json.dumps({"error": f"Ticker '{ticker_id}' not found"})
        
        # Convert to dict
        result = {
            "ticker_id": ticker.ticker_id,
            "name": ticker.name,
            "exchange": ticker.exchange,
            "industry": ticker.industry,
            "sector": ticker.sector,
            "market_cap": ticker.market_cap,
            "price": ticker.price,
            "target_price": ticker.target_price,
            "pe": ticker.pe,
            "ps": ticker.ps,
            "pb": ticker.pb,
            "pfcf": ticker.pfcf,
            "div_yield": ticker.div_yield,
            "growth_rate": ticker.growth_rate,
            "beta": ticker.beta,
            "roe": ticker.roe,
            "roa": ticker.roa,
            "roic": ticker.roic,
            "debt_to_equity": ticker.debt_to_equity,
            "ev_ebitda": ticker.ev_ebitda,
            "peg": ticker.peg,
            "r_dcf": ticker.r_dcf,
            "stddev": ticker.stddev,
            "predict_rev_cagr": ticker.predict_rev_cagr,
            "predict_eps_cagr": ticker.predict_eps_cagr,
        }
        
        # Round floats
        for key, val in result.items():
            if isinstance(val, float) and val is not None:
                result[key] = round(val, 4)
        
        return json.dumps(result, ensure_ascii=False)
    
    def _query_time_series(self, ticker_id: str, data_type: str, limit: int = 10) -> str:
        """Query time series data."""
        type_const = DATA_TYPE_MAP.get(data_type)
        if type_const is None:
            return json.dumps({"error": f"Unknown data type: {data_type}"})
        
        data = self.dao_data.select_ticker_data(ticker_id.upper(), type_const, limit)
        
        results = []
        for row in data:
            results.append({
                "date": row.date.isoformat() if row.date else None,
                "value": round(float(row.value), 4) if row.value else None
            })
        
        return json.dumps({
            "ticker_id": ticker_id.upper(),
            "data_type": data_type,
            "count": len(results),
            "data": results
        }, ensure_ascii=False)
    
    def _search_tickers(self, search_term: str, field: str = "all", limit: int = 20) -> str:
        """Search for tickers."""
        search_term = search_term.replace("'", "''")  # Escape quotes
        
        if field == "name":
            where = f"name LIKE '%{search_term}%'"
        elif field == "industry":
            where = f"industry LIKE '%{search_term}%'"
        elif field == "sector":
            where = f"sector LIKE '%{search_term}%'"
        else:
            where = f"(name LIKE '%{search_term}%' OR industry LIKE '%{search_term}%' OR sector LIKE '%{search_term}%' OR ticker_id LIKE '%{search_term}%')"
        
        sql = f"SELECT ticker_id, name, industry, sector FROM tickers WHERE {where} LIMIT {limit}"
        self.dao_tickers.cursor.execute(sql)
        rows = self.dao_tickers.cursor.fetchall()
        
        results = []
        for row in rows:
            results.append({
                "ticker_id": row[0],
                "name": row[1],
                "industry": row[2],
                "sector": row[3]
            })
        
        return json.dumps({"count": len(results), "data": results}, ensure_ascii=False)
    
    def _compare_tickers(self, ticker_ids: list, metrics: list = None) -> str:
        """Compare multiple tickers."""
        if metrics is None:
            metrics = ["price", "pe", "ps", "growth_rate", "market_cap", "roe"]
        
        results = []
        for tid in ticker_ids:
            ticker = self.dao_tickers.select_ticker(tid.upper())
            if ticker:
                result = {"ticker_id": tid.upper()}
                for metric in metrics:
                    val = getattr(ticker, metric, None)
                    if isinstance(val, float) and val is not None:
                        result[metric] = round(val, 4)
                    else:
                        result[metric] = val
                results.append(result)
        
        return json.dumps({"metrics": metrics, "data": results}, ensure_ascii=False)
