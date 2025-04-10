export type ChartData = [string[], number[]]; // [date_list, value_list]

export interface StockData {
  TICKER: string;
  TICKER_NAME: string;
  TICKER_DESCRIPTION: string;

  // Price chart
  PRICE_DATA: ChartData;
  TARGET_PRICE_DATA: ChartData;
  OPTION_PRICE_DATA: ChartData;
  EPS_PRICE_DATA: ChartData;
  FCF_PRICE_DATA: ChartData;

  // Margins chart
  GROSS_MARGIN: ChartData;
  OPER_MARGIN: ChartData;
  NET_MARGIN: ChartData;

  // Valuation charts
  PE: ChartData;
  PS: ChartData;
  PB: ChartData;
  PFCF: ChartData;

  // Income statement chart
  REVENUE: ChartData;
  GROSS_PROFIT: ChartData;
  EBITDA: ChartData;
  NET_INCOME: ChartData;

  // Balance sheet chart
  ASSETS: ChartData;
  LIABILITIES: ChartData;
  EQUITY: ChartData;
  LONG_TERM_DEBT: ChartData;
  CASH: ChartData;

  // FCF Statement chart
  OPER_FCF: ChartData;
  FCF: ChartData;

  // Shares outstanding chart
  SHARES_OUTSTANDING: ChartData;
}

// Typ pro Plotly trace object (může být rozšířen podle potřeby)
export interface PlotlyTrace {
  x: (string | number)[] | [number, number];
  y: (number | null)[];
  type: string;
  mode?: string;
  name?: string;
  marker?: {
    color?: string;
    line?: {
      color?: string;
      width?: number;
    }
  };
  line?: {
    color?: string;
    width?: number;
  };
  yaxis?: string; // Pro případné více os y
  // ... další Plotly atributy
}