from bokeh.plotting import figure
from bokeh.io import output_file, show
from bokeh.embed import file_html
from bokeh.resources import CDN
from concurrent.futures import ThreadPoolExecutor
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant

from stocks.frontend import ROW_WebPortfolioPosition
import plotly.graph_objects as go


#from flask import Flask,render_template, render_template_string, request, redirect, url_for

#app = Flask(__name__, template_folder='frontend/html', static_folder='frontend/html/static')

def prepare_chart_data_EXTEND(ticker_data_list: list[ROW_TickersData], length: int):
    if len(ticker_data_list) == 0:
        return prepare_chart_data(ticker_data_list)
    
    len_input = len(ticker_data_list)
    for i in range(1,length - len_input):
        record = ROW_TickersData()
        record.value = ticker_data_list[-1].value
        record.date = ticker_data_list[-1].date - timedelta(days=1)
        ticker_data_list.append(record)
    return prepare_chart_data(ticker_data_list) 

def prepare_chart_data(ticker_data_list: list[ROW_TickersData]):
    list_x = []
    list_y = []

    ticker_data_list.sort(key=lambda x: (x.date), reverse=False)

    for ticker_data in ticker_data_list:
        list_x.append(ticker_data.date)
        list_y.append(ticker_data.value)

    return [list_x, list_y]

def prepare_chart_data_TTM(ticker_data_list: list[ROW_TickersData]):
    list_x = []
    list_y = []

    ticker_data_list.sort(key=lambda x: (x.date), reverse=False)

    counter = -1
    for ticker_data in ticker_data_list:
        counter += 1
        if counter < 3:
            continue
        list_x.append(ticker_data.date)
        list_y.append(ticker_data_list[counter].value + ticker_data_list[counter-1].value + ticker_data_list[counter-2].value + ticker_data_list[counter-3].value)

    return [list_x, list_y]

def getChart(x_data, y_data_lists, line_title_list, chart_title):
    fig = go.Figure()

    counter = 0
    for y_data in y_data_lists:
        fig.add_trace(go.Scatter(x=x_data, y=y_data, mode='lines', name=line_title_list[counter]))
        counter += 1

    fig.update_layout(title=chart_title)
    fig_html = fig.to_html(full_html=False)
    return fig_html

def get_safe_value(value):
    try:
        return f'{value[1][-1]:.2f}'
    except:
        return 'NaN'

def get_safe_growth_rate(quarter_growth_data):
    try:
        return f'{predict_growth_rate(None, quarter_growth_data[1])[0]*4*100:.2f}'
    except Exception as e:
        return 'NaN'


@app.route('/api/query', methods=['POST'])
def execute_query():
    # Expect a JSON payload with a "query" field
    req_data = request.get_json()
    sql = req_data.get("query", "").strip()
    if not sql:
        return {"error": "Query parameter missing."}, 400

    try:
        connection = DB.get_connection_mysql()
        cursor = connection.cursor(dictionary=True)
        cursor.execute(sql)
        # Fetch all rows as a list of dictionaries
        rows = cursor.fetchall()
        connection.commit()
        return {"data": rows}
    except Exception as e:
        logger.error("execute_query error: " + str(e))
        return {"error": str(e)}, 500

@app.route('/ticker_reset/<ticker_id>', methods=['GET'])
def ticker_reset(ticker_id: str):
    resetAfterSplit([ticker_id])

@app.route('/ticker', methods=['POST'])
def ticker():
    ticker_ids = request.form['ticker_id']
    return ticker_id(ticker_ids)

@app.route('/ticker/<ticker_id>', methods=['GET'])
def ticker_id(ticker_id: str):

    connection = DB.get_connection_mysql()
    dao_portfolios = DAO_Portfolios(connection)
    dao_portfolio_positions = DAO_PortfolioPositions(connection)
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)

    annual = 5
    days_back = annual * 250
    ticker = dao_tickers.select_ticker(ticker_id)
    eps_discount_row = ROW_TickersData()
    eps_discount_row.date = datetime.today()
    eps_discount_row.value = ticker.eps_valuation

    fcf_discount_row = ROW_TickersData()
    fcf_discount_row.date = datetime.today()
    fcf_discount_row.value = ticker.fcf_valuation

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.PRICE, days_back)
    prepared_chart_data__price = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.DB_TICKERS__TARGET_PRICE, days_back)
    prepared_chart_data__target_price = prepare_chart_data_EXTEND(data_list, days_back)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_MONTH_AVG_PRICE, days_back)
    prepared_chart_data__option_month_price = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPTION_YEAR_AVG_PRICE, days_back)
    prepared_chart_data__option_year_price = prepare_chart_data_EXTEND(data_list, days_back)
    prepared_chart_data__eps_valuation = prepare_chart_data_EXTEND([eps_discount_row], days_back)
    prepared_chart_data__fcf_valuation = prepare_chart_data_EXTEND([fcf_discount_row], days_back)


    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PE__CONTINOUS, days_back)
    prepared_chart_data__pe = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PB__CONTINOUS, days_back)
    prepared_chart_data__pb = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PS__CONTINOUS, days_back)
    prepared_chart_data__ps = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.METRIC_PFCF__CONTINOUS, days_back)
    prepared_chart_data__pfcf = prepare_chart_data(data_list)


    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.SHARES_OUTSTANDING_Q, annual * 4)
    prepared_chart_data__shares = prepare_chart_data(data_list)

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_REVENUE_Q, (1 + annual) * 4)
    prepared_chart_data__revenue = prepare_chart_data_TTM(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_Q, (1 + annual) * 4)
    prepared_chart_data__gross = prepare_chart_data_TTM(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.EBITDA_Q, (1 + annual) * 4)
    prepared_chart_data__ebitda = prepare_chart_data_TTM(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_Q, (1 + annual) * 4)
    prepared_chart_data__net_income = prepare_chart_data_TTM(data_list)



    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_ASSETS_Q, annual * 4)
    prepared_chart_data__assets = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.TOTAL_LIABILITIES_Q, annual * 4)
    prepared_chart_data__liabilities = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.STOCKHOLDER_EQUITY_Q, annual * 4)
    prepared_chart_data__equity = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.LONG_TERM_DEBT_Q, annual * 4)
    prepared_chart_data__long_term_debt = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_Q, annual * 4)
    prepared_chart_data__cash = prepare_chart_data(data_list)



    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF_Q, (1 + annual) * 4)
    prepared_chart_data__fcf = prepare_chart_data_TTM(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.CASH_FLOW_CONTINUING_OPERATION_Q, (1 + annual) * 4)
    prepared_chart_data__fcf_oper = prepare_chart_data_TTM(data_list)



    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.STOCKHOLDER_EQUITY, annual)
    prepared_chart_data__balance_sheet = prepare_chart_data(data_list)

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.FCF, annual)
    prepared_chart_data__free_cash_flow_statement = prepare_chart_data(data_list)

    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.GROSS_PROFIT_MARGIN_Q, annual * 4)
    prepared_chart_data__gross_margin = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.OPERATING_INCOME_MARGIN_Q, annual * 4)
    prepared_chart_data__operation_margin = prepare_chart_data(data_list)
    data_list = dao_tickers_data.select_ticker_data(ticker_id, TICKERS_TIME_DATA__TYPE__CONST.NET_INCOME_MARGIN_Q, annual * 4)
    prepared_chart_data__net_margin = prepare_chart_data(data_list)
    

    ticker = dao_tickers.select_ticker(ticker_id)

    return render_template(
        'ticker.html', 
        ticker = ticker,
        plot_price = getChart(
            prepared_chart_data__price[0], 
            [
                prepared_chart_data__price[1],
                prepared_chart_data__target_price[1],
                prepared_chart_data__option_year_price[1],
                prepared_chart_data__eps_valuation[1],
                prepared_chart_data__fcf_valuation[1]
            ], 
            [
                f'Price ({prepared_chart_data__price[1][-1]:.2f})',
                f'Analyst target ({get_safe_value(prepared_chart_data__target_price)})',
                f'Option target ({get_safe_value(prepared_chart_data__option_year_price)})',
                f'EPS valuation ({get_safe_value(prepared_chart_data__eps_valuation)})',
                f'FCF valuation ({get_safe_value(prepared_chart_data__fcf_valuation)})'
            ], 
            'Price'
        ),
        plot_target_price = getChart(prepared_chart_data__target_price[0], [prepared_chart_data__target_price[1]], [''], f'Analysts Target Price ({get_safe_value(prepared_chart_data__target_price)})'),
        
        #plot_option_price_1M = getChart(prepared_chart_data__option_month_price[0], [prepared_chart_data__option_month_price[1]], [''], 'Option price 1M'),

        plot_margins = getChart(
            prepared_chart_data__gross_margin[0], 
            [
                prepared_chart_data__gross_margin[1],
                prepared_chart_data__operation_margin[1],
                prepared_chart_data__net_margin[1]
            ], 
            [
                f'Gross Margin ({prepared_chart_data__gross_margin[1][-1]*100:.2f}%)',
                f'Operating Margin ({prepared_chart_data__operation_margin[1][-1]*100:.2f}%)',
                f'Net Margin ({prepared_chart_data__net_margin[1][-1]*100:.2f}%)'
            ], 
            'Margins'
        ),


        plot_option_price_1Y = getChart(prepared_chart_data__option_year_price[0], [prepared_chart_data__option_year_price[1]], [''], f'Option price 1Y ({get_safe_value(prepared_chart_data__option_year_price)})'),

        plot_pe = getChart(prepared_chart_data__pe[0], [prepared_chart_data__pe[1]], [''], f'PE ({get_safe_value(prepared_chart_data__pe)})'),
        plot_pb = getChart(prepared_chart_data__pb[0], [prepared_chart_data__pb[1]], [''], f'PB ({get_safe_value(prepared_chart_data__pb)})'),
        plot_ps = getChart(prepared_chart_data__ps[0], [prepared_chart_data__ps[1]], [''], f'PS ({get_safe_value(prepared_chart_data__ps)})'),
        plot_pfcf = getChart(prepared_chart_data__pfcf[0], [prepared_chart_data__pfcf[1]], [''], f'PFCF ({get_safe_value(prepared_chart_data__pfcf)})'),

        plot_income_statement = getChart(
            prepared_chart_data__revenue[0], 
            [
                prepared_chart_data__revenue[1],
                prepared_chart_data__gross[1],
                prepared_chart_data__ebitda[1],
                prepared_chart_data__net_income[1]
            ], 
            [
                f'Revenue ({get_safe_growth_rate(prepared_chart_data__revenue)}% p.a.)',
                f'Gross Profit ({get_safe_growth_rate(prepared_chart_data__gross)}% p.a.)',
                f'EBITDA ({get_safe_growth_rate(prepared_chart_data__ebitda)}% p.a.)',
                f'Net Income ({get_safe_growth_rate(prepared_chart_data__net_income)}% p.a.)'
            ], 
            'Income statement (ttm)'
        ),

        plot_balance_sheet = getChart(
            prepared_chart_data__assets[0], 
            [
                prepared_chart_data__assets[1],
                prepared_chart_data__liabilities[1],
                prepared_chart_data__equity[1],
                prepared_chart_data__long_term_debt[1],
                prepared_chart_data__cash[1]
            ], 
            [
                f'Assets ({get_safe_growth_rate(prepared_chart_data__assets)}% p.a.)',
                f'Liabilities ({get_safe_growth_rate(prepared_chart_data__liabilities)}% p.a.)',
                f'Equity ({get_safe_growth_rate(prepared_chart_data__equity)}% p.a.)',
                f'Long debt ({get_safe_growth_rate(prepared_chart_data__long_term_debt)}% p.a.)',
                f'Cash ({get_safe_growth_rate(prepared_chart_data__cash)}% p.a.)'
            ], 
            'Balance sheet'
        ),

        plot_free_cash_flow_statement = getChart(
            prepared_chart_data__fcf_oper[0], 
            [
                prepared_chart_data__fcf_oper[1],
                prepared_chart_data__fcf[1]
            ], 
            [
                f'FCF Op ({get_safe_growth_rate(prepared_chart_data__fcf_oper)}% p.a.)',
                f'FCF ({get_safe_growth_rate(prepared_chart_data__fcf)}% p.a.)'
            ], 
            'FCF Statement (ttm)'
        ),     
        plot_shares = getChart(prepared_chart_data__shares[0], [prepared_chart_data__shares[1]], [''], f'Shares outstanding ({get_safe_growth_rate(prepared_chart_data__shares)}% p.a.)')
        
    )

@app.route('/chart')
def bokeh_plot():
    plot = figure(title="Apple", x_axis_label='Date', y_axis_label='Price', width=1600, height=800)

    connection = DB.get_connection_mysql()
    dao_tickers = DAO_Tickers(connection)
    dao_tickers_data = DAO_TickersData(connection)
    price_data_list = dao_tickers_data.select_ticker_data('AAPL', TICKERS_TIME_DATA__TYPE__CONST.PRICE, 500)
    price_data_list.sort(key=lambda x : x.date, reverse=False)

    date_list = []
    price_list = []

    count = 0
    for price in price_data_list:
        price : ROW_TickersData
        count +=1
        date_list.append(count)
        price_list.append(price.value)

    plot.line(date_list, price_list, line_width=1)
    
    # Embed plot into HTML via div element
    html = file_html(plot, CDN, "my plot")
    return render_template_string('<html><body>{{ plot_div | safe }}</body></html>', plot_div=html)

@app.route('/')
def main():
    jobs = scheduler.get_jobs()
    #for job in jobs:

    return render_template('index.html', jobs=jobs)

@app.route('/portfolios')
def portfolios():

    connection = DB.get_connection_mysql()
    dao_portfolios = DAO_Portfolios(connection)
    portfolios = dao_portfolios.select_all_portfolios()
    return render_template('portfolios.html', portfolios = portfolios)

@app.template_filter('percentage')
def format_percentage(value):
    if value == None:
        return '0%'
    return "{:.2%}".format(value)

@app.route('/portfolio/<int:portfolio_id>')
def portfolio(portfolio_id):

    connection = DB.get_connection_mysql()
    dao_portfolios = DAO_Portfolios(connection)
    dao_portfolio_positions = DAO_PortfolioPositions(connection)
    dao_tickers = DAO_Tickers(connection)
    portfolio = dao_portfolios.select_portfolio(portfolio_id)
    positions = dao_portfolio_positions.select_all_portfolio_positions(portfolio_id)
    web_positions = []

    for position in positions:
        position: ROW_PortfolioPositions
        ticker = dao_tickers.select_ticker(position.ticker_id)
        web_position: ROW_WebPortfolioPosition = ROW_WebPortfolioPosition(ticker, position)
        web_positions.append(web_position)

    return render_template('portfolio.html', web_positions = web_positions, portfolio = portfolio)

@app.route('/portfolios/submit_new', methods=['POST'])
def portfolios_submit_new():
    name = request.form['name']

    if (name not in (None, '')):
        connection = DB.get_connection_mysql()
        dao_portfolios = DAO_Portfolios(connection)
        p = ROW_Portfolios(name)
        dao_portfolios.insert_portfolio(p)

    return redirect(url_for('portfolios'))

@app.route('/portfolios/submit_delete', methods=['POST'])
def portfolios_submit_delete():
    id = request.form['id']

    if (id not in (None, '')):
        connection = DB.get_connection_mysql()
        dao_portfolios = DAO_Portfolios(connection)
        dao_portfolios.delete_portfolio(id)

    return redirect(url_for('portfolios'))

@app.route('/portfolio_positions/submit_new', methods=['POST'])
def portfolio_positions_submit_new():
    ticker_id = request.form['ticker_id']
    portfolio_id = request.form['portfolio_id']

    if (ticker_id not in (None, '') and portfolio_id not in (None, '')):
        connection = DB.get_connection_mysql()
        dao_tickers = DAO_Tickers(connection)
        dao_portfolio_positions = DAO_PortfolioPositions(connection)

        ticker = dao_tickers.select_ticker(ticker_id)
        if ticker != None:
            p = ROW_PortfolioPositions()
            p.portfolio_id = portfolio_id
            p.ticker_id = ticker_id
            dao_portfolio_positions.insert_portfolio_position(p)

    return redirect(url_for('portfolio',portfolio_id = portfolio_id))

@app.route('/portfolio_positions/submit_delete', methods=['POST'])
def portfolio_positions_submit_delete():
    ticker_id = request.form['ticker_id']
    portfolio_id = request.form['portfolio_id']

    if (ticker_id not in (None, '') and portfolio_id not in (None, '')):
        connection = DB.get_connection_mysql()
        dao_portfolio_positions = DAO_PortfolioPositions(connection)
        dao_portfolio_positions.delete_portfolio_position(portfolio_id=portfolio_id, ticker_id=ticker_id)

    return redirect(url_for('portfolio', portfolio_id = portfolio_id))

    app.run(debug=False,host='0.0.0.0')