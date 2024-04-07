import http.server
import socketserver

from data_providers.alpha_vantage import get_tickers_download

PORT = 8080

Handler = http.server.SimpleHTTPRequestHandler

if __name__ == "__main__":
    tickerList = get_tickers_download()
    for row in tickerList:
        print(f"Ticker: {row}")

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
