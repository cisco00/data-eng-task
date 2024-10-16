import yfinance as stock_data

ticker = "AAPL"

stock_dt = stock_data.download(ticker, start = "2024-01-01", end="2024-09-01")

