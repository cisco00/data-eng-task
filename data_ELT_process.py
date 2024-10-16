import yfinance as stock_data
from pymongo import MongoClient
from sqlalchemy.orm.collections import collection

ticker = "AAPL"

stock_dt = stock_data.download(ticker, start = "2024-01-01", end="2024-09-01")

# Converting data to a json file for easy access by mongo
stock_dt = stock_dt.to_dict(orient='records')

#Coonecting to Mongodb
client = MongoClient('localhost', 27017)

# Connecting to database
db = client['financeStockData']

collections = db['stock_data']

# Populating the table
collections.insert_many(stock_dt)