from pymongo import MongoClient
from datetime import datetime, timedelta
from sqlalchemy.orm.collections import collection
import yfinance as yf
#
# ticker = "AAPL"
#
# stock_dt = yf.download(ticker, start = "2024-01-01", end="2024-09-01")
#
# # Converting data to a json file for easy access by mongo
# stock_dt = stock_dt.to_dict(orient='records')
#
# #Coonecting to Mongodb
# client = MongoClient('localhost', 27017)
#
# # Connecting to database
# db = client['financeStockData']
#
# collections = db['stock_data']
#
# # Populating the table
# collections.insert_many(stock_dt)

class DataELT:
    def __init__(self):
        # Set up MongoDB connection
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['financeStockData']
        self.collection = self.db['stock_data']

    def insert_data_db(self, data):
        if data:
            self.collection.insert_many(data)
            print(f"Inserted {len(data)} records into MongoDB.")
        else:
            print("No Data Inserted")


    def data_retrieval(self):
        # Define the ticker symbol
        ticker = "AAPL"

        # Get today's date and yesterday's date
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)

        # Download the latest data
        stock_data = yf.download(ticker, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))

        # Convert DataFrame to a list of dictionaries
        data = stock_data.to_dict('records')

        return data


    def update(self):
        data = self.data_retrieval()
        self.insert_data_db(data)


# Example usage
if __name__ == "__main__":
    elt = DataELT()
    elt.update()
