from pymongo import MongoClient
from datetime import datetime, timedelta
from sqlalchemy.orm.collections import collection
import yfinance as yf

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
