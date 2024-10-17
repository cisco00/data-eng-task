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

def run_data_update():
    data = DataELT()
    data.update()

# Creating an automation with airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Ikwu_Francis",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "email": ["idokofrancis66@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    'daily_stock_data_update',
    default_args=default_args,
    description='DAG to update stock data daily using PythonOperator',
    schedule='0 0 * * *',  # Run every day at midnight
    catchup=False,
) as dag:

    update_stock_data = PythonOperator(
        task_id='run_dag',
        python_callable=run_data_update,
    )
update_stock_data