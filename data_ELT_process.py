from pymongo import MongoClient
from datetime import datetime, timedelta
import yfinance as yf
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow import DAG
from airflow.operators.python import PythonOperator, BaseOperator

# Define DataELT class
class DataELT:
    def __init__(self):
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
        ticker = "AAPL"
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)
        stock_data = yf.download(ticker, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
        data = stock_data.to_dict('records')
        return data

    def update(self):
        data = self.data_retrieval()
        self.insert_data_db(data)

# Python function to call DataELT update
def run_data_update():
    data = DataELT()
    data.update()

# Set default arguments
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

# Define the DAG
with DAG(
    'daily_stock_data_update',
    default_args=default_args,
    description='DAG to update stock data daily using PythonOperator',
    schedule='0 0 * * *',  # Run every day at midnight
    catchup=False,
) as dag:

    # PythonOperator to run the update function
    update_stock_data = PythonOperator(
        task_id='run_data_update',
        python_callable=run_data_update
    )

    output = BaseOperator(
        task_id='creating_output_file',
        bash_command='mkdir -p /home/oem/PycharmProjects/data-eng-task/output/'
    )

    # PapermillOperator to execute the Jupyter Notebook
    run_notebook = PapermillOperator(
        task_id='run_stock_data_notebook',
        input_nb='/home/oem/PycharmProjects/data-eng-task/forecastingModel.ipynb',  # Change this path
        output_nb='/home/oem/PycharmProjects/data-eng-task/output/data_retrieval_notebook_output_{{ ds }}.ipynb',  # Output path with dynamic date
        parameters={
            'ticker': 'AAPL',
            'start_date': '{{ ds }}',  # Airflow start date macro
            'end_date': '{{ next_ds }}'  # Airflow end date macro
        }
    )

    # Set task dependencies if needed
    update_stock_data >> run_notebook
