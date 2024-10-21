from pymongo import MongoClient
from datetime import datetime, timedelta
import yfinance as yf
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Function to connect to MongoDB
def connecting_db():
    client = MongoClient('localhost', 27017)
    db = client['financeStockData']  # Database name
    collection = db['stock_data']  # Collection name
    return collection

# Function to retrieve new stock data using yfinance
def data_retrieval(start_date, end_date):
    ticker = "AAPL"  # Example: Apple stock ticker
    stock_data = yf.download(ticker, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
    data = stock_data.reset_index().to_dict('records')  # Convert DataFrame to list of dictionaries
    return data

# Function to check the latest date in MongoDB
def get_last_saved_date(collection):
    last_entry = collection.find_one(sort=[("Date", -1)])  # Sort by 'Date' in descending order and get the first document
    if last_entry:
        return last_entry['Date']
    else:
        return None

# Function to insert new data into MongoDB
def save_data_db(new_data):
    collection = connecting_db()
    if new_data:
        collection.insert_many(new_data)  # Insert new stock data into the collection
        print(f"Inserted {len(new_data)} new records into MongoDB.")
    else:
        print("No new data to insert.")

# Function to update the MongoDB database with new data
def update_database():
    collection = connecting_db()

    # Check the latest saved date in MongoDB
    last_saved_date = get_last_saved_date(collection)

    if last_saved_date:
        start_date = last_saved_date + timedelta(days=1)
    else:
        start_date = datetime.now() - timedelta(days=365)  # If no data, fetch last 1 year
    end_date = datetime.now()

    # Fetch new stock data
    new_data = data_retrieval(start_date, end_date)

    # Insert the new data into MongoDB
    save_data_db(new_data)


# Set default arguments for the DAG
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
dag = DAG(
    'daily_stock_data_update',
    default_args=default_args,
    description='DAG to update stock data daily using PythonOperator',
    schedule_interval='0 0 * * *',  # Run every day at midnight
    catchup=False,
)

# Define the PythonOperator tasks
pulling_data_from_yfinance = PythonOperator(
    task_id="pulling_data_from_yfinance",
    python_callable=update_database,  # Update the database with the latest stock data
    dag=dag,
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
    },
    dag=dag
)

# Task dependencies
pulling_data_from_yfinance >> run_notebook
