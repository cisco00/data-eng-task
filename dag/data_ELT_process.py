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
    collection_apple = db['AAPL_stock_data']  # Apple collection
    collection_tesla = db['TSLA_stock_data']  # Tesla collection
    collection_googl = db['GOOGL_stock_data']  # Alphabet collection
    return collection_apple, collection_tesla, collection_googl


# Function to retrieve new stock data using yfinance
def data_retrieval(ticker, start_date, end_date):
    stock_data = yf.download(ticker, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
    stock_data.reset_index(inplace=True)

    # Ensure the 'Date' field is named correctly and converted to string for MongoDB storage
    stock_data['Date'] = stock_data['Date'].astype(str)

    data = stock_data.to_dict('records')  # Convert DataFrame to list of dictionaries
    return data


# Function to check the latest date in MongoDB
def get_last_saved_date(collection):
    last_entry = collection.find_one(
        sort=[("Date", -1)])  # Sort by 'Date' in descending order and get the first document
    if last_entry and 'Date' in last_entry:
        return datetime.strptime(last_entry['Date'], '%Y-%m-%d')
    else:
        return None


# Function to insert new data into MongoDB
def save_data_db(collection, new_data):
    if new_data:
        collection.insert_many(new_data)  # Insert new stock data into the collection
        print(f"Inserted {len(new_data)} new records into MongoDB.")
    else:
        print("No new data to insert.")


# Function to update the MongoDB database with new data
def update_database():
    collection_apple, collection_tesla, collection_googl = connecting_db()

    # List of tickers and their corresponding collections
    ticker_list = [
        ("AAPL", collection_apple),
        ("TSLA", collection_tesla),
        ("GOOGL", collection_googl)
    ]

    for ticker, collection in ticker_list:
        # Check the latest saved date in MongoDB
        last_saved_date = get_last_saved_date(collection)

        if last_saved_date:
            start_date = last_saved_date + timedelta(days=1)
        else:
            start_date = datetime.now() - timedelta(days=365)  # If no data, fetch last 1 year

        end_date = datetime.now()

        # Fetch new stock data
        new_data = data_retrieval(ticker, start_date, end_date)

        # Insert the new data into MongoDB
        save_data_db(collection, new_data)


# Call update_database() to perform the update
def check_document_structure(collection_name):
    collection, collection_2, collection_3 = connecting_db()

    # Select the appropriate collection based on the input argument
    if collection_name == "AAPL":
        collection = collection
    elif collection_name == "TESLA":
        collection = collection_2
    elif collection_name == "GOOGL":
        collection = collection_3
    else:
        raise ValueError(f"Unknown collection name: {collection_name}")

    # Now you can use find_one on the selected collection
    sample_document = collection.find_one()  # Retrieve one document from the collection

    if sample_document:
        print(f"Sample document from {collection_name}: {sample_document}")
    else:
        print(f"No documents found in {collection_name}.")


# Example usage:
check_document_structure("AAPL")
# check_document_structure()

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
    schedule='0 0 * * *',  # Run every day at midnight
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
