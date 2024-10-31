from airflow.operators.email import EmailOperator
from pymongo import MongoClient
from datetime import datetime, timedelta
import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import psycopg2
import json


with open('/home/oem/PycharmProjects/data-eng-task/dags/secret.json') as f:
    secrets = json.load(f)

# Access secrets
db_name = secrets['DB_NAME']
db_user = secrets['DB_USER']
db_pass = secrets['DB_PASS']

# Database connection and data handling class
class DatabaseHandler:
    def __init__(self, db_name, db_user, db_pass):
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass

    def connect_postgres(self):
        return psycopg2.connect(dbname=self.db_name, user=self.db_user, host="localhost", port="5432",
                                password=self.db_pass)

    def get_last_saved_date(self, table_name):
        conn = self.connect_postgres()
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(Date) FROM {table_name};")
        last_date = cur.fetchone()[0]
        cur.close()
        conn.close()
        return last_date

    def insert_data_postgres(self, new_data):
        try:
            conn = self.connect_postgres()
            cur = conn.cursor()
            insert_query = """INSERT INTO apple_stock_data ("Date", "Open", "High", "Low", "Close", "Volume") VALUES(%s, %s, %s, %s, %s, %s)"""
            dataset = [(row["Date"], row["Open"], row["High"], row["Low"], row["Close"], row["Volume"])
                       for row in new_data]
            cur.executemany(insert_query, dataset)
            conn.commit()
            print(f"{len(dataset)} records inserted into apple_stock_data.")
        except psycopg2.Error as e:
            print("Error inserting data into PostgreSQL:", e)
        finally:
            cur.close()
            conn.close()


# MongoDB connection and data handling
def connecting_db():
    client = MongoClient('localhost', 27017)
    db = client['financeStockData']  # Database name
    return {
        "AAPL": db['AAPL_stock_data'],  # Apple collection
        "TSLA": db['TSLA_stock_data'],  # Tesla collection
        "GOOGL": db['GOOGL_stock_data']  # Alphabet collection
    }


# Function to retrieve new stock data using yfinance
def data_retrieval(ticker, start_date, end_date):
    stock_data = yf.download(ticker, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
    stock_data.reset_index(inplace=True)
    stock_data['Date'] = stock_data['Date'].astype(str)
    return stock_data.to_dict('records')


# Function to get last saved date from MongoDB
def get_last_saved_date_mongo(collection):
    last_entry = collection.find_one(sort=[("Date", -1)])  # Sort by 'Date' in descending order
    if last_entry and 'Date' in last_entry:
        return datetime.strptime(last_entry['Date'], '%Y-%m-%d')
    return None


# Function to save data into MongoDB
def save_data_db(collection, new_data):
    if new_data:
        collection.insert_many(new_data)  # Insert new stock data into the collection
        print(f"Inserted {len(new_data)} new records into MongoDB.")
    else:
        print("No new data to insert.")


# Main function to update databases
def update_database():
    db_handler = DatabaseHandler(db_name, db_user, db_pass)
    collections = connecting_db()

    # List of tickers and their corresponding collections
    for ticker, collection in collections.items():
        # Check the latest saved date in MongoDB
        last_saved_date = get_last_saved_date_mongo(collection)

        if last_saved_date:
            start_date = last_saved_date + timedelta(days=1)
        else:
            start_date = datetime.now() - timedelta(days=365)  # If no data, fetch last 1 year

        end_date = datetime.now()

        # Fetch new stock data
        new_data = data_retrieval(ticker, start_date, end_date)

        # Insert the new data into MongoDB
        save_data_db(collection, new_data)

        # Insert the new data into PostgreSQL
        db_handler.insert_data_postgres(new_data)

update_database()


def email_failure(context):
    subject = "Airflow Task Failed"
    body = f"Task failed: {context['task_instance_key_str']}"
    send_email(to="recipient@example.com", subject=subject, html_content=body)


# Set up the Airflow DAG (example)
default_args = {
    "owner": "Ikwu_Francis",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": email_failure,
}

dag = DAG(
    'daily_stock_data_update',
    default_args=default_args,
    description='DAG to update stock data daily',
    schedule='@daily',  # Run every day at midnight
    catchup=False,
)

# Define the PythonOperator task
update_task = PythonOperator(
    task_id="update_database_task",
    python_callable=update_database,
    dag=dag,
)


# email_task = EmailOperator(
#     task_id='send_failure_email',
#     to="idokofrancis66@gmail.com",
#     subject="Airflow Task Failed",
#     html_content="The stock data update task has failed.",
#     dag=dag,
# )

update_task