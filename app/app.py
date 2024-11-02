import pickle as pk
import streamlit as st
import pandas as pd
import psycopg2
import json

with open("/home/oem/PycharmProjects/data-eng-task/dags/secret.json") as f:
    secret = json.load(f)

db_name = secret['DB_NAME']
db_user = secret['DB_USER']
db_pass = secret['DB_PASS']

st.title("Apple Stock Forecasting Model")

model = pk.load(open("/home/oem/PycharmProjects/data-eng-task/app/arima_model.pkl", "rb"))


def db_data_retrieval(db_name, db_user, db_pass, db_host, db_port):
    # Establish the connection
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )

    query = """SELECT * FROM apple_stock_data ORDER BY Date"""

    df = pd.read_sql_query(query, conn)

    conn.close()

    return df


def column_sel():
    df = db_data_retrieval(db_name, db_user, db_pass, "LocalHost", "54321")

    df = df[["Date", "Adj Close"]]
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index("Date", inplace=True)

    return df

def data_cleaning(df):
    clean_df = df.dropna(axis=0, how='any')
    return clean_df


df = column_sel()
clean_df = data_cleaning(df)
st.write("Last 10 data points:", clean_df.tail(10))

forecast_steps = 10
forecast = model.forecast(steps=forecast_steps)
st.write("Forecasted values for the next 10 steps:", forecast)
