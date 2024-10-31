import pickle as pk
import streamlit as st
import pandas as pd
import psycopg2 as postgres
import json

from dags.data_ELT_process import db_name

with open("/home/oem/PycharmProjects/data-eng-task/dags/secret.json") as f:
    secret = json.load(f)

db_name = secret['DB_NAME']
db_user = secret['DB_USER']
db_pass = secret['DB_PASS']

st.title("Apple Forecasting Model")

model = pk.load(open("model.pkl", "rb"))

def db_data_retriveal(db_name, db_user, db_pass, db_host, db_port):
    db = postgres.connect(db_name, db_user, db_pass, host=db_host, port=db_port)
    db.cursor().execute("SELECT Adj Close from apple_stock_data")

    df = pd.DataFrame(db.cursor().fetchall())

    return df

def data_cleaning(df):
    clean_df = df.dropna(axis=0, how='any')
    return clean_df

def model_selection(model):
    return
