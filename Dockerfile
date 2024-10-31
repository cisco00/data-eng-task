FROM apache/airflow:2.7.0-python3.10

# Install OS-level dependencies as root
USER root
RUN apt-get update && apt-get install -y \
    python3-dev \
    build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch to the airflow user to install Python dependencies
USER airflow
RUN pip install --no-cache-dir pymongo yfinance papermill