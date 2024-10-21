FROM apache/airflow:latest-python3.10

USER root
RUN apt-get update &&\
    apt-get -y install git && \
    apt-get clean

USER airflow