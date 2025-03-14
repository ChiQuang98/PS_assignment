FROM apache/airflow:2.7.3-python3.10
LABEL maintainer="Andrew Tran"

WORKDIR ${AIRFLOW_HOME}

COPY requirements.txt .

RUN pip install --no-cache-dir "apache-airflow==2.7.3" -r requirements.txt

USER root
ENV PIP_USER=true
USER airflow

