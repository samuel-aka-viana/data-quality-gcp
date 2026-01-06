FROM apache/airflow:2.8.1

USER airflow

RUN pip install --no-cache-dir "apache-airflow-providers-google>=10.12.0" google-cloud-bigquery pandas
