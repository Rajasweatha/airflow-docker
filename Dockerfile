FROM apache/airflow:3.0.6

USER airflow

RUN pip install dbt-core dbt-snowflake

USER airflow