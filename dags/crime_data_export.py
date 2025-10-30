from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# config
DATABASE = "CRIME_DB"
SCHEMA = "EXPORTS"
STAGE = "CRIME_EXPORT_STAGE"
SNOWFLAKE_CONN_ID = "snowflake_conn"
DBT_PROJECT_PATH = "/opt/airflow/dbt/crime_data_analysis"

# Failure alert
def on_failure_notify(context):
    task = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')
    log_url = task.log_url

    message = f"""Airflow Task Failed
    DAG: {dag_id}
    Task: {task.task_id}
    Execution Date: {execution_date}
    Log: {log_url}
    """
    logging.error(message)

# Export
def export_crime_data():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    filename = f"export_crime_by_location_type_{datetime.now().strftime('%Y-%m-%d')}.csv"

    results = cursor.execute(f"""
        COPY INTO @{DATABASE}.PUBLIC.{STAGE}/{filename}
        FROM (
            SELECT * FROM {DATABASE}.{SCHEMA}.EXPORT_CRIME_BY_LOCATION_TYPE
        )
        OVERWRITE = TRUE
        HEADER = TRUE
        SINGLE = TRUE
        MAX_FILE_SIZE = 4900000000
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' COMPRESSION = NONE)
    """).fetchall()

    logging.info(results)
    logging.info(f"Crime data exported to stage: @{DATABASE}.PUBLIC.{STAGE}/{filename}")

# DAG
with DAG(
    dag_id="crime_data_export",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "on_failure_callback": on_failure_notify
    }
) as dag:

    run_dbt_model = BashOperator(
        task_id="run_dbt_model",
        bash_command="""
        cd /opt/airflow/dbt/crime_data_analysis &&
        dbt run --select crime_by_location_type
        """,
        on_failure_callback=on_failure_notify,
    )

    export_task = PythonOperator(
        task_id="export_crime_data",
        python_callable=export_crime_data,
        on_failure_callback=on_failure_notify,
    )

    run_dbt_model >> export_task