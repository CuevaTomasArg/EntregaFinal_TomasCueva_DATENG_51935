from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta

QUERY_CREATE_TABLE = '''
    CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.market_charts(
        id VARCHAR(100) DISTKEY PRIMARY KEY,
        prices FLOAT,
        total_volumes FLOAT,
        market_caps FLOAT,
        date_unix DOUBLE PRECISION 
    );
'''

defaul_args = {
    "owner": "Tomas Cueva",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds = 5),
}

with DAG(
    dag_id = "etl_market_chart",
    default_args = defaul_args,
    description = "ETL de la cotización de 5 tokens a lo largo del tiempo",
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "redshift_default",
        sql = QUERY_CREATE_TABLE,
        dag = dag,
    )

    spark_etl_market_charts = SparkSubmitOperator(
        task_id = "spark_etl_market_charts",
        application = f'{Variable.get("spark_scripts_dir")}/ETL_market_charts.py',
        conn_id = "spark_default",
        dag = dag,
        driver_class_path = Variable.get("driver_class_path"),
    )

    create_table >> spark_etl_market_charts
