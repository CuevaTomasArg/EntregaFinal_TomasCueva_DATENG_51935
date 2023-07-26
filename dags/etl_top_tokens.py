from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta

QUERY_CREATE_TABLE = '''
    CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.criptos_market_cap (
        id VARCHAR(100) DISTKEY PRIMARY KEY,
        symbol CHAR(4),
        name VARCHAR(100),
        current_price FLOAT,
        market_cap FLOAT,
        market_cap_rank INT,
        total_volume FLOAT,
        high_24h FLOAT,
        low_24h FLOAT,
        price_change_24h FLOAT,
        price_change_percentage_24h FLOAT,
        market_cap_change_24h FLOAT,
        market_cap_change_percentage_24h FLOAT,
        circulating_supply FLOAT,
        ath FLOAT,
        ath_change_percentage FLOAT,
        ath_date TIMESTAMP,
        atl FLOAT,
        atl_change_percentage FLOAT,
        atl_date TIMESTAMP,
        last_updated TIMESTAMP
    );
'''

defaul_args = {
    "owner": "Tomas Cueva",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds = 5),
}

with DAG(
    dag_id = "etl_top_tokens",
    default_args = defaul_args,
    description = "ETL del top 100 tokens con mayor capitalización ded mercado",
    schedule_interval = "@daily",
    catchup = False,
) as dag:
    
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    spark_etl_tokens = SparkSubmitOperator(
        task_id = "spark_etl_tokens",
        application = f'{Variable.get("spark_scripts_dir")}/ETL_top_tokens.py',
        conn_id = "spark_default",
        dag = dag,
        driver_class_path = Variable.get("driver_class_path"),
    )

    create_table >> spark_etl_tokens
