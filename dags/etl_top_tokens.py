from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta

QUERY_DROP_TABLE = '''
            DROP TABLE IF EXISTS cuevatomass02_coderhouse.criptos_market_cap;
'''

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
            ) SORTKEY(market_cap_rank, high_24h, low_24h, total_volume);
        '''

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM users WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""


# create function to get process_date and push it to xcom
def get_process_date(**kwargs):
    # If process_date is provided take it, otherwise take today
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


defaul_args = {
    "owner": "Tomas Cueva",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_top_tokens",
    default_args=defaul_args,
    description="ETL de carga de datos del top 100 tokens con mayor capitalización ded mercado",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
    )

    drop_table = SQLExecuteQueryOperator(
        task_id = "drop_table",
        conn_id = "redshift_default",
        sql = QUERY_DROP_TABLE,
        dag = dag,
    )
    
    create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "redshift_default",
        sql = QUERY_CREATE_TABLE,
        dag = dag,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id = "clean_process_date",
        conn_id = "redshift_default",
        sql = QUERY_CLEAN_PROCESS_DATE,
        dag = dag,
    )

    spark_etl_tokens = SparkSubmitOperator(
        task_id = "spark_etl_tokens",
        application = f'{Variable.get("spark_scripts_dir")}/ETL_top_tokens.py',
        conn_id = "spark_default",
        dag = dag,
        driver_class_path = Variable.get("driver_class_path"),
    )

    get_process_date_task >> drop_table >> create_table >> clean_process_date >> spark_etl_tokens
