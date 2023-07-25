from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta

#QUERY_CREATE_TABLE = '''
#    CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.market_charts(
#        id VARCHAR(100) DISTKEY PRIMARY KEY,
#        prices FLOAT,
#        total_volumes FLOAT,
#        market_caps FLOAT,
#        date_unix DOUBLE PRECISION 
#    );
#'''

#def get_process_date(**kwargs):
#    if (
#        "process_date" in kwargs["dag_run"].conf
#        and kwargs["dag_run"].conf["process_date"] is not None
#    ):
#        process_date = kwargs["dag_run"].conf["process_date"]
#    else:
#        process_date = kwargs["dag_run"].conf.get(
#            "process_date", datetime.now().strftime("%Y-%m-%d")
#        )
#    kwargs["ti"].xcom_push(key = "process_date", value = process_date)

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
    
    #get_process_date_task = PythonOperator(
    #    task_id = "get_process_date",
    #    python_callable = get_process_date,
    #    provide_context = True,
    #    dag = dag,
    #)
    
    #create_table = SQLExecuteQueryOperator(
    #    task_id = "create_table",
    #    conn_id = "redshift_default",
    #    sql = QUERY_CREATE_TABLE,
    #    dag = dag,
    #)

    spark_etl_market_charts = SparkSubmitOperator(
        task_id = "spark_etl_market_charts",
        application = f'{Variable.get("spark_scripts_dir")}/ETL_market_charts.py',
        conn_id = "spark_default",
        dag = dag,
        driver_class_path = Variable.get("driver_class_path"),
    )

    spark_etl_market_charts
