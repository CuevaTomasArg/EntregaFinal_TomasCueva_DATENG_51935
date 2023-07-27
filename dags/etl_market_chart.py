from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import smtplib

QUERY_CREATE_TABLE = '''
    CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.market_charts(
        id VARCHAR(100) DISTKEY PRIMARY KEY,
        prices FLOAT,
        total_volumes FLOAT,
        market_caps FLOAT,
        date_unix DOUBLE PRECISION 
    );
'''

default_args = {
    "owner": "Tomas Cueva",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

def enviar_fallo():
    try:
        smtp_conn = smtplib.SMTP('smtp.gmail.com', 587)
        smtp_conn.starttls()
        smtp_conn.login(Variable.get('smtp_from'), Variable.get('smtp_password'))
        subject = 'ERROR: Market Charts ETL'
        body_text = 'Error en el proceso ETL, revisar LOGs'
        message = 'Subject: {}\n\n{}'.format(subject, body_text)
        smtp_conn.sendmail(Variable.get('smtp_from'), Variable.get('smtp_to'), message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')
        raise exception

with DAG(
    dag_id = "etl_market_chart",
    default_args = default_args,
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

    send_email_failure = PythonOperator(
        task_id = 'enviar_fallo',
        python_callable = enviar_fallo,
        trigger_rule = 'all_failed',  
        provide_context = True, 
        dag = dag,
    )

    create_table >> spark_etl_market_charts >> send_email_failure
