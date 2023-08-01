from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.models import Variable
from datetime import datetime, timedelta
from os import environ as env
from sqlalchemy import create_engine, text
import smtplib

QUERY_CHECK_TABLE = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = 'cuevatomass02_coderhouse'
      AND table_name = 'market_charts'
"""


def bitcoin_trend(**kwargs):
    REDSHIFT_HOST = env['REDSHIFT_HOST']
    REDSHIFT_PORT = env['REDSHIFT_PORT']
    REDSHIFT_DB = env['REDSHIFT_DB']
    REDSHIFT_USER = env["REDSHIFT_USER"]
    REDSHIFT_PASSWORD = env["REDSHIFT_PASSWORD"]
    REDSHIFT_URL = f"postgresql://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"
    engine = create_engine(REDSHIFT_URL)

    # Definimos la consulta SQL para obtener la tendencia de Bitcoin en los últimos 7 días
    QUERY_BITCOIN_TREND = """
    WITH last_7_values AS (
        SELECT *
        FROM market_charts
        WHERE id = 'bitcoin'
        ORDER BY date_unix DESC
        LIMIT 7
    )
    SELECT ((SELECT MAX(prices) FROM last_7_values) / (SELECT MIN(prices) FROM last_7_values) * 100) - 100 AS price_increase;
    """

    # Ejecutamos la consulta y obtenemos el resultado
    with engine.connect() as connection:
        result = connection.execute(text(QUERY_BITCOIN_TREND)).fetchone()

    trend_bitcoin = result[0]
    kwargs["ti"].xcom_push(key = "trend_bitcoin", value = trend_bitcoin)

def bitcoin_alert_email(**kwargs):
    try:
        smtp_conn = smtplib.SMTP('smtp.gmail.com', 587)
        smtp_conn.starttls()
        smtp_conn.login(Variable.get('smtp_from'), Variable.get('smtp_password'))
        subject = 'TENDENCIA DE BITCOIN ESTA SEMANA'
        trend_bitcoin = kwargs["ti"].xcom_pull(key = "trend_bitcoin", task_ids = "get_trend_bitcoin")
        print(f"La tendencia semanal fue:{trend_bitcoin}")
        body_text = f"Tendencia de Bitcoin en los ultimos 7 dias: {trend_bitcoin:.2f}%"
        message = 'Subject: {}\n\n{}'.format(subject, body_text)
        smtp_conn.sendmail(Variable.get('smtp_from'), Variable.get('smtp_to'), message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')
        raise exception

default_args = {
    "owner": "Tomas Cueva",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id = "trigger_bitcoin",
    default_args = default_args,
    description = "Envío de alertas para comprar bitcoin",
    schedule_interval = "@weekly",
    catchup = False
) as dag:

    check_table_sensor = SqlSensor(
        task_id = "check_market_charts_table",
        conn_id = "redshift_default",
        sql = QUERY_CHECK_TABLE,
        poke_interval = 30,
        timeout = 3600,
    )
    
    get_trend_bitcoin = PythonOperator(
        task_id = "get_trend_bitcoin",
        python_callable = bitcoin_trend,
        provide_context = True,
        dag = dag,
    )
    
    bitcoin_alert = PythonOperator(
        task_id = 'send_alert',
        python_callable = bitcoin_alert_email,
        provide_context = True, 
        dag = dag,
    )

    check_table_sensor >> get_trend_bitcoin >> bitcoin_alert
