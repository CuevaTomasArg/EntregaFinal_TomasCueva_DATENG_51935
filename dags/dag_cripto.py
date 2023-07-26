from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.providers.email.operators.email import EmailOperator
from utils.extract_CoinGecko import get_top_tokens
from utils.connection_spark import PySparkSession
from scripts.ETL_top_tokens import ETLTopTokens
from scripts.ETL_market_charts import ETLMarketCharts

QUERY_CREATE_TABLE_TOP = '''
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

QUERY_CREATE_TABLE_CHARTS = '''
    CREATE TABLE IF NOT EXISTS cuevatomass02_coderhouse.market_charts(
        id VARCHAR(100) DISTKEY PRIMARY KEY,
        prices FLOAT,
        total_volumes FLOAT,
        market_caps FLOAT,
        date_unix DOUBLE PRECISION 
    );
'''

default_args = {
    'owner': '@CuevaTomasArg',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 26),
    'email': ['cuevatomass02@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def run_etl_market_charts(ti):
    etl = ETLMarketCharts()
    data = etl.extract()
    df = etl.transform(data)
    etl.load(df)

def run_etl_top_tokens():
    etl = ETLTopTokens()
    json = etl.extract()
    
    if isinstance(json, str):
        print('Error:', json)
    else:
        df = etl.transform(json)
        etl.load(df)
        
def get_top_tokens_list():
    top_tokens = get_top_tokens()  # Use the function to get the top 5 tokens
    return top_tokens

def decide_branch(**kwargs):
    top_tokens = kwargs['ti'].xcom_pull(task_ids='get_top_tokens')
    return 'etl_market_charts' if top_tokens else 'end_dag'

with DAG(
    dag_id = 'dag_cripto',
    default_args = default_args,
    description = 'DAG para realizar tareas ETL para datos de criptomonedas',
    tags = ['Criptos', 'DatosMercado'],
    schedule_interval = '@daily',
    catchup = False,
) as dag:

    check_redshift_tables = SqlSensor(
        task_id = 'check_redshift_tables',
        conn_id = "redshift_default",
        sql = 'SELECT 1;',
        poke_interval = 10,
        timeout = 120,
        dag = dag
    )

    get_top_tokens_task = PythonOperator(
        task_id = 'get_top_tokens',
        python_callable = get_top_tokens_list,
    )

    check_top_tokens = BranchPythonOperator(
        task_id = 'check_top_tokens',
        python_callable = decide_branch,
        provide_context = True,
    )

    etl_top_tokens_task = PythonOperator(
        task_id='etl_top_tokens',
        python_callable=ETLTopTokens().run_etl,
    )

    etl_market_charts_task = PythonOperator(
        task_id='etl_market_charts',
        python_callable=ETLMarketCharts().run_etl,
    )

    end_dag_task = EmailOperator(
        task_id='end_dag',
        to='cuevatomass02@gmail.com',
        subject='Crypto ETL DAG Failed',
        html_content='The DAG encountered an issue during execution.',
    )

    check_redshift_tables >> get_top_tokens_task
    get_top_tokens_task >> check_top_tokens
    check_top_tokens >> etl_top_tokens_task
    check_top_tokens >> etl_market_charts_task
    etl_top_tokens_task >> etl_market_charts_task
    etl_market_charts_task >> end_dag_task
