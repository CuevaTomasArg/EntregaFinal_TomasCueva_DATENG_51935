from airflow import DAG
#from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    "owner": "Tomas Cueva",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

# Define el DAG actual
with DAG(
    dag_id = "trigger_bitcoin",
    default_args = default_args,
    description = "Envío de alertas para comprar bitcoin",
    schedule_interval = "@daily",
    catchup = False
) as dag:

    # Define la tarea anterior (de otro DAG) que se debe haber completado correctamente
    previous_dag_id = "etl_marlet_chart"  # Reemplaza esto con el ID del DAG anterior
    previous_task_id = "check_table_create"  # Reemplaza esto con el ID de la tarea del DAG anterior

    # Sensor que espera a que se complete la tarea del DAG anterior
    #wait_for_previous_dag = ExternalTaskSensor(
    #    task_id = "wait_etl_market_chart_dag",
    #    external_dag_id = previous_dag_id,
    #    external_task_id = previous_task_id,
    #    mode = "reschedule",  # Puedes usar "reschedule" para volver a verificar en intervalos regulares o "poke" para una verificación más frecuente
    #    timeout = 60 * 15,  # Tiempo de espera en segundos antes de que el sensor falle (opcional)
    #)

    spark_bitcoin_trigger = SparkSubmitOperator(
        task_id = "spark_bitcoin_trigger",
        application = f'{Variable.get("spark_scripts_dir")}/bitcoin_trigger.py',
        conn_id = "spark_default",
        dag = dag,
        driver_class_path = Variable.get("driver_class_path"),
    )

    spark_bitcoin_trigger
