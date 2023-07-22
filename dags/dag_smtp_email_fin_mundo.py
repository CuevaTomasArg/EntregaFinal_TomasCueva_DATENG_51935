from datetime import datetime
from email import message
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
import smtplib

pais=['Argentina','Brasil','Colombia']
acronimo= ['AR','BR','CO']
lista_fin_mundo=[2040,2080,2095]

texto=[]

for i in range(len(pais)):
    string='Pais {} ({}), Fecha fin mundo estimada: {}'.format(pais[i], acronimo[i],lista_fin_mundo[i])
    texto.append(string)

final = '\n'.join(texto)
print(final)

def enviar():
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('cuevatomass02@gmail.com','TomasteawitaProyecta2002!')
        subject='Fechas fin del mundo'
        body_text=final
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('cuevatomass02@gmail.com','tomassantiagocueva@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')

default_args={
    'owner': 'DavidBU',
    'start_date': datetime(2022,9,7)
}

with DAG(
    dag_id='dag_smtp_email_fin_mundo',
    default_args=default_args,
    schedule_interval='@daily') as dag:

    tarea_1=PythonOperator(
        task_id='dag_envio_fin_mundo',
        python_callable=enviar
    )

    tarea_1