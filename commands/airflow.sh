curl -O docker-compose.yaml 'https://airflow.apache.org/docs/apache-airflow/2.3.3/docker-compose.yaml'

mkdir plugins
mkdir dags
mkdir logs

docker-compose up airflow-init

docker-compose up --build

docker-compose up