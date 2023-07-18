# 51940-CuevaTomas
---
Dentro de este repositorio se encuentra el proceso del proyecto final del curso "Data Engineering flex" de Coderhouse.

## Contenido
Los archivos a tener en cuenta son:
* `docker-compose.yml`: Archivo de configuración de Docker Compose. Contiene la configuración de los servicios de Airflow y Spark.
* `.env`: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
* `Dockerfile.airflow`:  Dockerfile utilizado por Docker Compose para construir la imagen de airflow.
* `Dockerfile.spark`:  Dockerfile utilizado por Docker Compose para construir la imagen de spark master, worker y submit.
* `requirements.txt` : Lista las dependencias necesarias para correr el ETL.
* `drivers/`: Carpeta con drivers.
    * `postgresql-42.5.2.jar`: Driver de Postgres para Spark.
* `dags/`: Carpeta con los archivos de los DAGs.
    * `etl_top_tokens.py`: DAG que ejecuta el ETL de extracción, transformación y carga de datos de API de CoinGecko a Amazon Redshift.
* `logs/`: Carpeta con los archivos de logs de Airflow.
* `plugins/`: Carpeta con los plugins de Airflow.
* `scripts/`: Carpeta con scripts SQL, Bash y Python.
    * `bash/`: Carpeta con scripts de bash.
    * `sql/`: Carpeta con scripts SQL.
        * `creacion_tabla.sql`: scripts de creación de tablas en Amazon Redshift.
    * `utils/`: Paquete de python compuesto por modulos para extraer, transformar y cargar datos.
        * `extract_CoinGecko.py`: Modulo para extrar datos de la API pública de CoinGecko.
        * `transform_df.py`: Modulo para transformar o castear datos.
        * `load_redshift.py`: Modulo de carga de datos desde DataFrame de Spark a Data Warehouse de Amazon Redshift.
    * `ETL_top_tokens.py`: Script que contiene el proceso ETL que ejecuta el DAG `etl_top_tokens.py`.


## Ejecución del proyecto
1. Posicionarse en la carpeta raiz del proyecto,a esta altura del archivo `docker-compose.yml`.
2. Crear las siguientes carpetas a la misma altura del `docker-compose.yml`.
```bash
mkdir -p logs,plugins
```
3. Ejecutar el siguiente comando para levantar los servicios de Airflow y Spark.
```bash
docker-compose up --build
```
4. Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/` con usuario "airflow" y contraseña "airflow".
5. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
6. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Spark:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`
    * Extra: `{"queue": "default"}`
7. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `driver_class_path`
    * Value: `/tmp/drivers/postgresql-42.5.2.jar`
8. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `spark_scripts_dir`
    * Value: `/opt/airflow/scripts`
9. Si no aparecen los dags una vez agregadas las conexiones y las variables, detener el contenedor con `ctrl + c` en la misma consola (si por alguna razon usaste un `docker-compose up -d` utiliza en comando `docker-compose down` dentro del mismo directorio) y ejecuta denuevo el comando `docker-compose up`.
10. Ejecutar los DAGs `etl_top_tokens` y `etl_market_chart`.


