# DOcumentación a agregar
como hacer lo del SMTP
cambiar las variables del airflow.cfg del apartado [SMTP]
Poner las variables adecuadas dentro de Airflow
Agregar las contraseñas correspondientes a las variables de entorno

# 51940-CuevaTomas
---
Dentro de este repositorio se encuentra el proceso del proyecto final del curso "Data Engineering flex" de Coderhouse.

## Contenido
Los archivos a tener en cuenta son:
* `config/`: Carpeta con archifo airflow.cfg
* `docker-compose.yml`: Archivo de configuración de Docker Compose. Contiene la configuración de los servicios de Airflow y Spark.
* `.env`: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
* `Dockerfile.airflow`:  Dockerfile utilizado por Docker Compose para construir la imagen de airflow.
* `Dockerfile.spark`:  Dockerfile utilizado por Docker Compose para construir la imagen de spark master, worker y submit.
* `requirements.txt` : Lista las dependencias necesarias para correr el ETL.
* `drivers/`: Carpeta con drivers.
    * `postgresql-42.5.2.jar`: Driver de Postgres para Spark.
* `dags/`: Carpeta con los archivos de los DAGs.
    * `etl_top_tokens.py`: DAG que ejecuta el ETL de datos de la API de CoinGecko a Amazon Redshift; extrae los 100 tokens con mayor capitalización de mercado.
    * `etl_market_charts.py`: DAG que ejecuta el ETL de datos de la API de CoinGecko a Amazon Redshift; extrae los datos históricos del mercado que incluyen precio, capitalización de mercado y volumen de 24 horas de 5 tokens.
    * `trigger_bitcoin`: Este DAG utiliza un sensor SQL, que verifica si existe la tabla `market_charts` en Redshift, si existe, se ejecuta una consulta a la tabla para hacer el calculo de la tendencia de bitcoin en los ultimos 7 días y envia un email con la tendencia de bitcoin.
* `logs/`: Carpeta con los archivos de logs de Airflow.
* `plugins/`: Carpeta con los plugins de Airflow.
* `scripts/`: Carpeta con scripts SQL, Bash y Python.
    * `bash/`: Carpeta con scripts de bash.
    * `sql/`: Carpeta con scripts SQL.
        * `creacion_tabla.sql`: scripts de creación de tablas en Amazon Redshift.
    * `utils/`: Paquete de python compuesto por modulos para extraer, transformar y cargar datos.
        * `connection_spark.py`: Modulo con clase para crear una sesión de spark con conección a Amazon Redshift.
        * `load_redshift.py`: Modulo de carga de datos desde DataFrame de Spark a Data Warehouse de Amazon Redshift.
    * `ETL_top_tokens.py`: Script que contiene el proceso ETL que ejecuta el DAG `etl_top_tokens.py`.
    * `ETL_market_charts.py`: Script que contiene el proceso ETL que ejecuta el DAG `etl_market_charts.py`.



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
7. En la pestaña `Admin -> Variables` crear las siguientes variables con los siguientes datos:
    1. Variables para la ruta del Driver de Postgre:
        * Key: `driver_class_path`
        * Value: `/tmp/drivers/postgresql-42.5.2.jar`
    2. Variable para la ruta con los scripts para los DAGs que utilizan operadores de Spark:
        * Key: `spark_scripts_dir`
        * Value: `/opt/airflow/scripts`
8. Para el envío de Emails con SMTP, es necesario que la cuenta desde donde se va a enviar el email sea de tipo `gmail` y se genera una contraseña de aplicación. Posteriormente creamos las siguientes variables:
    1. Email de origen:
        * Key: `smtp_from`
        * Value: `tu_email@gmail.com`
    2. Contraseña de aplicación:    
        * Key: `smtp_password`
        * Value: `<contraseña de aplicación creada en gmail>`
    3. Email destinatario:
        * Key: `smtp_to`
        * Value: `el email al cual deseas enviar tus reportes diarios(puede ser @gmail, @yahoo o cualquier otro).`
9. Si no aparecen los dags una vez agregadas las conexiones y las variables, detener el contenedor con `ctrl + c` en la misma consola (si por alguna razon usaste un `docker-compose up -d` utiliza en comando `docker-compose down` dentro del mismo directorio) y ejecuta denuevo el comando `docker-compose up`.
10. Ejecutar los DAGs `etl_top_tokens` y `etl_market_chart`, si es la primera vez que ejecutas los dags, espera a que finalice el DAG `etl_market_chart` para poder ejecutar el DAG `trigger_bitcoin`.


