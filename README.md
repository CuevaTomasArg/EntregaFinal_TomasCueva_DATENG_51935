# 51940-CuevaTomas
---
Dentro de este repositorio se encuentra el proceso del proyecto final del curso "Data Engineering flex" de Coderhouse.

## Contenido
* docker-compose.yml: Este archivo contiene los servicios utilizados para la ejecución del proyecto:
    * Servicio pyspark:
        * Utiliza la imagen jupyter/pyspark-notebook:2023-04-24.
        * El contenedor se nombra como "sem7-pyspark".
        * Mapea el puerto local 8888 al puerto 8888 dentro del contenedor.
        * Comparte el directorio local ./docker_shared_folder/working_dir con el directorio /home/coder/working_dir dentro del contenedor.
        * Utiliza el archivo .env ubicado en ./docker_shared_folder para cargar las variables de entorno.
        * Configura las variables de entorno NB_USER, NB_GID, CHOWN_HOME, CHOWN_HOME_OPTS y SPARK_CLASSPATH.
        * Se asigna a la red sem_7_net con una dirección IP específica.

    * Servicio postgres:
        * Utiliza la imagen oficial de Postgres versión 15.
        * El contenedor se nombra como "sem7-postgres-db".
        * Mapea el puerto local 5435 al puerto 5435 dentro del contenedor.
        * Comparte el directorio local ./docker_shared_folder/postgres_data con el directorio /var/lib/postgresql/data dentro del contenedor.
        * Configura el comando -p 5435 para especificar el puerto de escucha de Postgres.
        * Utiliza el archivo .env ubicado en ./docker_shared_folder para cargar las variables de entorno.
        * Se asigna a la red sem_7_net con una dirección IP específica.
        * Además, se define una red llamada sem_7_net con la configuración IPAM (IP Address Management) que utiliza el controlador predeterminado y asigna una subred 172.7.7.0/16.

* docker_shared_folder/: Contiene los siguientes directorios y archivos.
    * working_dir: Es el directorio que contiene el notebook, scripts y modulo necesarios para la ejecución del proyecto.
        * etl/: Es un modulo de python el cual contiene las clases necesarias para ejecutar realizar un ETL a través de las clases Extract, Transform y Load, las cuales se encuentran en los respectivos archivos.
        * scripts/: Directorio con scripts de python y sql.
        * spark_drivers/: contiene el driver para conectarse a la base de datos de Redshift mediante el uso de Spark.
        * ETL-notebook.ipynb: El notebook principal el cual importa las bibliotecas Pandas, Requests, Numpy , SQLAlchemy y psycopg2-binary junto con el paquete etl que se encuentra dentro de la misma altura que este archivo.
    * .env: Archivo con variables de entorno.

## Creación del entorno
1. Para crear el entorno es necesario tener instalada Docker Desktop (https://www.docker.com/products/docker-desktop).
2. Creamos el "directorio postgres_data", dentro del directorio docker_shared_folder.
3. Posicionados en el directorio raiz del proyecto ejecutamos:
```bash
  docker-compose up --build
```
4. Para acceder a JupyterLab ingresar a http://localhost:8888/lab?token=coder .


