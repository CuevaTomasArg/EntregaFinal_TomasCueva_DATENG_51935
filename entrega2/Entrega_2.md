# Entrega 2 - ETL de API de CoinGecko a tablas de Amazon Redshift
---
Dentro de este directorio encontraremos los siguientes archivos y carpetas:
* docker_shared_folder/: donde se alojaran los volumenes de los contenedores
    * data/: se alojaran datos de prueba
    * postgres_data/: se aloja el volumen de la imagen de PostgreSQL
    * scripts/: Dentro encontraremos querys y scripts de python.
        * sql/: Query para la creación de tablas con la sintaxis de Amazon Redshift.
        * python/: Los scripts bases de ETL para extraer datos de la API de CoinGecko, transformarlos y cargarlos dentro de amazon redshift que se utilizaran dentro del directorio working_dir de la carpeta docker_shared_folder.
    * working_dir/: notebooks ejecutaremos los codigos provenientes de los scripts ETL mencionados anteriormente
    * .env: Este archivo contiene variables de entorno para podedr conectarnos con redshift y poder realizar operaciones de carga y creacion de tablas
* docker-compose.yml: Crear contenedor de Dcker
* Dockerfile.python: Con el se creara la imagen de python dentro del archivo docker-compose.yml.

## Configuración de entorno
1. Nos posicionamos dentro de este direcorio "./entrega2"
2. Ejecutamos:
```bash
# La primera vez
docker-compose up --build

# En la segunda vez que se ejecuta ya alcanza con
docker-compose up --build
# o si no queremos ver los logs
docker-compose up -d
```

3. Una vez terminado, nos conectamos a nuestra base de datos en AWS desde notebooks correspondientes
