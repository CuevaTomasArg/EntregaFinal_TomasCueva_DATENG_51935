# Error con el driver
---
El error se encuentra explicitamente en el notebooke- ETL-notebook.ipynb, el cual se produce al intentar subir el dataframe a la base de datos de redshift. Me asegure de tener las variables de entorno correctamente declaradas.

## Explico brevemente como lo estoy intentando ejecutar
Desarrolla un paquete de Python el cual es el directorio llamado "etl", el mismo tien modulos de extraccion, transformación y carga de datos.
Cada uno tiene sus clases Extract, Transform y Load. 

* El modulo "load_pyspark_redshift_connectr.py" es el que me esta trayendo el error.

En resumen, lo que hago en el notebook es:

1. Instalo las bibliotecas que voy a utilizar.
2. Importo las bibliotecas y los modulos del paquete etl.
3. Instancio las clases de los modulos del paquete etl.
4. Extraigo y transformo los datos utilizando pandas por ahora.
5. Ejecuto el metodo "execute" de la clase Load que cual llama al metodo load_to_redshift, el cual es el que tiene el error que estoy intentando corregir.

Averiguando, me di cuenta que es un error del driver que se encuentra en el directorio "/home/coder/working_dir/spark_drivers/postgresql-42.5.2.jar", el archivo "postgresql-42.5.2.jar" lo descargue del repositorio de la clase y lo puse en el directorio spark_drivers antes de ejecutar el "docker-compose up --build".

Todo esto lo estoy ejecutando desde el localhost:8888/lab?token=coder
