# Entrega 1 - consumo de API y creación de table en Redshift
---
En esta carpeta se encuentran los archivos correspondientes a la entrega 1, creacion de tabla en redshift y lectura de JSON extraido de una API con sus trasformaciones de datos correspondientes con el uso de Pandas. Dentro de este directorio encontraremos los siguiente archivos y carpetas relevantes:

* DB/ : Dentro se encontraran los datos extraidos de la API de CoinGecko

* Dockerfile: Este Dockerfile construirá una imagen Docker basada en Jupyter Notebook que incluirá las librerías Pandas y Requests instaladas, copiará todos los archivos .ipynb desde el contexto de construcción al directorio de trabajo dentro de la imagen y establecerá ese directorio como el directorio de trabajo predeterminado. Esto proporciona un entorno listo para ejecutar y trabajar con archivos Jupyter Notebook que utilizan las librerías mencionadas.


## Configuracion del entorno

1. construir la imagen
```bash
docker build -t jupyter-env .
```
2. Ejecutar el contenedor
```bash
docker run -p 8888:8888 -v <ruta_absoluta_al_directorio_local>:/home/jovyan/work jupyter-env
```

```bash
#En mi caso que uso windows 
docker run -p 8888:8888 -v "C:\\Users\tomas\OneDrive\Escritorio\DataEngineer\ProyectoFinalCoderhouse\entrega1:/home/jovyan/work" jupyter-env
```
3. Luego de ejecutar el comando, copia la url entera que imprime al final, debe ser algo como esto: "http://127.0.0.1:8888/lab?token=c0816829b0", luego pegala en tu navegador, o si utilizas VS code, va a "seleccionar kernel", escoge la opcion "Servidor de Jupyter existente..." y pega la url cuando te lo solicite. Con esto ya podras ejecutar los bloques de codigo de los notebooks de este repositorio 
