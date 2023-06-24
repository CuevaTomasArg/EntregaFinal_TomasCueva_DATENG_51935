# Utiliza una imagen base de Jupyter Notebook
FROM jupyter/base-notebook:latest

# Instala las librerías adicionales
RUN pip install pandas requests

# Copia tus archivos .ipynb al directorio de trabajo
COPY *.ipynb /home/jovyan/work/

# Establece el directorio de trabajo
WORKDIR /home/jovyan/work
