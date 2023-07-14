FROM jupyter/pyspark-notebook:2023-04-24

USER root

RUN pip install --upgrade pip
RUN pip install jupyterlab==3.1.12 notebook spylon-kernel
RUN python -m spylon_kernel install
