FROM jupyter/pyspark-notebook:latest

USER root

RUN pip install --upgrade pip

COPY ./requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
