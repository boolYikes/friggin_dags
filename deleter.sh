#!/bin/bash
docker rmi friggin_dags-airflow-triggerer:latest && \
docker rmi friggin_dags-airflow-worker:latest && \
docker rmi friggin_dags-airflow-scheduler:latest && \
docker rmi friggin_dags-airflow-init:latest && \
docker rmi friggin_dags-airflow-webserver:latest
