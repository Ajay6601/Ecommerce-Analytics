#!/bin/bash
airflow db init
airflow users create \
    --username admin \
    --password admin \
    --role Admin \
    --email admin@example.com \
    --firstname Admin \
    --lastname User
airflow webserver --hostname 0.0.0.0 --port 8080 &
airflow scheduler
