FROM apache/airflow:2.8.1

USER root
RUN apt-get update && \
    apt-get install -y dos2unix

COPY entrypoint.sh /entrypoint.sh
RUN dos2unix /entrypoint.sh && \
    chmod +x /entrypoint.sh  # Fix line endings + permissions [3][5]

USER airflow
ENTRYPOINT ["/entrypoint.sh"]
