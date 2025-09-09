```
FROM apache/airflow:2.10.2

USER root
ENV DEBIAN_FRONTEND=noninteractive

# Install Java + wget + curl
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-11-jdk-headless wget curl tar bash && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz && tar -xzf spark-3.4.2-bin-hadoop3.tgz -C /opt/ && mv /opt/spark-3.4.2-bin-hadoop3 /opt/spark && rm spark-3.4.2-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow
```
```
version: '3'

services:
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data

  airflow-init:
    image: puckel/docker-airflow:1.10.9
    depends_on:
      - postgres
    environment:
      - EXECUTOR=Local
    volumes:
      - ./dags:/usr/local/airflow/dags
    entrypoint: >
      /bin/bash -c "airflow initdb && airflow create_user -r Admin -u admin -p admin -e admin@example.com -f Air -l Flow"

  webserver:
    image: puckel/docker-airflow:1.10.9
    restart: always
    depends_on:
      - postgres
      - airflow-init
    environment:
      - EXECUTOR=Local
    volumes:
      - ./dags:/usr/local/airflow/dags
    ports:
      - "8080:8080"
    command: webserver

  scheduler:
    image: puckel/docker-airflow:1.10.9
    restart: always
    depends_on:
      - webserver
      - postgres
    environment:
      - EXECUTOR=Local
    volumes:
      - ./dags:/usr/local/airflow/dags
    command: scheduler

  spark:
    image: bitnami/spark:3.4.2
    environment:
      - SPARK_MODE=master
    volumes:
      - ./dags:/opt/airflow/dags
    ports:
      - "7077:7077"
      - "8081:8080"

volumes:
  postgres-db-volume:
```

```
version: '3'

services:
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data

  airflow-init:
    image: apache/airflow:2.10.2
    depends_on:
      - postgres
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
    entrypoint: >
      /bin/bash -c "airflow db init &&
                    airflow users create --username admin --password admin --firstname Air --lastname Flow --role Admin --email admin@example.com"

  webserver:
    image: apache/airflow:2.10.2
    depends_on:
      - postgres
      - airflow-init
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
    ports:
      - "8080:8080"
    command: webserver

  scheduler:
    image: apache/airflow:2.10.2
    depends_on:
      - webserver
      - postgres
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
    command: scheduler

  spark:
    image: bitnami/spark:3.4.2
    environment:
      - SPARK_MODE=master
    volumes:
      - ./dags:/opt/airflow/dags
    ports:
      - "7077:7077"
      - "8081:8080"

volumes:
  postgres-db-volume:
```
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

ingest_task = SparkSubmitOperator(
    task_id='run_spark_ingestion',
    application='/opt/airflow/dags/airline-delay-ingestion.jar',
    java_class='com.spark.DataIngestion',
    conn_id='spark_default',
    application_args=[
        '/opt/airflow/dags/Airline_Delay_Cause.csv',
        '/opt/airflow/dags/output/ingested_data'
    ],
    dag=dag
)

