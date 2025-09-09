https://chatgpt.com/s/t_68c086d2cfd88191bed949e1ca29d54e

```
package com.spark.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class AirlineDay3Silver {
    public static void main(String[] args) {
        String inputCsv = args.length > 0 ? args[0] : "data/Airline_Delay_Cause.csv";
        String outputParquet = args.length > 1 ? args[1] : "data/silver/Airline_Delay_Cause_Silver.parquet";

        SparkSession spark = SparkSession.builder()
                .appName("Airline Bronze to Silver Cleaning")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("nullValue", "")
                .csv(inputCsv);

        // === Basic Cleanup ===
        Dataset<Row> cleaned = df
                // Trim + uppercase categorical fields
                .withColumn("carrier", upper(trim(col("carrier"))))
                .withColumn("airport", upper(trim(col("airport"))))
                // Replace null indicators with 0
                .withColumn("arr_del15", coalesce(col("arr_del15"), lit(0)))
                .withColumn("arr_cancelled", coalesce(col("arr_cancelled"), lit(0)))
                .withColumn("arr_diverted", coalesce(col("arr_diverted"), lit(0)))
                // Ensure arr_flights is not null (if all cts are zero, default to 0)
                .withColumn("arr_flights",
                        when(col("arr_flights").isNull()
                                .and(coalesce(col("carrier_ct"), lit(0))
                                        .plus(coalesce(col("weather_ct"), lit(0)))
                                        .plus(coalesce(col("nas_ct"), lit(0)))
                                        .plus(coalesce(col("security_ct"), lit(0)))
                                        .plus(coalesce(col("late_aircraft_ct"), lit(0))).equalTo(0)),
                                lit(0))
                                .otherwise(coalesce(col("arr_flights"), lit(0))))
                // Round *_ct columns
                .withColumn("carrier_ct", coalesce(round(col("carrier_ct")), lit(0)).cast("int"))
                .withColumn("weather_ct", coalesce(round(col("weather_ct")), lit(0)).cast("int"))
                .withColumn("nas_ct", coalesce(round(col("nas_ct")), lit(0)).cast("int"))
                .withColumn("security_ct", coalesce(round(col("security_ct")), lit(0)).cast("int"))
                .withColumn("late_aircraft_ct", coalesce(round(col("late_aircraft_ct")), lit(0)).cast("int"))
                // Delay fields → default 0
                .withColumn("arr_delay", coalesce(col("arr_delay"), lit(0)))
                .withColumn("carrier_delay", coalesce(col("carrier_delay"), lit(0)))
                .withColumn("weather_delay", coalesce(col("weather_delay"), lit(0)))
                .withColumn("nas_delay", coalesce(col("nas_delay"), lit(0)))
                .withColumn("security_delay", coalesce(col("security_delay"), lit(0)))
                .withColumn("late_aircraft_delay", coalesce(col("late_aircraft_delay"), lit(0)));

        // === Data Quality Flags ===
        cleaned = cleaned
                .withColumn("data_quality_flag",
                        when(df.columns().length == 0, lit("EMPTY_ROW"))
                                .when(col("year").isNull().or(col("month").isNull()), lit("MISSING_DATE"))
                                .when(col("carrier").isNull().or(col("airport").isNull()), lit("MISSING_ID"))
                                .when(col("arr_flights").lt(0), lit("BAD_FLIGHT_COUNT"))
                                .when(col("arr_delay").lt(-60), lit("BAD_DELAY"))
                                .otherwise(lit("OK"))
                );

        // Save Silver Layer
        cleaned.write().mode("overwrite").parquet(outputParquet);
        spark.stop();
    }
}

```
# new setup airflow

```
FROM apache/airflow:2.9.3-python3.11   # latest stable in 2025

USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Install only required extras (NOT apache-airflow itself again)
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.10.0


```

```
version: '3.9'

x-spark-common: &spark-common
  image: bitnami/spark:3.5.6   # matches your Maven Spark version
  environment:
    - SPARK_MODE=standalone
  volumes:
    - ./jobs:/opt/bitnami/spark/jobs   # mount .jar jobs
  networks:
    - code-with-yu

x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  env_file:
    - airflow.env
  volumes:
    - ./jobs:/opt/airflow/jobs
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
  depends_on:
    - postgres
  networks:
    - code-with-yu

services:
  spark-master:
    <<: *spark-common
    command: bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "8081:8080"   # Spark WebUI
      - "7077:7077"   # Spark master port

  spark-worker:
    <<: *spark-common
    command: bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    environment:
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g
      - SPARK_MASTER_URL=spark://spark-master:7077

  postgres:
    image: postgres:15.3
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    networks:
      - code-with-yu

  webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      - scheduler

  scheduler:
    <<: *airflow-common
    command: bash -c "airflow db migrate && \
                      airflow users create --username admin --firstname Vidhi --lastname Jaiswal \
                        --role Admin --email admin@example.com --password admin || true && \
                      airflow scheduler"

networks:
  code-with-yu:

```

```
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__WEBSERVER__SECRET_KEY=supersecretkey2025
AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8080

```

















# previous setup 
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

```
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Default args
default_args = {
    'owner': 'vidhi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='daily_spark_ingestion',
    default_args=default_args,
    description='Run daily Spark ingestion job (Java JAR)',
    start_date=datetime(2025, 9, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Spark Submit Task (must be indented)
    ingest_task = SparkSubmitOperator(
        task_id='run_spark_ingestion',
        application='/opt/airflow/dags/airline-delay-ingestion.jar',
        java_class='com.spark.DataIngestion',
        conn_id='spark_default',
        application_args=[
            '/opt/airflow/dags/Airline_Delay_Cause.csv',
            '/opt/airflow/dags/output/ingested_data'
        ],
        verbose=True
    )

    ingest_task
```
