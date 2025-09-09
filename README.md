# Startup-Predictor-Project-II-

```
FROM apache/airflow:2.10.2

USER root

# Update and install dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl wget tar bash procps && \
    rm -rf /var/lib/apt/lists/*

# Download and extract Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.2-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-3.4.2-bin-hadoop3 /opt/spark && \
    rm spark-3.4.2-bin-hadoop3.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow
```
