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
