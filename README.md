# S
```

FROM apache/airflow:2.10.2

USER root

# Set DEBIAN_FRONTEND=noninteractive to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Update and install dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-11-jdk \
       wget \
       curl \
       tar \
       bash \
       procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download and extract Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz \
    && tar -xzf spark-3.4.2-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-3.4.2-bin-hadoop3 /opt/spark \
    && rm spark-3.4.2-bin-hadoop3.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow
