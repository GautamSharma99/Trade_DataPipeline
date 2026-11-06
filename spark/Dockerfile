FROM python:3.11-slim-bookworm

# Spark needs a JVM (Java 17 on bookworm)
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

RUN pip install --no-cache-dir pyspark==3.5.3

# Download the Spark <-> Kafka connector at build time so the job starts offline
ENV KAFKA_PKG=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
RUN python -c "from pyspark.sql import SparkSession as S; \
S.builder.master('local[1]').config('spark.jars.packages', '$KAFKA_PKG').getOrCreate().stop()"

WORKDIR /app
COPY stream_job.py batch_analysis.py ./
CMD ["python", "stream_job.py"]
