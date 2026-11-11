FROM apache/spark:3.5.3
USER root
WORKDIR /app

# Kafka connector jars are baked into the image (instead of `--packages`) so
# startup doesn't depend on Maven Central. Versions match Spark 3.5.3.
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/jars/

COPY main.py /app/
ENV PYTHONUNBUFFERED=1 \
    SPARK_MASTER_URL=spark://spark-master:7077 \
    SPARK_CORES_MAX=2 \
    SPARK_EXECUTOR_MEMORY=1g

# --jars ships the connector to the executors on the Spark workers too.
CMD ["sh", "-c", "exec /opt/spark/bin/spark-submit --master \"$SPARK_MASTER_URL\" --conf spark.cores.max=$SPARK_CORES_MAX --conf spark.executor.memory=$SPARK_EXECUTOR_MEMORY --conf spark.driver.host=$(hostname -i) --jars $(ls /opt/jars/*.jar | tr '\\n' ',' | sed 's/,$//') /app/main.py"]
