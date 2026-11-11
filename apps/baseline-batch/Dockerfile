FROM apache/spark:3.5.3
USER root
WORKDIR /app

ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/jars/

COPY job.py run.sh /app/
RUN chmod +x /app/run.sh
ENV PYTHONUNBUFFERED=1
CMD ["/app/run.sh"]
