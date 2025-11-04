FROM apache/spark:3.5.3
USER root
WORKDIR /app
COPY . /app
ENV PYTHONUNBUFFERED=1
# Pull Kafka connector + clients at runtime via --packages
CMD ["/opt/spark/bin/spark-submit","--master","spark://spark-master:7077","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.5.2","/app/main.py"]
