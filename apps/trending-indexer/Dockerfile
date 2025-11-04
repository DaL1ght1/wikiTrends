# apps/stream-aggregator/Dockerfile
FROM apache/spark:3.5.3
USER root
RUN pip install --no-cache-dir pyspark==3.5.3
WORKDIR /app
COPY . /app
ENV PYTHONUNBUFFERED=1
# IMPORTANT: run on your cluster, not local
CMD ["/opt/spark/bin/spark-submit","--master","spark://spark-master:7077","--conf","spark.sql.shuffle.partitions=4","--conf","spark.default.parallelism=4","--conf","spark.sql.streaming.stateStore.maintenanceInterval=10s","/app/main.py"]
