FROM bitnami/spark:3.5
WORKDIR /app
# If you need extra Py libs inside Spark executors, add them here
COPY . /app
ENV PYTHONUNBUFFERED=1
# Expect your submit.sh to call spark-submit with the right args
CMD ["/bin/bash", "submit.sh"]