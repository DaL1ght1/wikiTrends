FROM bitnami/spark:3.5
WORKDIR /app
COPY . /app
ENV PYTHONUNBUFFERED=1
CMD ["spark-submit", "/app/job.py"]