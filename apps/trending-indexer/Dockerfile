FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir kafka-python opensearch-py ujson
ENV PYTHONUNBUFFERED=1
CMD ["python", "indexer.py"]
