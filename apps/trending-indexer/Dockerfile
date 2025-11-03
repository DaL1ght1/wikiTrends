FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python opensearch-py ujson
COPY . /app
ENV PYTHONUNBUFFERED=1
CMD ["python", "indexer.py"]