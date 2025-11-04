FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python ujson requests
COPY . /app
CMD ["python","writer.py"]
