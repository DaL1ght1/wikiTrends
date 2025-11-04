FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir sseclient-py kafka-python ujson requests
COPY . /app
ENV PYTHONUNBUFFERED=1
CMD ["python","app.py"]
