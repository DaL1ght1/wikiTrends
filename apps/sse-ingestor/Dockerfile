FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py config.yml ./
ENV PYTHONUNBUFFERED=1
VOLUME ["/state"]
CMD ["python", "app.py"]
