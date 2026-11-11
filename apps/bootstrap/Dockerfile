# One-shot setup jobs (stdlib only). The compose service picks the script:
#   opensearch-init  -> python opensearch_setup.py
#   dashboards-init  -> python dashboards_setup.py
FROM python:3.11-slim
WORKDIR /app
COPY *.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "opensearch_setup.py"]
