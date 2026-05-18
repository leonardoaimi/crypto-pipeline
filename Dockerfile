FROM python:3.11-slim

WORKDIR /app

# Install only ingestion dependencies — spark/dbt are not needed in this image.
# Pinned to the same versions as requirements.txt so local and container
# environments are consistent.
RUN pip install --no-cache-dir \
    "websockets>=12.0" \
    "httpx>=0.27.0" \
    "pydantic>=2.7.0" \
    "kafka-python>=2.0.2" \
    "pyyaml>=6.0.1"

COPY ingestion/ ./ingestion/
COPY config.yml .

CMD ["python", "-m", "ingestion.entrypoint"]
