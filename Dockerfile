FROM python:3.9-slim-buster

RUN apt-get update && apt-get install -y wget gzip

RUN wget https://github.com/duckdb/duckdb/releases/download/v1.2.1/duckdb_cli-linux-arm64.gz  && \
    gzip -d duckdb_cli-linux-arm64.gz && \
    mv duckdb_cli-linux-arm64 /usr/local/bin/duckdb && \
    chmod +x /usr/local/bin/duckdb # Removed the 'rm duckdb_cli-linux-arm64.gz' line

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "app.py"]