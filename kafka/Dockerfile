FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker-cli \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pandas kafka-python

WORKDIR /app

COPY consumer_hdfs.py /app/consumer_hdfs.py
COPY consumer_recovery.py /app/consumer_recovery.py

CMD ["python", "/app/consumer_hdfs.py"]
