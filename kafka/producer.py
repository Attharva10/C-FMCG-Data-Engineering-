import pandas as pd
import json
import time

from kafka import KafkaProducer


# ============================================
# CONFIGURATION
# ============================================

CSV_FILE = "data/transactions/transactions_source.csv"

KAFKA_SERVER = "localhost:9092"

TOPIC_NAME = "fmcg-sales"

BATCH_SIZE = 100

DELAY_SECONDS = 0.2


# ============================================
# CREATE KAFKA PRODUCER
# ============================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Kafka Producer connected successfully!")


# ============================================
# READ CSV
# ============================================

df = pd.read_csv(CSV_FILE)

print(f"Total records: {len(df):,}")


# ============================================
# SEND DATA TO KAFKA
# ============================================

for start in range(0, len(df), BATCH_SIZE):

    batch = df.iloc[start:start + BATCH_SIZE]

    for _, row in batch.iterrows():

        message = row.to_dict()

        producer.send(
            TOPIC_NAME,
            value=message
        )

    producer.flush()

    print(
        f"Sent records: "
        f"{min(start + BATCH_SIZE, len(df)):,} / {len(df):,}"
    )

    time.sleep(DELAY_SECONDS)


# ============================================
# CLOSE PRODUCER
# ============================================

producer.close()

print("====================================")
print("PRODUCER COMPLETED")
print("====================================")