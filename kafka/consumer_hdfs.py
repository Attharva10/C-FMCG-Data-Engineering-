import json
import subprocess
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer


# ============================================
# CONFIGURATION
# ============================================

KAFKA_SERVER = "kafka:29092"
TOPIC_NAME = "fmcg-sales"
GROUP_ID = "fmcg-airflow"

EXPECTED_RECORDS = 1_100_000
BATCH_SIZE = 10_000

HDFS_PATH = "/fmcg/raw/transactions"

LOCAL_BATCH_DIR = Path("/opt/airflow/hdfs_staging")
LOCAL_BATCH_DIR.mkdir(parents=True, exist_ok=True)


# ============================================
# KAFKA CONSUMER
# ============================================

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    consumer_timeout_ms=10000,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=GROUP_ID,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

print("========================================")
print("KAFKA → HDFS INGESTION STARTED")
print("========================================")
print(f"Topic          : {TOPIC_NAME}")
print(f"Expected rows  : {EXPECTED_RECORDS:,}")
print(f"Batch size     : {BATCH_SIZE:,}")
print(f"HDFS location  : {HDFS_PATH}")
print()


# ============================================
# HDFS UPLOAD FUNCTION
# ============================================

def upload_to_hdfs(local_file: Path):

    container_file = f"/tmp/{local_file.name}"

    subprocess.run(
        [
            "docker",
            "cp",
            str(local_file),
            f"fmcg-namenode:{container_file}",
        ],
        check=True,
    )

    subprocess.run(
        [
            "docker",
            "exec",
            "fmcg-namenode",
            "hdfs",
            "dfs",
            "-put",
            "-f",
            container_file,
            HDFS_PATH,
        ],
        check=True,
    )


# ============================================
# MAIN INGESTION LOOP
# ============================================

batch = []
batch_number = 1
total_received = 0


try:

    for message in consumer:

        batch.append(message.value)
        total_received += 1

        # When batch reaches 10,000
        if len(batch) >= BATCH_SIZE:

            df = pd.DataFrame(batch)

            local_file = (
                LOCAL_BATCH_DIR
                / f"transactions_batch_{batch_number:05d}.csv"
            )

            df.to_csv(
                local_file,
                index=False
            )

            upload_to_hdfs(local_file)

            consumer.commit()

            print(
                f"Batch {batch_number:03d} | "
                f"Rows: {len(batch):,} | "
                f"Total: {total_received:,} / "
                f"{EXPECTED_RECORDS:,}"
            )

            batch.clear()
            batch_number += 1

            # Remove local staging file
            local_file.unlink(missing_ok=True)

        # Stop after expected number of rows
        if total_received >= EXPECTED_RECORDS:
            break


    # ========================================
    # FINAL PARTIAL BATCH
    # ========================================

    if batch:

        df = pd.DataFrame(batch)

        local_file = (
            LOCAL_BATCH_DIR
            / f"transactions_batch_{batch_number:05d}.csv"
        )

        df.to_csv(
            local_file,
            index=False
        )

        upload_to_hdfs(local_file)

        consumer.commit()

        print(
            f"Final batch {batch_number:03d} | "
            f"Rows: {len(batch):,}"
        )

        local_file.unlink(missing_ok=True)


finally:

    consumer.close()


# ============================================
# COMPLETION
# ============================================

print()
print("========================================")
print("KAFKA → HDFS INGESTION COMPLETE")
print("========================================")
print(f"Total records received: {total_received:,}")
print(f"Expected records      : {EXPECTED_RECORDS:,}")
print("========================================")