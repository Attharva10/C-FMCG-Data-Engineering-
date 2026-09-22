from kafka import KafkaConsumer
import json


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "fmcg-sales"


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fmcg-test-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)


print("Kafka Consumer started...")
print("Reading messages from Kafka...\n")


count = 0

for message in consumer:

    print(message.value)

    count += 1

    if count >= 20:
        break


consumer.close()

print("\n================================")
print("CONSUMER TEST COMPLETE")
print("Messages received:", count)
print("================================")