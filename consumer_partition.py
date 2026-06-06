from confluent_kafka import Consumer, Producer, TopicPartition
import json

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "fraud-engine",
    "auto.offset.reset": "earliest"
})

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

consumer.assign([
    TopicPartition("topic_raw", 0)
])

print("Listening only to Partition 0")

while True:

    msg = consumer.poll(1)

    if msg is None:
        continue

    if msg.error():
        print(msg.error())
        continue

    record = json.loads(
        msg.value().decode()
    )

    print("Received:", record)

    if record["amount"] > 100000:

        producer.produce(
            "topic_fraud",
            json.dumps(record)
        )

        producer.flush()

        print("Fraud Alert:", record)