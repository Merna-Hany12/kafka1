from confluent_kafka import Producer
import json
import uuid
import random
import time
producer = Producer({
    "bootstrap.servers": "localhost:9092"
})
locations = ["Cairo", "Alexandria"]
while True:
    
    location = random.choice(locations)
    record = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 100),
        "amount": random.randint(1000, 200000),
        "location": location
    }
    partition = 0 if location == "Cairo" else 1
    producer.produce(
        topic="topic_raw",
        value=json.dumps(record),
        partition=partition
    )

    producer.flush()
    print(
        f"Sent -> Partition {partition}: {record}"
    )
    time.sleep(1)