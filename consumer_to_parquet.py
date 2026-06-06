from confluent_kafka import Consumer
import pandas as pd
import json
import os
import time

os.makedirs("data_lake", exist_ok=True)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "parquet-sink",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["topic_fraud"])

buffer = []
batch_size = 5

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

    buffer.append(record)

    print("Buffered:", record)

    if len(buffer) >= batch_size:

        df = pd.DataFrame(buffer)

        filename = (
            f"data_lake/fraud_{int(time.time())}.parquet"
        )

        df.to_parquet(
            filename,
            engine="pyarrow"
        )

        print(
            f"Saved {len(buffer)} records to {filename}"
        )

        buffer.clear()