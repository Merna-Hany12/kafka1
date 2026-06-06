import io
import random
import time
from confluent_kafka import Producer
from fastavro import parse_schema, validate, schemaless_writer
producer = Producer({
    "bootstrap.servers": "localhost:9092"
})
schema = {
    "type": "record",
    "name": "Sale",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "item_name", "type": "string"},
        {"name": "price", "type": "float"}
    ]
}

parsed_schema = parse_schema(schema)
items = ["laptop", "phone", "mouse", "keyboard", "monitor"]
def send_to_kafka(data):
    buffer = io.BytesIO()
    if not validate(data, parsed_schema):
        raise ValueError(f"❌ Schema validation failed: {data}")
    schemaless_writer(buffer, parsed_schema, data)
    producer.produce(
        topic="sales_topic",
        value=buffer.getvalue()
    )
    producer.flush()
while True:
    '''# INVALID DATA TEST
    data = {
        "order_id": "ABC_NOT_AN_INT",   # WRONG TYPE
        "item_name": "Laptop",
        "price": 1200.50
    }'''
    data = {
        "order_id": random.randint(1, 10000),
        "item_name": random.choice(items),
        "price": round(random.uniform(10, 2000), 2)
    }
    send_to_kafka(data)
    print("Sent:", data)
    time.sleep(1)