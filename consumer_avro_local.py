import io
from confluent_kafka import Consumer
from fastavro import parse_schema, schemaless_reader

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
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "avro-local-group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["sales_topic"])
print("Consuming raw Avro bytes...\n")
while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Error:", msg.error())
        continue
    buffer = io.BytesIO(msg.value())
    record = schemaless_reader(buffer, parsed_schema)

    print("Decoded:", record)