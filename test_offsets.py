from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "earliest-test", #change this (earliest-test  atest-test)
    "auto.offset.reset": "earliest"#change this (earliest-latest)
})

consumer.subscribe(["topic_raw"])

while True:
    msg = consumer.poll(1)

    if msg:
        print(msg.value().decode())