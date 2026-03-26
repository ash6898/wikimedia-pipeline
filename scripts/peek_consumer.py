from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'peek-consumer-3',
    'auto.offset.reset': 'latest'
})

consumer.subscribe(['wiki-edits'])

try:
    count = 0
    while count < 5:
        msg = consumer.poll(timeout=5.0)
        if msg is None:
            continue
        data = json.loads(msg.value())
        print(json.dumps(data, indent=2))
        count += 1
finally:
    consumer.close()