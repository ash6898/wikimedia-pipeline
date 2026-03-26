import json
import signal
import time

import sseclient
import urllib.request
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'wiki-edits'
WIKIMEDIA_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
msg_count = 0
running = True

def shutdown(signum, frame):
    global running
    print(f"Shutting down gracefully...")
    running = False

signal.signal(signal.SIGINT, shutdown)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def connect_stream():
    request = urllib.request.Request(
        WIKIMEDIA_URL,
        headers={'User-Agent': 'wikimedia_producer/1.0 (learning project)'}
    )
    response = urllib.request.urlopen(request)
    return sseclient.SSEClient(response)
    
def main():
    global msg_count
    backoff = 1

    while running:
        try:
            for event in connect_stream().events():
                if not running:
                    break
                if event.event != 'message' or not event.data:
                    continue

                data = json.loads(event.data)

                if data.get('type') != 'edit':
                    continue

                payload = {
                    'wiki': data.get('wiki'),
                    'user': data.get('user'),
                    'title': data.get('title'),
                    'bot': data.get('bot'),
                    'timestamp': data.get('timestamp'),
                    'minor': data.get('minor'),
                    'namespace': data.get('namespace'),
                }

                producer.produce(
                    topic=TOPIC,
                    key=payload['wiki'],
                    value=json.dumps(payload),
                    callback=delivery_report,
                )

                msg_count += 1
                if msg_count % 100 == 0:
                    producer.flush(timeout=5)
                    print(f"Produced {msg_count} messages so far...")

            backoff = 1  # Reset backoff after successful connection
        except Exception as e:
            print(f'stream error: {e}. Retrying in {backoff} seconds...')
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)  # Exponential backoff with maximum of 60 seconds

    producer.flush(timeout=5)
    print(f"Shutdown complete. Total messages produced: {msg_count}")


if __name__ == "__main__":
    main()