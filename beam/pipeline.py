import json
import time
import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2
import os
from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
INPUT_TOPIC = 'wiki-edits'
OUTPUT_TOPIC = 'wiki-counts'
POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_CONN = f'host=localhost port=5432 dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD}'
WINDOW_SIZE = 60


class ParseEdit(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode('utf-8'))
            if record.get('namespace') != 0:
                return
            yield (record['wiki'], record)
        except Exception:
            return


class WriteToPostgres(beam.DoFn):
    def process(self, element, window_start, window_end):
        wiki, count = element

        conn = psycopg2.connect(POSTGRES_CONN)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO wiki_window_counts (wiki, window_start, window_end, edit_count)
               VALUES (%s, %s, %s, %s)""",
            (wiki, window_start, window_end, count)
        )
        conn.commit()
        cur.close()
        conn.close()


def collect_batch(consumer, window_seconds):
    messages = []
    window_start = datetime.datetime.now(datetime.timezone.utc)
    deadline = time.time() + window_seconds

    print(f"Collecting window starting at {window_start.isoformat()} ...")

    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        messages.append(msg.value())

    window_end = datetime.datetime.now(datetime.timezone.utc)
    print(f"Window closed. Collected {len(messages)} messages.")
    return messages, window_start, window_end


def run_batch(messages, window_start, window_end):
    if not messages:
        print("No messages in window, skipping.")
        return

    options = PipelineOptions(['--runner=DirectRunner'])

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Create' >> beam.Create(messages)
            | 'ParseEdit' >> beam.ParDo(ParseEdit())
            | 'CountPerWiki' >> beam.combiners.Count.PerKey()
            | 'WriteToPostgres' >> beam.ParDo(WriteToPostgres(),
                                               window_start=window_start,
                                               window_end=window_end)
        )


def main():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'beam-pipeline',
        'auto.offset.reset': 'latest',
    })
    consumer.subscribe([INPUT_TOPIC])
    print("Pipeline started. Collecting 60s windows...")

    try:
        while True:
            messages, window_start, window_end = collect_batch(consumer, WINDOW_SIZE)
            run_batch(messages, window_start, window_end)
    except KeyboardInterrupt:
        print("Shutting down.")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
