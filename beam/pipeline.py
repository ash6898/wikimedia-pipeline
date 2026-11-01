"""Streaming per-wiki edit counts with Beam event-time windows.

Reads article edits from the Kafka topic ``wiki-edits``, assigns each edit the
Wikimedia event timestamp, and aggregates with Beam ``FixedWindows`` of 60
seconds (epoch-aligned). When the watermark passes a window end, the per-wiki
count is upserted into PostgreSQL ``wiki_window_counts``.

Runner: DirectRunner (single process). Beam's Python ``ReadFromKafka`` is a
cross-language transform and is not supported on DirectRunner, so a splittable
DoFn polls Kafka with ``confluent-kafka`` and emits timestamped elements.
Windowing, triggering, and combining are Beam's, not a wall-clock sleep loop
around a batch pipeline.

Watermark: each read cycle advances the watermark to
``(max event timestamp observed on earlier cycles) - WATERMARK_SLACK_SECONDS``.
A window is written once that watermark passes the window end. That happens
after an edit at least ``WATERMARK_SLACK_SECONDS`` past the window has been
read and the following read cycle applies the watermark. Until then the
window stays open, and out-of-order edits that arrive while it is open are
counted. The window that contains the newest edit stays unwritten. After a
window fires, allowed lateness is zero, so later edits for that window are
dropped.
"""

import json
import logging
import os
import time

import apache_beam as beam
import psycopg2
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import WatermarkEstimator
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.core import RestrictionProvider
from apache_beam.transforms.core import WatermarkEstimatorProvider
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

try:
    from dotenv import load_dotenv
except ImportError:  # Spark/Docker images may not install python-dotenv.
    load_dotenv = None

WINDOW_SIZE_SECONDS = 60
WATERMARK_SLACK_SECONDS = 15
_KAFKA_READ_SECONDS = 1.0
_UNBOUNDED_OFFSETS = 2**62

UPSERT_WINDOW_COUNT_SQL = """
INSERT INTO wiki_window_counts (wiki, window_start, window_end, edit_count)
VALUES (%s, %s, %s, %s)
ON CONFLICT (wiki, window_start)
DO UPDATE SET
    edit_count = EXCLUDED.edit_count,
    window_end = EXCLUDED.window_end,
    inserted_at = NOW()
"""

_CONSUMERS = {}


def _env(name, default):
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def postgres_dsn():
    if load_dotenv is not None:
        load_dotenv()
    return (
        f"host={_env('POSTGRES_HOST', 'localhost')} "
        f"port={_env('POSTGRES_PORT', '5432')} "
        f"dbname={_env('POSTGRES_DB', 'wikimedia')} "
        f"user={_env('POSTGRES_USER', 'wikimedia')} "
        f"password={_env('POSTGRES_PASSWORD', 'wikimedia')} "
        "connect_timeout=10"
    )


def parse_edit_record(raw):
    """Return ``(wiki, event_timestamp)`` for a namespace-0 edit, else None."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        return None
    try:
        record = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(record, dict):
        return None
    if record.get("namespace") != 0:
        return None
    wiki = record.get("wiki")
    timestamp = record.get("timestamp")
    if not isinstance(wiki, str) or wiki == "":
        return None
    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
        return None
    return wiki, float(timestamp)


def window_bounds(window):
    """UTC start (inclusive) and end (exclusive) of a Beam interval window."""
    return (
        window.start.to_utc_datetime(has_tz=True),
        window.end.to_utc_datetime(has_tz=True),
    )


def count_edits_per_wiki_window(edits):
    """Count timestamped wiki names into epoch-aligned 60-second windows.

    ``edits`` is a PCollection of wiki-name strings whose element timestamps
    are the Wikimedia event times.
    """
    return (
        edits
        | "PairWithOne" >> beam.Map(lambda wiki: (wiki, 1))
        | "FixedWindow60s" >> beam.WindowInto(
            beam.window.FixedWindows(WINDOW_SIZE_SECONDS),
            trigger=AfterWatermark(),
            accumulation_mode=AccumulationMode.DISCARDING,
            allowed_lateness=0,
        )
        | "CountPerWiki" >> beam.combiners.Count.PerKey()
    )


class _EventTimeWatermarkEstimator(WatermarkEstimator):
    """Watermark trails the newest event time seen on earlier read cycles."""

    def __init__(self, state):
        self.watermark = None
        self.max_event_ts = None
        if state is not None:
            watermark, self.max_event_ts = state
            if watermark is not None:
                self.watermark = Timestamp(watermark)

    def observe_timestamp(self, timestamp):
        event_ts = float(Timestamp.of(timestamp))
        if self.max_event_ts is None or event_ts > self.max_event_ts:
            self.max_event_ts = event_ts

    def current_watermark(self):
        if self.watermark is None:
            return MIN_TIMESTAMP
        return self.watermark

    def get_estimator_state(self):
        watermark = None if self.watermark is None else float(self.watermark)
        return (watermark, self.max_event_ts)

    def advance_slack(self, slack_seconds):
        if self.max_event_ts is None:
            return
        candidate = Timestamp(self.max_event_ts - slack_seconds)
        if self.watermark is None or candidate > self.watermark:
            self.watermark = candidate

    def advance_to(self, timestamp):
        candidate = Timestamp(timestamp)
        if self.watermark is None or candidate > self.watermark:
            self.watermark = candidate

    def get_max_event_ts(self):
        return self.max_event_ts


class _EventTimeWatermarkEstimatorProvider(WatermarkEstimatorProvider):
    def initial_estimator_state(self, element, restriction):
        return None

    def create_watermark_estimator(self, estimator_state):
        return _EventTimeWatermarkEstimator(estimator_state)


class _CheckpointOnlyTracker(OffsetRestrictionTracker):
    """Allow self-checkpoints (fraction 0) and refuse parallel splits.

    One reader owns the Kafka consumer. A second split would hold the
    watermark at the beginning of time and the windows would never close.
    """

    def try_split(self, fraction_of_remainder):
        if fraction_of_remainder:
            return None
        return super().try_split(fraction_of_remainder)


class _ReadRestrictionProvider(RestrictionProvider):
    def initial_restriction(self, element):
        kind, stop = element
        if kind == "memory":
            return OffsetRange(0, int(stop))
        return OffsetRange(0, _UNBOUNDED_OFFSETS)

    def create_tracker(self, restriction):
        return _CheckpointOnlyTracker(restriction)

    def restriction_size(self, element, restriction):
        kind, _ = element
        if kind == "memory":
            return max(0, restriction.stop - restriction.start)
        return 1


class ReadArticleEdits(beam.DoFn):
    """Unbounded source of ``(wiki name)`` elements at Wikimedia event time.

    Pass ``records`` (JSON strings or bytes) for an in-memory finite source.
    The restriction is one element longer than ``records`` so the final
    invocation can move the watermark past the last window. Leave ``records``
    unset to poll Kafka until the pipeline is stopped.
    """

    def __init__(
        self,
        bootstrap_servers,
        topic,
        group_id,
        offset_reset,
        records=None,
        batch_size=500,
        slack_seconds=WATERMARK_SLACK_SECONDS,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.offset_reset = offset_reset
        self.records = records
        self.batch_size = batch_size
        self.slack_seconds = slack_seconds

    def _consumer_key(self):
        return (self.bootstrap_servers, self.group_id, self.topic)

    def _kafka_consumer(self):
        key = self._consumer_key()
        consumer = _CONSUMERS.get(key)
        if consumer is None:
            from confluent_kafka import Consumer

            consumer = Consumer({
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": self.offset_reset,
                "enable.auto.commit": True,
            })
            consumer.subscribe([self.topic])
            _CONSUMERS[key] = consumer
        return consumer

    def _poll_kafka(self):
        from confluent_kafka import KafkaError

        consumer = self._kafka_consumer()
        messages = []
        deadline = time.time() + _KAFKA_READ_SECONDS
        while time.time() < deadline and len(messages) < self.batch_size:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            error = msg.error()
            if error is not None:
                if error.code() != KafkaError._PARTITION_EOF:
                    logging.warning("kafka read: %s", error)
                continue
            messages.append(msg.value())
        return messages

    def teardown(self):
        consumer = _CONSUMERS.pop(self._consumer_key(), None)
        if consumer is not None:
            consumer.close()

    @beam.DoFn.unbounded_per_element()
    def process(
        self,
        element,
        tracker=beam.DoFn.RestrictionParam(_ReadRestrictionProvider()),
        watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
            _EventTimeWatermarkEstimatorProvider()),
    ):
        watermark_estimator.advance_slack(self.slack_seconds)
        start = tracker.current_restriction().start

        if self.records is not None and start >= len(self.records):
            if tracker.try_claim(start):
                max_event_ts = watermark_estimator.get_max_event_ts()
                if max_event_ts is not None:
                    watermark_estimator.advance_to(
                        max_event_ts + WINDOW_SIZE_SECONDS + 1)
            return

        if self.records is not None:
            end = min(start + self.batch_size, len(self.records))
            batch = list(self.records[start:end])
        else:
            batch = self._poll_kafka()

        if not batch:
            tracker.defer_remainder(Duration(seconds=0.2))
            return

        index = start
        article_edits = 0
        for raw in batch:
            if not tracker.try_claim(index):
                return
            index += 1
            parsed = parse_edit_record(raw)
            if parsed is None:
                continue
            wiki, event_ts = parsed
            article_edits += 1
            yield TimestampedValue(wiki, event_ts)

        logging.info(
            "read cycle: %s messages, %s article edits",
            len(batch),
            article_edits,
        )
        resume = 0 if self.records is not None else 0.2
        tracker.defer_remainder(Duration(seconds=resume))


class WriteWindowCount(beam.DoFn):
    """Upsert one ``(wiki, window)`` count into ``wiki_window_counts``."""

    def setup(self):
        self.conn = psycopg2.connect(postgres_dsn())

    def teardown(self):
        conn = getattr(self, "conn", None)
        if conn is not None:
            conn.close()

    def process(self, element, window=beam.DoFn.WindowParam):
        wiki, count = element
        window_start, window_end = window_bounds(window)
        with self.conn.cursor() as cur:
            cur.execute(
                UPSERT_WINDOW_COUNT_SQL,
                (wiki, window_start, window_end, int(count)),
            )
        self.conn.commit()
        print(
            f"wiki_window_counts {wiki} "
            f"[{window_start.isoformat()}, {window_end.isoformat()}) "
            f"edit_count={int(count)}",
            flush=True,
        )
        return []


def build_pipeline(p, reader, seed):
    edits = (
        p
        | "SeedReader" >> beam.Create([seed])
        | "ReadArticleEdits" >> beam.ParDo(reader)
    )
    return count_edits_per_wiki_window(edits)


def main():
    if load_dotenv is not None:
        load_dotenv()
    logging.basicConfig(level=logging.INFO)

    bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = _env("KAFKA_TOPIC", "wiki-edits")
    group_id = _env("KAFKA_GROUP_ID", "beam-fixed-windows")
    offset_reset = _env("KAFKA_AUTO_OFFSET_RESET", "latest")

    print(
        "Beam DirectRunner streaming pipeline: "
        f"FixedWindows({WINDOW_SIZE_SECONDS}s) on event timestamps, "
        f"watermark slack {WATERMARK_SLACK_SECONDS}s. "
        f"Kafka {bootstrap} topic {topic} group {group_id} "
        f"auto.offset.reset={offset_reset}.",
        flush=True,
    )

    options = PipelineOptions(["--runner=DirectRunner", "--streaming"])
    options.view_as(StandardOptions).streaming = True
    reader = ReadArticleEdits(bootstrap, topic, group_id, offset_reset)
    with beam.Pipeline(options=options) as pipeline:
        (
            build_pipeline(pipeline, reader, ("kafka", None))
            | "WritePostgres" >> beam.ParDo(WriteWindowCount())
        )


if __name__ == "__main__":
    main()
