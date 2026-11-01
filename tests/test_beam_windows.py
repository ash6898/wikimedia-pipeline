"""Event-time FixedWindows(60) counts, including the streaming source."""

import json
import logging
import unittest

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from beam.pipeline import ReadArticleEdits
from beam.pipeline import WINDOW_SIZE_SECONDS
from beam.pipeline import build_pipeline
from beam.pipeline import count_edits_per_wiki_window
from beam.pipeline import parse_edit_record
from beam.pipeline import window_bounds

# 1_700_000_040 is an exact 60-second boundary (1_700_000_040 % 60 == 0).
BOUNDARY = 1_700_000_040


def interval(start):
    return IntervalWindow(
        Timestamp(start), Timestamp(start + WINDOW_SIZE_SECONDS)
    )


def edit(wiki, timestamp, namespace=0, bot=False):
    return json.dumps({
        "wiki": wiki,
        "user": "Example",
        "title": "Page",
        "bot": bot,
        "timestamp": timestamp,
        "minor": False,
        "namespace": namespace,
    }).encode("utf-8")


class ParseEditTest(unittest.TestCase):
    def test_article_edit(self):
        self.assertEqual(
            parse_edit_record(edit("enwiki", BOUNDARY + 5)),
            ("enwiki", float(BOUNDARY + 5)),
        )

    def test_non_article_and_garbage_are_dropped(self):
        self.assertIsNone(parse_edit_record(edit("enwiki", BOUNDARY, namespace=1)))
        self.assertIsNone(parse_edit_record(b"not-json"))
        self.assertIsNone(parse_edit_record(edit("", BOUNDARY)))


class WindowBoundsTest(unittest.TestCase):
    def test_bounds_are_utc(self):
        start, end = window_bounds(interval(BOUNDARY))
        self.assertEqual(start.timestamp(), BOUNDARY)
        self.assertEqual(end.timestamp(), BOUNDARY + WINDOW_SIZE_SECONDS)
        self.assertEqual(start.tzinfo.utcoffset(start).total_seconds(), 0)


class FixedWindowCountTest(unittest.TestCase):
    def setUp(self):
        logging.getLogger("apache_beam").setLevel(logging.ERROR)

    def test_teststream_assigns_epoch_aligned_60s_windows(self):
        """Window assignment itself, with the watermark controlled by TestStream."""
        stream = (
            TestStream()
            .advance_watermark_to(BOUNDARY - 1)
            .add_elements([
                TimestampedValue("enwiki", BOUNDARY - 1),
                TimestampedValue("enwiki", BOUNDARY),
                TimestampedValue("enwiki", BOUNDARY + 10),
                TimestampedValue("dewiki", BOUNDARY + 20),
            ])
            .advance_watermark_to_infinity()
        )
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as pipeline:
            counts = count_edits_per_wiki_window(pipeline | stream)
            assert_that(
                counts,
                equal_to_per_window({
                    interval(BOUNDARY - WINDOW_SIZE_SECONDS): [("enwiki", 1)],
                    interval(BOUNDARY): [("enwiki", 2), ("dewiki", 1)],
                }),
                reify_windows=True,
            )

    def test_streaming_source_counts_event_time_windows(self):
        records = [
            edit("enwiki", BOUNDARY + 5),
            edit("enwiki", BOUNDARY + 15, namespace=1),  # not an article
            edit("enwiki", BOUNDARY + 20),
            edit("dewiki", BOUNDARY + 30),
            edit("enwiki", BOUNDARY + WINDOW_SIZE_SECONDS + 5),
            edit("frwiki", BOUNDARY + WINDOW_SIZE_SECONDS + 15),
        ]
        reader = ReadArticleEdits(
            "unused:9092",
            "wiki-edits",
            "test",
            "earliest",
            records=records,
            batch_size=2,
        )
        options = PipelineOptions(["--runner=DirectRunner", "--streaming"])
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as pipeline:
            counts = build_pipeline(
                pipeline, reader, ("memory", len(records) + 1)
            )
            assert_that(
                counts,
                equal_to_per_window({
                    interval(BOUNDARY): [("enwiki", 2), ("dewiki", 1)],
                    interval(BOUNDARY + WINDOW_SIZE_SECONDS): [
                        ("enwiki", 1),
                        ("frwiki", 1),
                    ],
                }),
                reify_windows=True,
            )

    def test_edit_within_watermark_slack_stays_in_the_window(self):
        # One record per read cycle. The second timestamp is 10s behind the
        # first, inside the 15s slack, and must still be counted.
        records = [
            edit("enwiki", BOUNDARY + 50),
            edit("enwiki", BOUNDARY + 40),
        ]
        reader = ReadArticleEdits(
            "unused:9092",
            "wiki-edits",
            "test-slack",
            "earliest",
            records=records,
            batch_size=1,
        )
        options = PipelineOptions(["--runner=DirectRunner", "--streaming"])
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as pipeline:
            counts = build_pipeline(
                pipeline, reader, ("memory", len(records) + 1)
            )
            assert_that(
                counts,
                equal_to_per_window({
                    interval(BOUNDARY): [("enwiki", 2)],
                }),
                reify_windows=True,
            )

    def test_out_of_order_edit_is_counted_while_the_window_is_open(self):
        # 30s behind the newest event. The window has not closed yet, so the
        # late-arriving edit is still in the on-time count.
        records = [
            edit("enwiki", BOUNDARY + 50),
            edit("enwiki", BOUNDARY + 20),
        ]
        reader = ReadArticleEdits(
            "unused:9092",
            "wiki-edits",
            "test-late",
            "earliest",
            records=records,
            batch_size=1,
        )
        options = PipelineOptions(["--runner=DirectRunner", "--streaming"])
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as pipeline:
            counts = build_pipeline(
                pipeline, reader, ("memory", len(records) + 1)
            )
            assert_that(
                counts,
                equal_to_per_window({
                    interval(BOUNDARY): [("enwiki", 2)],
                }),
                reify_windows=True,
            )


if __name__ == "__main__":
    unittest.main()
