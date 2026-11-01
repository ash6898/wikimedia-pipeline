"""PySpark bot-vs-human ratios on epoch-aligned 60-second windows."""

import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.analytics import WINDOW_SECONDS
from spark.analytics import bot_human_ratios
from spark.analytics import overall_bot_ratios
from spark.analytics import parse_kafka_edits
from spark.analytics import EDIT_SCHEMA

# Same boundary the Beam tests use. 1_700_000_040 % 60 == 0.
BOUNDARY = 1_700_000_040


class BotRatioTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("bot-ratio-test")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_ratios_match_beam_window_boundaries(self):
        rows = [
            ("enwiki", True, BOUNDARY - 1, 0),
            ("enwiki", True, BOUNDARY, 0),
            ("enwiki", False, BOUNDARY + 10, 0),
            ("enwiki", None, BOUNDARY + 20, 0),  # missing bot flag -> human
            ("dewiki", False, BOUNDARY + 30, 0),
            ("enwiki", True, BOUNDARY + 5, 1),  # not an article
            ("enwiki", True, BOUNDARY + WINDOW_SECONDS, 0),
            ("frwiki", False, 1_700_000_000, 2),  # not an article
        ]
        edits = self.spark.createDataFrame(
            rows, ["wiki", "bot", "timestamp", "namespace"]
        )
        ratios = bot_human_ratios(edits).select(
            "wiki",
            F.unix_timestamp("window_start").alias("start"),
            F.unix_timestamp("window_end").alias("end"),
            "bot_edits",
            "human_edits",
            "bot_ratio",
        )
        actual = {
            (row.wiki, row.start): row for row in ratios.collect()
        }

        previous = BOUNDARY - WINDOW_SECONDS
        self.assertEqual(set(actual), {
            ("enwiki", previous),
            ("enwiki", BOUNDARY),
            ("dewiki", BOUNDARY),
            ("enwiki", BOUNDARY + WINDOW_SECONDS),
        })

        first = actual[("enwiki", previous)]
        self.assertEqual((first.bot_edits, first.human_edits), (1, 0))
        self.assertAlmostEqual(first.bot_ratio, 1.0)
        self.assertEqual(first.end, previous + WINDOW_SECONDS)

        current = actual[("enwiki", BOUNDARY)]
        self.assertEqual((current.bot_edits, current.human_edits), (1, 2))
        self.assertAlmostEqual(current.bot_ratio, 1.0 / 3.0)

        dewiki = actual[("dewiki", BOUNDARY)]
        self.assertEqual((dewiki.bot_edits, dewiki.human_edits), (0, 1))
        self.assertAlmostEqual(dewiki.bot_ratio, 0.0)

        nxt = actual[("enwiki", BOUNDARY + WINDOW_SECONDS)]
        self.assertEqual((nxt.bot_edits, nxt.human_edits), (1, 0))

        overall = {
            row.wiki: row for row in overall_bot_ratios(bot_human_ratios(edits)).collect()
        }
        self.assertEqual((overall["enwiki"].bot_edits, overall["enwiki"].human_edits), (3, 2))
        self.assertAlmostEqual(overall["enwiki"].bot_ratio, 3.0 / 5.0)

    def test_parse_kafka_json(self):
        payload = (
            '{"wiki":"enwiki","user":"Alice","title":"Page","bot":false,'
            '"timestamp":1700000040,"minor":false,"namespace":0}'
        )
        raw = self.spark.createDataFrame([(payload,)], ["value"])
        parsed = parse_kafka_edits(raw)
        self.assertEqual(parsed.schema.simpleString(), EDIT_SCHEMA.simpleString())
        row = parsed.collect()[0]
        self.assertEqual(row.wiki, "enwiki")
        self.assertFalse(row.bot)
        self.assertEqual(row.timestamp, BOUNDARY)
        self.assertEqual(row.namespace, 0)


if __name__ == "__main__":
    unittest.main()
