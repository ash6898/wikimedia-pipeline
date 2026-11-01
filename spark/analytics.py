"""Batch analytics over Beam window counts and raw wiki edits.

``wiki_window_counts`` (written by the Beam streaming job) is the source for
top wikis by edit count and edit velocity.

``bot_ratios`` is computed here from the raw ``wiki-edits`` Kafka topic.
Each row is one wiki and one epoch-aligned 60-second event-time window, the
same boundaries as Beam ``FixedWindows(60)``:

    bot_ratio = bot_edits / (bot_edits + human_edits)

Only namespace 0 (article) edits are included. A missing ``bot`` flag counts
as a human edit. The job recomputes the table from the topic retained by
Kafka (the broker default is 7 days) and replaces previous rows.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

WINDOW_SECONDS = 60

EDIT_SCHEMA = StructType([
    StructField("wiki", StringType()),
    StructField("user", StringType()),
    StructField("title", StringType()),
    StructField("bot", BooleanType()),
    StructField("timestamp", LongType()),
    StructField("minor", BooleanType()),
    StructField("namespace", IntegerType()),
])


def _env(name, default):
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def postgres_settings():
    if load_dotenv is not None:
        load_dotenv()
    return {
        "host": _env("POSTGRES_HOST", "localhost"),
        "port": _env("POSTGRES_PORT", "5432"),
        "db": _env("POSTGRES_DB", "wikimedia"),
        "user": _env("POSTGRES_USER", "wikimedia"),
        "password": _env("POSTGRES_PASSWORD", "wikimedia"),
    }


def jdbc_url(settings=None):
    settings = settings or postgres_settings()
    return (
        f"jdbc:postgresql://{settings['host']}:{settings['port']}/{settings['db']}"
        "?options=-c%20TimeZone%3DUTC"
    )


def jdbc_properties(settings=None):
    settings = settings or postgres_settings()
    return {
        "user": settings["user"],
        "password": settings["password"],
        "driver": "org.postgresql.Driver",
    }


def build_spark():
    return (
        SparkSession.builder
        .appName("WikimediaAnalytics")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.4,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .getOrCreate()
    )


def top_wikis(window_counts):
    """Sum Beam window counts into total article edits per wiki."""
    return (
        window_counts.groupBy("wiki")
        .agg(F.sum("edit_count").alias("total_edits"))
        .orderBy(F.desc("total_edits"))
    )


def edit_velocity(window_counts):
    """Average edits per minute per wiki, from each stored window's duration."""
    duration_minutes = (
        F.unix_timestamp("window_end") - F.unix_timestamp("window_start")
    ) / F.lit(60.0)
    return (
        window_counts.withColumn("duration_minutes", duration_minutes)
        .withColumn(
            "edits_per_minute",
            F.when(
                F.col("duration_minutes") > 0,
                F.col("edit_count") / F.col("duration_minutes"),
            ),
        )
        .groupBy("wiki")
        .agg(F.avg("edits_per_minute").alias("avg_edits_per_minute"))
        .orderBy(F.desc("avg_edits_per_minute"))
    )


def bot_human_ratios(edits):
    """Per-wiki bot vs human counts for each epoch-aligned 60-second window.

    ``edits`` columns: ``wiki``, ``bot``, ``timestamp`` (Unix seconds),
    ``namespace``. ``bot_ratio`` is the fraction of article edits made by bots.
    """
    articles = (
        edits.where(F.col("namespace") == 0)
        .where(F.col("wiki").isNotNull() & (F.col("wiki") != ""))
        .where(F.col("timestamp").isNotNull())
    )
    window_start = (
        F.floor(F.col("timestamp").cast("double") / F.lit(float(WINDOW_SECONDS)))
        * F.lit(WINDOW_SECONDS)
    ).cast("long")
    counted = articles.groupBy(
        "wiki",
        window_start.alias("window_start_s"),
    ).agg(
        F.sum(F.when(F.col("bot") == F.lit(True), 1).otherwise(0))
        .cast("int")
        .alias("bot_edits"),
        F.sum(F.when(F.col("bot") == F.lit(True), 0).otherwise(1))
        .cast("int")
        .alias("human_edits"),
    )
    total = F.col("bot_edits") + F.col("human_edits")
    return counted.select(
        "wiki",
        F.timestamp_seconds(F.col("window_start_s").cast("double")).alias(
            "window_start"
        ),
        F.timestamp_seconds(
            (F.col("window_start_s") + F.lit(WINDOW_SECONDS)).cast("double")
        ).alias("window_end"),
        "bot_edits",
        "human_edits",
        (F.col("bot_edits").cast("double") / total.cast("double")).alias(
            "bot_ratio"
        ),
    )


def overall_bot_ratios(window_ratios):
    """Sum per-window bot and human counts into one ratio per wiki."""
    totals = window_ratios.groupBy("wiki").agg(
        F.sum("bot_edits").cast("int").alias("bot_edits"),
        F.sum("human_edits").cast("int").alias("human_edits"),
    )
    total = F.col("bot_edits") + F.col("human_edits")
    return totals.select(
        "wiki",
        "bot_edits",
        "human_edits",
        (F.col("bot_edits").cast("double") / total.cast("double")).alias(
            "bot_ratio"
        ),
    ).orderBy(F.desc(F.col("bot_edits") + F.col("human_edits")))


def parse_kafka_edits(kafka_df):
    """Parse ``wiki-edits`` Kafka values into edit columns."""
    return (
        kafka_df.select(
            F.from_json(F.col("value").cast("string"), EDIT_SCHEMA).alias("edit")
        )
        .select("edit.*")
    )


def read_kafka_edits(spark):
    bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = _env("KAFKA_TOPIC", "wiki-edits")
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return parse_kafka_edits(raw)


def write_postgres(df, table, settings=None):
    """Replace ``table`` contents without dropping its columns or constraints."""
    (
        df.write.mode("overwrite")
        .option("truncate", "true")
        .jdbc(jdbc_url(settings), table, properties=jdbc_properties(settings))
    )


def main():
    settings = postgres_settings()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    window_counts = spark.read.jdbc(
        url=jdbc_url(settings),
        table="wiki_window_counts",
        properties=jdbc_properties(settings),
    )
    window_counts.cache()
    window_rows = window_counts.count()
    print(f"wiki_window_counts rows: {window_rows}")

    ranked = top_wikis(window_counts)
    print("Top wikis by edit count:")
    ranked.show(10, truncate=False)

    velocity = edit_velocity(window_counts)
    print("Edit velocity (average edits per minute):")
    velocity.show(10, truncate=False)

    print("Edits per window, all wikis:")
    (
        window_counts.groupBy("window_start")
        .agg(F.sum("edit_count").alias("total_edits"))
        .orderBy("window_start")
        .show(20, truncate=False)
    )

    if window_rows == 0:
        print("wiki_window_counts is empty; top_editors was not modified.")
    else:
        (
            ranked.select(
                "wiki",
                F.lit(None).cast("string").alias("editor"),
                F.col("total_edits").cast("int").alias("edit_count"),
                F.lit(None).cast("boolean").alias("is_bot"),
            )
            .write.mode("overwrite")
            .option("truncate", "true")
            .jdbc(
                jdbc_url(settings),
                "top_editors",
                properties=jdbc_properties(settings),
            )
        )
        print("top_editors written (one row per wiki; editor and is_bot are null).")

    print(
        "Bot vs human ratios from Kafka topic "
        f"{_env('KAFKA_TOPIC', 'wiki-edits')} "
        "(namespace 0, 60s event-time windows):"
    )
    ratios = bot_human_ratios(read_kafka_edits(spark))
    ratios.cache()
    ratio_rows = ratios.count()
    if ratio_rows == 0:
        print("No article edits in Kafka; bot_ratios was not modified.")
    else:
        print("Per wiki, summed across windows:")
        overall_bot_ratios(ratios).show(20, truncate=False)
        print("Per wiki per 60s window (written to bot_ratios):")
        ratios.orderBy("window_start", "wiki").show(20, truncate=False)
        write_postgres(ratios, "bot_ratios", settings)
        print(f"bot_ratios written ({ratio_rows} rows).")

    spark.stop()


if __name__ == "__main__":
    main()
