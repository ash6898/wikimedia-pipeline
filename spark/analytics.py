import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
JDBC_URL = f'jdbc:postgresql://localhost:5432/{POSTGRES_DB}'
JDBC_PROPERTIES = {
    'user': POSTGRES_USER,
    'password': POSTGRES_PASSWORD,
    'driver': 'org.postgresql.Driver'
}

def main():
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    os.environ['PATH'] = os.environ['PATH'] + ';C:\\hadoop\\bin'

    spark = SparkSession.builder \
        .appName("WikimediaAnalytics") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4") \
        .config("spark.jars.ivy", "C:\\Users\\aakas\\.ivy2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.jdbc(
        url=JDBC_URL,
        table="wiki_window_counts",
        properties=JDBC_PROPERTIES
    )

    df.cache()
    print(f"Total rows loaded: {df.count()}")

    # Top wikis by total edit count
    top_wikis = df.groupBy("wiki") \
        .agg(F.sum("edit_count").alias("total_edits")) \
        .orderBy(F.desc("total_edits"))
    
    top_wikis.show(10, truncate=False)

    # Edit velocity - edits per minute per wiki
    velocity = df.withColumn(
        "duration_minutes",
        (F.unix_timestamp("window_end") - F.unix_timestamp("window_start")) / 60
    ).withColumn(
        "edits_per_minute",
        F.col("edit_count") / F.col("duration_minutes")
    ).groupBy("wiki") \
        .agg(F.avg("edits_per_minute").alias("avg_edits_per_minute")) \
        .orderBy(F.desc("avg_edits_per_minute"))
    
    velocity.show(10, truncate=False)

    # Window trends — total edits per time window across all wikis
    trends = df.groupBy("window_start") \
        .agg(F.sum("edit_count").alias("total_edits")) \
        .orderBy("window_start")

    trends.show(20)

    # Write top wikis to top_editors table
    top_wikis.withColumnRenamed("wiki", "wiki") \
        .withColumn("editor", F.lit(None).cast("string")) \
        .withColumn("edit_count", F.col("total_edits").cast("integer")) \
        .withColumn("is_bot", F.lit(None).cast("boolean")) \
        .select("wiki", "editor", "edit_count", "is_bot") \
        .write.jdbc(
            url=JDBC_URL,
            table="top_editors",
            mode="overwrite",
            properties=JDBC_PROPERTIES,
        )

    print("top_editors written.")

    spark.stop()


if __name__ == "__main__":
    main()