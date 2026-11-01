# Wikimedia Real-Time Pipeline

A local lambda-style pipeline on Wikimedia's public recent-changes stream: a Python producer writes article edits to Kafka, an Apache Beam streaming job aggregates them in event-time windows into PostgreSQL, and a PySpark batch job computes edit velocity, top wikis, and bot-vs-human ratios.

## Architecture

```
Wikimedia SSE (recentchange)
        |
        v
Producer (Python, host)          filters type=edit
        |
        v
Kafka KRaft topic wiki-edits     payload includes wiki, bot, timestamp, namespace
        |                                    \
        |                                     \
        v                                      v
Apache Beam DirectRunner                 PySpark batch
event-time FixedWindows(60s)             reads wiki-edits from earliest offset
Count per wiki per window                bot vs human per wiki per 60s window
        |                                      |
        v                                      v
PostgreSQL wiki_window_counts            PostgreSQL bot_ratios
        |
        v
PySpark batch
top wikis by edit count, edit velocity
        |
        v
PostgreSQL top_editors
```

| Component | Version | Role |
|---|---|---|
| Apache Kafka (KRaft) | 3.7.0 | `wiki-edits` topic |
| Apache Beam DirectRunner | 2.62.0 | Event-time 60s windows |
| Apache Spark | 3.5.3 | Batch analytics |
| PostgreSQL | 17 | `wiki_window_counts`, `top_editors`, `bot_ratios` |
| confluent-kafka | 2.5.0 | Producer and the Beam Kafka reader |

## What the Beam job actually does

`beam/pipeline.py` is a streaming pipeline on the **DirectRunner** (one process, the default Python runner). It is not a wall-clock loop that buffers messages for 60 seconds and then runs a batch pipeline.

1. A splittable DoFn polls `wiki-edits` with `confluent-kafka`.
2. Namespace 0 (article) edits are emitted as Beam elements whose **event timestamp** is the Wikimedia `timestamp` field (Unix seconds).
3. `WindowInto(FixedWindows(60))` assigns those timestamps to epoch-aligned intervals `[N*60, (N+1)*60)`.
4. `Count.PerKey()` counts edits per wiki inside each window. The trigger is the default after-watermark trigger, discarding mode, allowed lateness 0.
5. When the watermark passes a window's end, the count is upserted into `wiki_window_counts`.

The watermark trails the newest event timestamp seen on earlier read cycles by 15 seconds (`WATERMARK_SLACK_SECONDS`). A window is written after an edit at least 15 seconds past that window has been read and the next read cycle advances the watermark. Until then the window stays open, so out-of-order edits that arrive while it is open are counted. The window that still contains the newest edit is not in Postgres yet. Once a window has fired, allowed lateness is zero and later edits for that window are dropped. Edits in the same poll are emitted together, before the watermark moves.

Beam's Python `ReadFromKafka` is a cross-language Java transform. The Beam 2.62 docs limit it to portable Flink, portable Spark, and Dataflow. It does not run on the DirectRunner. This repo stays on the DirectRunner so `python beam/pipeline.py` is the streaming job: a real Beam event-time window, with a Python Kafka reader in front of it. It is not a distributed Flink or Dataflow deployment.

`KAFKA_AUTO_OFFSET_RESET` defaults to `latest`. Set it to `earliest` to replay the retained topic. Replays upsert `(wiki, window_start)` and replace that window's count.

## What the Spark job actually does

`spark/analytics.py` is a batch recompute.

From `wiki_window_counts` (the Beam output):

- **Top wikis** by summed `edit_count`, written to `top_editors`. That table's `editor` and `is_bot` columns are null. The metric is top wikis, which is what the table is used for.
- **Edit velocity**: for each stored window, `edit_count / window_duration_minutes`, then the average per wiki. Beam's windows are 60 seconds, so this is the window's edit count expressed per minute.
- A printed total of edits per window start.

From the raw `wiki-edits` topic (earliest offset through whatever Kafka still retains; the image default is 7 days):

- **Bot vs human ratios** for namespace 0 edits. `bot: true` is a bot edit. A missing or false `bot` flag is a human edit.
- Windows are the same epoch-aligned 60-second boundaries as Beam `FixedWindows(60)`, using the event `timestamp`.
- `bot_ratio = bot_edits / (bot_edits + human_edits)`.
- Rows are written to `bot_ratios`, replacing the previous contents. The console also prints one ratio per wiki summed across those windows.

An empty input does not wipe the corresponding output table.

## Quick start

Docker Compose runs Kafka and PostgreSQL. The producer and the Beam job run on the host and use `localhost:9092`. Copy `.env.example` to `.env` to override the defaults (`wikimedia` / `wikimedia` / `wikimedia`); both Compose and the Python processes work with those defaults if `.env` is absent.

Schema changes are applied only when the Postgres volume is created. If you already have a volume from an older checkout, recreate it so `bot_ratio` and the unique keys exist:

```bash
docker compose down -v
docker compose up -d
```

```bash
docker compose up -d
```

```bash
pip install -r producer/requirements.txt
python producer/wikimedia_producer.py
```

```bash
pip install -r beam/requirements.txt
python beam/pipeline.py
```

Wait until a 60-second boundary has passed and later edits have pulled the watermark across it (roughly 15 seconds into the next window). Then:

```bash
pip install -r spark/requirements.txt
python spark/analytics.py
```

The first Spark launch downloads the PostgreSQL JDBC driver and `spark-sql-kafka`. Java 17+ is required for Spark. The Beam DirectRunner path does not need a separate Java expansion service.

To run the same Spark job inside Compose (it uses `kafka:29092` and `postgres:5432`):

```bash
docker compose --profile analytics run --rm spark-analytics
```

The Spark UI for that run is on port 4040. Kafka UI is on http://localhost:8080.

## Queries

```sql
SELECT wiki, window_start, window_end, edit_count
FROM wiki_window_counts
ORDER BY window_start DESC, edit_count DESC
LIMIT 20;

SELECT wiki, window_start, bot_edits, human_edits, bot_ratio
FROM bot_ratios
ORDER BY window_start DESC, wiki
LIMIT 20;

SELECT wiki, edit_count
FROM top_editors
ORDER BY edit_count DESC
LIMIT 20;
```

## Tests

```bash
pip install -r beam/requirements.txt -r spark/requirements.txt
python -m unittest tests/test_beam_windows.py tests/test_bot_ratios.py
```

The Beam tests drive `FixedWindows(60)` with `TestStream` and with the same splittable reader used in production, fed from memory instead of Kafka. The Spark tests check window alignment, the namespace filter, null `bot` values, and `bot_ratio` without a broker.

## Project layout

```
wikimedia-pipeline/
├── docker-compose.yml
├── .env.example
├── producer/
│   ├── wikimedia_producer.py
│   └── requirements.txt
├── beam/
│   ├── pipeline.py
│   └── requirements.txt
├── spark/
│   ├── analytics.py
│   └── requirements.txt
├── postgres/
│   └── init.sql
├── scripts/
│   ├── create_topics.sh
│   └── peek_consumer.py
└── tests/
    ├── test_beam_windows.py
    └── test_bot_ratios.py
```

## Prerequisites

- Docker with Compose v2
- Python 3.9+
- Java 17+ (PySpark)
