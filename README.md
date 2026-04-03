# Wikimedia Real-Time Pipeline

A full end-to-end streaming + batch data pipeline built on real public data. Demonstrates a Lambda architecture using Wikimedia's live recent-changes stream.

## Architecture

```
Wikimedia SSE Stream
        ↓
   Producer (Python)
        ↓
   Kafka (wiki-edits topic)
        ↓
   Apache Beam (60s micro-batch windows)
        ↓
   PostgreSQL (wiki_window_counts)
        ↓
   PySpark (batch analytics)
        ↓
   PostgreSQL (top_editors, bot_ratios)
```

## Stack

| Component | Version | Role |
|---|---|---|
| Apache Kafka (KRaft) | 3.7.0 | Message broker |
| Apache Spark | 3.5.3 | Batch analytics |
| PostgreSQL | 17 | Persistent storage |
| Apache Beam | 2.62.0 | Stream processing |
| confluent-kafka | 2.5.0 | Kafka Python client |
| sseclient-py | 1.9.0 | Wikimedia SSE client |

## Quick Start

**1. Start infrastructure:**
```bash
docker compose up -d
```

**2. Install producer dependencies:**
```bash
pip install -r producer/requirements.txt
```

**3. Start the producer:**
```bash
python producer/wikimedia_producer.py
```

**4. Install Beam dependencies:**
```bash
pip install -r beam/requirements.txt
```

**5. Start the Beam pipeline:**
```bash
python beam/pipeline.py
```

**6. After data accumulates, run Spark analytics:**
```bash
pip install -r spark/requirements.txt
python spark/analytics.py
```

## Services

| Service | URL | Purpose |
|---|---|---|
| Kafka UI | http://localhost:8080 | Monitor Kafka topics and messages |

## Data Flow

- **Producer** — connects to `https://stream.wikimedia.org/v2/stream/recentchange`, filters for article edits (namespace=0), publishes to `wiki-edits` Kafka topic
- **Beam pipeline** — consumes `wiki-edits` in 60s micro-batch windows, counts edits per wiki, writes to `wiki_window_counts` table
- **Spark analytics** — reads `wiki_window_counts`, computes top wikis by edit count, edit velocity (edits/min), and window trends

## Database Schema

```sql
wiki_window_counts  -- edit counts per wiki per 60s window (written by Beam)
top_editors         -- top wikis by total edits (written by Spark)
bot_ratios          -- bot vs human edit ratios (written by Spark)
```

## Project Structure

```
wikimedia-pipeline/
├── docker-compose.yml
├── .env
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
└── scripts/
    ├── create_topics.sh
    └── peek_consumer.py
```

## Prerequisites

- Docker Desktop
- Python 3.9+
- Java 17+ (required by Apache Beam)
