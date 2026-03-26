#!/bin/bash

/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 \
    --create --if-not-exists \
    --topic wiki-edits \
    --partitions 3 \
    --replication-factor 1

/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 \
    --create --if-not-exists \
    --topic wiki-counts \
    --partitions 3 \
    --replication-factor 1

