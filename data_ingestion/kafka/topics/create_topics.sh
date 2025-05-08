#!/bin/bash

KAFKA_BOOTSTRAP_SERVERS=${1:-localhost:29092}

echo "Creating Kafka topics on $KAFKA_BOOTSTRAP_SERVERS..."

# Create raw events topic
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --create --if-not-exists \
  --topic uk-retail-raw \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000  # 24 hours

# Create processed events topic
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --create --if-not-exists \
  --topic uk-retail-processed \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000  # 24 hours

# Create analytics events topic
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --create --if-not-exists \
  --topic uk-retail-analytics \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000  # 24 hours

echo "Topics created successfully!"
echo "Listing all topics:"
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --list