#!/bin/bash

KAFKA_CONTAINER="ecommerce-analytics-kafka-1"
BOOTSTRAP_SERVERS="kafka:9092"

echo "Creating Kafka topics using container $KAFKA_CONTAINER..."

# Create raw events topic
echo "Creating topic: uk-retail-raw"
docker exec -it $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS \
  --create --if-not-exists \
  --topic uk-retail-raw \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000

# Create processed events topic
echo "Creating topic: uk-retail-processed"
docker exec -it $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS \
  --create --if-not-exists \
  --topic uk-retail-processed \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000

# Create analytics events topic
echo "Creating topic: uk-retail-analytics"
docker exec -it $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS \
  --create --if-not-exists \
  --topic uk-retail-analytics \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000

# Create customer events topic
echo "Creating topic: customer-events"
docker exec -it $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS \
  --create --if-not-exists \
  --topic customer-events \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000

echo "Topics created successfully!"
echo "Listing all topics:"
docker exec -it $KAFKA_CONTAINER kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS --list