#!/bin/bash

echo "Setting up E-commerce Analytics Platform..."

# Create required directories
mkdir -p data/raw
mkdir -p data/processed
mkdir -p flink_processing/conf
mkdir -p monitoring/grafana/provisioning/datasources
mkdir -p monitoring/prometheus

# Copy example env file if it doesn't exist
if [ ! -f .env ]; then
    cp .env.example .env
    echo "Created .env file from example. Please update with your configuration."
fi

# Make scripts executable
chmod +x scripts/download_dataset.sh
chmod +x data_ingestion/kafka/topics/create_topics.sh
chmod +x flink_processing/build.sh
chmod +x flink_processing/submit.sh

echo "Setup complete!"
echo "Next steps:"
echo "1. Download the UK Retail dataset: ./scripts/download_dataset.sh"
echo "2. Start the environment: docker-compose up -d"
echo "3. Process the dataset: python -m data_ingestion.loaders.uk_retail_loader --input data/raw/online_retail_II.csv --output data/processed/uk_retail_events.json"
echo "4. Create Kafka topics: ./data_ingestion/kafka/topics/create_topics.sh"
echo "5. Start the event simulator: python -m data_ingestion.loaders.event_simulator --input data/processed/uk_retail_events.json"
echo "6. Build and submit Flink job: cd flink_processing && ./build.sh && ./submit.sh"