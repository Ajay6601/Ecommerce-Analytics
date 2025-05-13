#!/bin/bash

# Navigate to the script's directory
cd "$(dirname "$0")"

# Find the JAR file - corrected approach
JAR_FILE=""
for jar in build/libs/ecommerce-analytics-flink-*-all.jar; do
    if [ -f "$jar" ]; then
        JAR_FILE="$jar"
        break
    fi
done

# Check if JAR file was found
if [ -z "$JAR_FILE" ]; then
    echo "JAR file not found. Run ./build.sh first."
    exit 1
fi
echo "Found JAR: $JAR_FILE"

# Find the Flink JobManager container
CONTAINER_NAME="ecommerce-analytics-flink-jobmanager-1"
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    echo "Flink JobManager container not found."
    echo "Is Docker Compose running? Check with 'docker-compose ps'"
    exit 1
fi
echo "✓ Found Flink JobManager container: $CONTAINER_NAME"

# Copy the JAR to the container
echo "Copying JAR to container..."
docker cp "$JAR_FILE" "$CONTAINER_NAME:/opt/flink/usrlib/ecommerce-analytics-flink.jar"
if [ $? -ne 0 ]; then
    echo "Failed to copy JAR to container."
    exit 1
fi
echo "JAR copied to container."

# Submit the job
echo "Submitting job to Flink cluster..."
docker exec -it "$CONTAINER_NAME" flink run \
  -c com.ecommerce.analytics.RealTimeAnalytics \
  /opt/flink/usrlib/ecommerce-analytics-flink.jar \
  --bootstrap-servers kafka:9092 \
  --input-topic uk-retail-raw \
  --output-topic uk-retail-processed

if [ $? -eq 0 ]; then
    echo ""
    echo "Job submitted successfully!"
    echo "View the job in the Flink Dashboard: http://localhost:8083"
else
    echo "Job submission failed. Check the error messages above."
    exit 1
fi