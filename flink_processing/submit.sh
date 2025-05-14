#!/bin/bash
# Navigate to the script's directory
cd "$(dirname "$0")"

# Find the JAR file
JAR_FILE=""
for jar in build/libs/ecommerce-analytics-flink-*-all.jar; do
    if [ -f "$jar" ]; then
        JAR_FILE="$jar"
        break
    fi
done

if [ -z "$JAR_FILE" ]; then
    echo "❌ JAR file not found. Run ./build.sh first."
    exit 1
fi
echo "✓ Found JAR: $JAR_FILE"

CONTAINER_NAME="ecommerce-analytics-flink-jobmanager-1"
echo "✓ Using Flink JobManager container: $CONTAINER_NAME"

# Copy JAR to container
echo "Copying JAR to container..."
docker cp "$JAR_FILE" "${CONTAINER_NAME}:/tmp/flink-job.jar"

if [ $? -ne 0 ]; then
    echo "❌ Failed to copy JAR to container."
    exit 1
fi
echo "✅ JAR copied to container."

# The key change: Use sh -c to run the command entirely inside the container
echo "Submitting job to Flink cluster..."
docker exec $CONTAINER_NAME sh -c 'flink run -c com.ecommerce.analytics.RealTimeAnalytics /tmp/flink-job.jar --bootstrap-servers kafka:9092 --input-topic uk-retail-raw --output-topic uk-retail-processed'

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Job submitted successfully!"
    echo "📊 View the job in the Flink Dashboard: http://localhost:8083"
else
    echo "❌ Job submission failed. Check the error messages above."
    exit 1
fi