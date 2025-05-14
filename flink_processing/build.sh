#!/bin/bash

# Navigate to the script's directory
cd "$(dirname "$0")"

echo "Using Gradle to build shadow JAR..."

# Clean and build
./gradlew clean shadowJar

# Check if build succeeded
if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Build completed successfully!"

    # Find the JAR using a proper approach
    JAR_FILE=""
    for jar in build/libs/ecommerce-analytics-flink-*-all.jar; do
        if [ -f "$jar" ]; then
            JAR_FILE="$jar"
            break
        fi
    done

    if [ -n "$JAR_FILE" ]; then
        JAR_SIZE=$(du -h "$JAR_FILE" | cut -f1)
        echo "✓ JAR created: $JAR_FILE ($JAR_SIZE)"
        echo ""
        echo "To submit the job, run: ./submit.sh"
    else
        echo "JAR not found in build/libs/. Check the build output."
        exit 1
    fi
else
    echo "Build failed. Check the build output for errors."
    exit 1
fi