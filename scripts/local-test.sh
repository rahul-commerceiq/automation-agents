#!/bin/bash

# Local testing script for Spark Size Calculator

set -e

# Create test data directory
TEST_DATA_DIR="/tmp/spark-test-data"
TEST_OUTPUT_DIR="/tmp/spark-test-output"

echo "Setting up test environment..."

# Clean up previous test data
rm -rf "$TEST_DATA_DIR" "$TEST_OUTPUT_DIR"

# Create test directory structure
mkdir -p "$TEST_DATA_DIR/subdir1" "$TEST_DATA_DIR/subdir2"

# Create test files with various sizes
echo "Creating test files..."
echo "Small file content" > "$TEST_DATA_DIR/small.txt"
head -c 1048576 /dev/zero > "$TEST_DATA_DIR/medium.bin"  # 1MB
head -c 10485760 /dev/zero > "$TEST_DATA_DIR/large.bin"  # 10MB
echo '{"test": "data", "size": "json"}' > "$TEST_DATA_DIR/data.json"
echo "print('Python script')" > "$TEST_DATA_DIR/script.py"
echo "Nested file content" > "$TEST_DATA_DIR/subdir1/nested.txt"
echo "Another nested file" > "$TEST_DATA_DIR/subdir2/another.txt"
echo ".hidden content" > "$TEST_DATA_DIR/.hidden"

echo "Test data created at: $TEST_DATA_DIR"
echo "Files created:"
find "$TEST_DATA_DIR" -type f -exec ls -lh {} \;

echo ""
echo "Building and running Spark job..."

# Build the project
sbt assembly

# Run the job
./scripts/submit-job.sh \
  --master local[2] \
  --input-path "$TEST_DATA_DIR" \
  --output-path "$TEST_OUTPUT_DIR" \
  --output-format json

echo ""
echo "Job completed! Results:"
echo "Output directory contents:"
find "$TEST_OUTPUT_DIR" -type f -name "*.json" -exec echo "=== {} ===" \; -exec cat {} \;

echo ""
echo "Cleaning up test data..."
rm -rf "$TEST_DATA_DIR" "$TEST_OUTPUT_DIR"
echo "Test completed successfully!"