#!/bin/bash

# Spark Size Calculator Job Submission Script

set -e

# Default values
SPARK_MASTER="local[*]"
INPUT_PATH="/data/input"
OUTPUT_PATH="/data/output"
OUTPUT_FORMAT="json"
JAR_PATH="target/scala-2.12/spark-size-calculator-assembly-0.1.0-SNAPSHOT.jar"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --master)
      SPARK_MASTER="$2"
      shift 2
      ;;
    --input-path)
      INPUT_PATH="$2"
      shift 2
      ;;
    --output-path)
      OUTPUT_PATH="$2"
      shift 2
      ;;
    --output-format)
      OUTPUT_FORMAT="$2"
      shift 2
      ;;
    --jar-path)
      JAR_PATH="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo "Options:"
      echo "  --master MASTER           Spark master URL (default: local[*])"
      echo "  --input-path PATH         Input data path (default: /data/input)"
      echo "  --output-path PATH        Output path (default: /data/output)"
      echo "  --output-format FORMAT    Output format: json|csv|parquet (default: json)"
      echo "  --jar-path PATH           Path to the JAR file"
      echo "  --help                    Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check if JAR exists
if [[ ! -f "$JAR_PATH" ]]; then
  echo "Error: JAR file not found at $JAR_PATH"
  echo "Please build the project first: sbt assembly"
  exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs

echo "Submitting Spark job with the following configuration:"
echo "  Master: $SPARK_MASTER"
echo "  Input Path: $INPUT_PATH"
echo "  Output Path: $OUTPUT_PATH"
echo "  Output Format: $OUTPUT_FORMAT"
echo "  JAR Path: $JAR_PATH"
echo ""

# Submit the Spark job
spark-submit \
  --master "$SPARK_MASTER" \
  --class "com.boomerang.spark.SizeCalculatorJob" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  "$JAR_PATH" \
  --input-path "$INPUT_PATH" \
  --output-path "$OUTPUT_PATH" \
  --output-format "$OUTPUT_FORMAT"

echo "Job submission completed!"