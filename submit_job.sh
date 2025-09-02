#!/bin/bash

# Spark Size Calculation Job Submission Script
# Usage: ./submit_job.sh [options]

set -e

# Default values
CONFIG_FILE="src/main/resources/application.conf"
INPUT_PATH=""
OUTPUT_PATH=""
SIZE_COLUMN="size"
INPUT_FORMAT="parquet"
OUTPUT_FORMAT="parquet"
SPARK_MASTER="local[*]"
EXECUTOR_MEMORY="2g"
DRIVER_MEMORY="1g"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG_FILE="$2"
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
        --size-column)
            SIZE_COLUMN="$2"
            shift 2
            ;;
        --input-format)
            INPUT_FORMAT="$2"
            shift 2
            ;;
        --output-format)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        --spark-master)
            SPARK_MASTER="$2"
            shift 2
            ;;
        --executor-memory)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        --driver-memory)
            DRIVER_MEMORY="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --config FILE           Configuration file path"
            echo "  --input-path PATH       Input data path"
            echo "  --output-path PATH      Output results path"
            echo "  --size-column COLUMN    Name of size column (default: size)"
            echo "  --input-format FORMAT   Input format: parquet|csv|json (default: parquet)"
            echo "  --output-format FORMAT  Output format: parquet|csv|json (default: parquet)"
            echo "  --spark-master MASTER   Spark master URL (default: local[*])"
            echo "  --executor-memory MEM   Executor memory (default: 2g)"
            echo "  --driver-memory MEM     Driver memory (default: 1g)"
            echo "  --help                  Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build spark-submit command
SPARK_SUBMIT_CMD="spark-submit"
SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --master $SPARK_MASTER"
SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --executor-memory $EXECUTOR_MEMORY"
SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --driver-memory $DRIVER_MEMORY"
SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --py-files src/main/python/config.py,src/main/python/utils.py"
SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD src/main/python/size_calculator.py"

# Add application arguments
if [[ -n "$CONFIG_FILE" ]]; then
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --config $CONFIG_FILE"
fi

if [[ -n "$INPUT_PATH" ]]; then
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --input-path $INPUT_PATH"
fi

if [[ -n "$OUTPUT_PATH" ]]; then
    SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --output-path $OUTPUT_PATH"
fi

SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --size-column $SIZE_COLUMN"
SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --input-format $INPUT_FORMAT"
SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --output-format $OUTPUT_FORMAT"

echo "Submitting Spark job with command:"
echo "$SPARK_SUBMIT_CMD"
echo ""

# Execute the command
eval $SPARK_SUBMIT_CMD