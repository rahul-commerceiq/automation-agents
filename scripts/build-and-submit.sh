#!/bin/bash

# Build and Submit Spark Size Calculator Job

set -e

echo "Building Spark Size Calculator..."

# Clean and build the project
sbt clean assembly

echo "Build completed successfully!"

# Submit the job with provided arguments
./scripts/submit-job.sh "$@"