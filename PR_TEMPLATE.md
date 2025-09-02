# CP-1: Implement Spark job for data size calculation

## Overview
This PR implements CP-1 by creating a comprehensive Spark job for calculating data sizes across various storage systems.

## Changes Made

### Core Implementation
- **SizeCalculatorJob**: Main Spark application entry point with command-line argument handling
- **SizeCalculator**: Core logic for calculating data sizes with support for multiple data sources
- **JobConfig**: Configuration management using Typesafe Config

### Features Implemented
- ✅ Multi-source support (HDFS, S3, Local filesystem)
- ✅ Comprehensive metrics (file counts, total size, averages, largest/smallest files)
- ✅ Extension-based analysis and grouping
- ✅ Flexible output formats (JSON, CSV, Parquet)
- ✅ Optional checksum calculation (MD5)
- ✅ Configurable via command-line arguments and config files
- ✅ Robust error handling and logging
- ✅ Performance optimizations with caching

### Project Structure
- Build configuration with SBT and assembly plugin
- Docker containerization support
- Comprehensive test suite
- Deployment and utility scripts
- Detailed documentation

### Testing
- Unit tests for core functionality
- Integration tests for different configurations
- Local testing script for validation

### Documentation
- Comprehensive README with usage examples
- Implementation plan (plan.md)
- Configuration documentation
- Troubleshooting guide

## Usage Examples

### Basic usage:
```bash
./scripts/build-and-submit.sh \
  --input-path /data/input \
  --output-path /data/output \
  --output-format json
```

### HDFS cluster:
```bash
./scripts/submit-job.sh \
  --master yarn \
  --input-path hdfs://namenode:9000/data \
  --output-path hdfs://namenode:9000/reports
```

### Local testing:
```bash
./scripts/local-test.sh
```

## Technical Details

The implementation follows Spark best practices:
- Efficient DataFrame operations with proper caching
- Configurable parallelism for different cluster sizes
- Adaptive query execution enabled
- Proper resource management and cleanup

## Testing

Run the test suite:
```bash
sbt test
```

Run local integration test:
```bash
./scripts/local-test.sh
```

## Deployment

The application can be deployed via:
1. Direct spark-submit with the assembly JAR
2. Docker container with pre-configured environment
3. Cluster deployment with YARN or Kubernetes

**Resolves: CP-1**

## Files Changed
- `plan.md` - Implementation plan and strategy
- `build.sbt` - SBT build configuration with Spark dependencies
- `src/main/scala/com/boomerang/spark/SizeCalculatorJob.scala` - Main application
- `src/main/scala/com/boomerang/spark/config/JobConfig.scala` - Configuration management
- `src/main/scala/com/boomerang/spark/utils/SizeCalculator.scala` - Core calculation logic
- `src/main/resources/application.conf` - Application configuration
- `src/main/resources/log4j2.properties` - Logging configuration
- `scripts/` - Deployment and testing scripts
- `Dockerfile` - Container deployment support
- `README.md` - Comprehensive documentation
- Test files and project configuration