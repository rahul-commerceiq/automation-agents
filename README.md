# Spark Data Size Calculator

A Spark job implementation for calculating data sizes across various storage systems as part of CP-1.

## Overview

This Spark application calculates comprehensive size statistics for data stored in HDFS, S3, or local filesystem. It provides detailed metrics including total size, file counts, extension-based statistics, and performance insights.

## Features

- **Multi-source Support**: HDFS, S3, Local filesystem
- **Comprehensive Metrics**: File counts, sizes, averages, largest/smallest files
- **Extension Analysis**: Group statistics by file extension
- **Flexible Output**: JSON, CSV, or Parquet formats
- **Performance Optimized**: Configurable parallelism and caching
- **Error Handling**: Robust error handling with detailed logging
- **Checksum Calculation**: Optional MD5 checksum calculation
- **Docker Support**: Containerized deployment option

## Quick Start

### Prerequisites

- Apache Spark 3.4.1+
- Scala 2.12+
- SBT 1.9+
- Java 11+

### Building the Project

```bash
# Clone and build
git clone <repository-url>
cd spark-size-calculator
sbt assembly
```

### Running the Job

#### Local Mode
```bash
# Using the convenience script
./scripts/build-and-submit.sh \
  --input-path /path/to/data \
  --output-path /path/to/output \
  --output-format json

# Or directly with spark-submit
spark-submit \
  --class com.boomerang.spark.SizeCalculatorJob \
  --master local[*] \
  target/scala-2.12/spark-size-calculator-assembly-*.jar \
  --input-path /path/to/data \
  --output-path /path/to/output
```

#### Cluster Mode
```bash
./scripts/submit-job.sh \
  --master spark://master:7077 \
  --input-path hdfs://namenode:9000/data \
  --output-path hdfs://namenode:9000/output \
  --output-format parquet
```

#### Docker
```bash
# Build Docker image
docker build -t spark-size-calculator .

# Run container
docker run \
  -v /local/data:/data \
  spark-size-calculator \
  --input-path /data/input \
  --output-path /data/output
```

## Configuration

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--input-path` | Path to input data | `/data/input` |
| `--output-path` | Path for output results | `/data/output` |
| `--output-format` | Output format (json/csv/parquet) | `json` |
| `--include-hidden` | Include hidden files | `false` |
| `--recursive` | Process directories recursively | `true` |
| `--calculate-checksums` | Calculate MD5 checksums | `false` |
| `--group-by-extension` | Group statistics by file extension | `true` |
| `--parallelism` | Number of parallel tasks | `4` |

### Configuration File

Edit `src/main/resources/application.conf` for default settings:

```hocon
spark {
  default {
    input-path = "/data/input"
    output-path = "/data/output"
    output-format = "json"
  }
  
  calculation {
    include-hidden-files = false
    recursive = true
    calculate-checksums = false
    group-by-extension = true
  }
  
  performance {
    parallelism = 4
    max-records-per-file = 1000000
    cache-intermediate-results = true
  }
}
```

## Output Format

### Summary Output
```json
{
  "metric": "total_files",
  "value": "15420"
}
```

### Extension Statistics
```json
{
  "extension": "txt",
  "file_count": 1250,
  "total_size_bytes": 52428800,
  "total_size_formatted": "50.00 MB"
}
```

## Examples

### Calculate sizes for HDFS directory
```bash
./scripts/submit-job.sh \
  --master yarn \
  --input-path hdfs://namenode:9000/user/data \
  --output-path hdfs://namenode:9000/user/reports \
  --output-format parquet \
  --recursive true
```

### Calculate sizes for S3 bucket
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

./scripts/submit-job.sh \
  --input-path s3a://your-bucket/data/ \
  --output-path s3a://your-bucket/reports/ \
  --output-format json
```

### Local filesystem with checksums
```bash
./scripts/submit-job.sh \
  --master local[4] \
  --input-path /local/data \
  --output-path /local/output \
  --calculate-checksums true \
  --include-hidden true
```

## Performance Tuning

### For Large Datasets
- Increase executor memory: `--executor-memory 4g`
- Increase driver memory: `--driver-memory 2g`
- Adjust parallelism based on cluster size
- Use Parquet output format for better performance
- Enable intermediate result caching

### For Small Datasets
- Use local mode: `--master local[*]`
- Reduce parallelism to avoid overhead
- Use JSON or CSV output for simplicity

## Monitoring

### Spark UI
Access the Spark UI at `http://driver:4040` to monitor job progress and performance.

### Logs
- Application logs: `logs/spark-size-calculator.log`
- Spark logs: Check Spark history server or executor logs

## Troubleshooting

### Common Issues

1. **OutOfMemoryError**
   - Increase driver/executor memory
   - Reduce parallelism
   - Enable result caching

2. **Permission Denied**
   - Check filesystem permissions
   - Verify Spark user has access to input/output paths

3. **File Not Found**
   - Verify input path exists
   - Check filesystem connectivity (HDFS/S3)

### Debug Mode
Enable debug logging by setting log level to DEBUG in `log4j2.properties`.

## Development

### Building
```bash
sbt compile
sbt test
sbt assembly
```

### Testing
```bash
sbt test
```

### Code Style
```bash
sbt scalafmt
```

## Contributing

1. Create a feature branch
2. Make changes
3. Add tests
4. Run `sbt test`
5. Submit a pull request

## License

Internal use only - Boomerang Technologies