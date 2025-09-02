# Spark Size Calculation Job

A Python-based Spark job implementation for calculating data size metrics as part of CP-1.

## Overview

This project implements a PySpark job that processes input data and calculates comprehensive size metrics including:
- Total record count
- Estimated data size in bytes
- Column-level statistics (null counts, distinct values, average lengths)
- Partition-level information
- Processing timestamps

## Features

- **Multiple Input Formats**: Supports Parquet, JSON, CSV, and Delta formats
- **Configurable Output**: Flexible output formats and modes
- **Comprehensive Metrics**: Both table-level and column-level statistics
- **Error Handling**: Robust error handling with detailed logging
- **Testing**: Complete unit test suite with pytest
- **Performance Optimized**: Uses Spark SQL adaptive query execution
- **Easy Setup**: Python-based implementation for easier deployment and testing

## Project Structure

```
spark_jobs/                     # Main job package
├── __init__.py
├── size_calculation_job.py     # Main job implementation
└── utils.py                    # Utility functions
tests/                          # Test files
├── __init__.py
└── test_size_calculation_job.py # Unit tests
config/                         # Configuration files
└── job.conf                    # Job configuration
sample_data/                    # Sample data for testing
logs/                           # Log output directory
requirements.txt                # Python dependencies
create_sample_data.py          # Script to generate test data
run_job.py                     # Job runner script
```

## Configuration

The job uses ConfigParser for configuration management. Key configuration parameters:

- `spark_app_name`: Application name
- `input_path`: Input data path
- `input_format`: Input format (parquet, json, csv, delta)
- `output_path`: Output path for results
- `output_format`: Output format
- `output_mode`: Output mode (overwrite, append, etc.)

Configuration can be overridden using environment variables or command-line arguments.

## Usage

### Installing Dependencies

```bash
pip install -r requirements.txt
```

### Creating Sample Data

```bash
python create_sample_data.py
```

### Running the Job

#### Using the runner script:
```bash
python run_job.py sample_data/employees.parquet output
```

#### Direct execution:
```bash
python spark_jobs/size_calculation_job.py \
  --input-path sample_data/employees.parquet \
  --output-path output \
  --input-format parquet \
  --output-format parquet
```

#### With spark-submit:
```bash
spark-submit \
  --master local[*] \
  spark_jobs/size_calculation_job.py \
  --input-path sample_data/employees.parquet \
  --output-path output
```

### Environment Variables

Configuration can be overridden using environment variables:
```bash
export INPUT_PATH="/path/to/input/data"
export OUTPUT_PATH="/path/to/output/results"
export INPUT_FORMAT="parquet"
export OUTPUT_FORMAT="parquet"
export SPARK_MASTER="local[*]"
```

## Output

The job produces two sets of results:

### Size Metrics (`size_metrics/`)
- `tableName`: Name extracted from input path
- `recordCount`: Total number of records
- `sizeInBytes`: Estimated size in bytes
- `columnCount`: Number of columns
- `partitionCount`: Number of partitions
- `avgRecordSizeBytes`: Average record size
- `timestamp`: Processing timestamp

### Column Metrics (`column_metrics/`)
- `columnName`: Column name
- `dataType`: Data type
- `nullCount`: Number of null values
- `distinctCount`: Number of distinct values
- `avgLength`: Average length (for string columns)

## Testing

Run tests with:

```bash
pytest tests/ -v
```

Or run specific test files:
```bash
pytest tests/test_size_calculation_job.py -v
```

The test suite includes:
- Basic functionality tests
- Empty dataset handling
- Column metrics validation
- Different input format testing
- Full job execution tests

## Development

### Prerequisites

- Python 3.8+
- PySpark 3.4.1
- Java 8 or 11 (for Spark)

### Adding New Features

1. Implement new functionality in the main job class
2. Add corresponding tests
3. Update configuration as needed
4. Update documentation

## Monitoring and Logging

The job includes comprehensive logging using Logback. Logs are written to:
- Console (INFO level and above)
- File: `logs/spark-size-job.log` (with rotation)

Spark-specific logging is reduced to WARN level to minimize noise.

## Performance Considerations

- Uses Spark SQL adaptive query execution for optimal performance
- Implements sampling for size estimation on large datasets
- Configurable batch processing
- Efficient column-level metric calculations

## Error Handling

The job implements comprehensive error handling:
- Input validation for file formats and paths
- Graceful handling of missing or corrupt data
- Detailed error logging with stack traces
- Proper resource cleanup on failure

## Contributing

1. Follow the existing code style and patterns
2. Add tests for new functionality
3. Update documentation as needed
4. Ensure all tests pass before submitting PR