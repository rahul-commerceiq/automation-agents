# CP-1: Implement Spark job for size calculation

## Overview
This PR implements CP-1 requirements for creating a Spark job that calculates size metrics from input data.

## Changes Made

- **Core Implementation**: Complete Spark job for size calculations with support for basic metrics, percentiles, and group analysis
- **Configuration Management**: Flexible configuration system supporting file-based and command-line configuration  
- **Data Processing**: Robust data validation, cleaning, and error handling
- **Multiple Formats**: Support for Parquet, CSV, and JSON input/output formats
- **Testing**: Comprehensive unit test suite with sample data generation
- **Documentation**: Complete README with usage examples and troubleshooting guide
- **Deployment**: Job submission script with configurable parameters

## Key Features

1. **Basic Size Metrics**: Total, average, min, max, count, standard deviation
2. **Percentile Analysis**: Configurable percentile calculations (25th, 50th, 75th, 90th, 95th, 99th)
3. **Group Analysis**: Size metrics grouped by categorical dimensions (e.g., category, department)
4. **Data Validation**: Handles null values, negative sizes, and malformed data
5. **Scalability**: Optimized for large datasets using Spark's distributed computing
6. **Flexibility**: Configurable via files or command-line arguments

## Files Added

- `plan.md` - Implementation plan and technical requirements
- `src/main/python/size_calculator.py` - Main Spark job implementation
- `src/main/python/config.py` - Configuration management
- `src/main/python/utils.py` - Utility functions and helpers
- `src/main/resources/application.conf` - Default configuration
- `tests/` - Comprehensive unit test suite
- `submit_job.sh` - Job submission script
- `generate_sample_data.py` - Sample data generator for testing
- `run_example.py` - Example runner script
- `requirements.txt` - Python dependencies
- `README.md` - Documentation and usage guide

## Usage Example

```bash
# Basic usage
./submit_job.sh --input-path /data/input.parquet --output-path /data/results

# Advanced usage with custom configuration
./submit_job.sh \
    --input-path /data/files.csv \
    --output-path /data/size_analysis \
    --size-column file_size \
    --input-format csv \
    --spark-master yarn \
    --executor-memory 4g
```

## Testing

Run the test suite:
```bash
pytest tests/ -v
```

Generate and test with sample data:
```bash
python generate_sample_data.py --records 10000
python run_example.py
```

## References

- Issue: CP-1
- Confluence Documentation: [Draft - Writing Spark Job](https://boomerang.atlassian.net/wiki/spaces/DTP/pages/3303473270/Draft+-+Writing+spark+job)

## Ready for Review

This implementation provides a complete, production-ready Spark job for size calculations that can handle large datasets efficiently and provides comprehensive metrics analysis.