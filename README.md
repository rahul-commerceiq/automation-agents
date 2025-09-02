# Spark Size Calculation Job (CP-1)

A scalable Apache Spark job for calculating size metrics from large datasets. This job processes input data to compute comprehensive size statistics including basic metrics, percentiles, and group-based analysis.

## Features

- **Basic Size Metrics**: Total, average, min, max, count, standard deviation
- **Percentile Analysis**: Configurable percentile calculations (25th, 50th, 75th, 90th, 95th, 99th)
- **Group Analysis**: Size metrics grouped by categorical dimensions
- **Multiple Data Formats**: Support for Parquet, CSV, and JSON input/output
- **Data Validation**: Robust handling of null, negative, and malformed data
- **Configurable**: Flexible configuration via files or command-line arguments
- **Scalable**: Optimized for large datasets using Spark's distributed computing

## Quick Start

### Prerequisites

- Apache Spark 3.x
- Python 3.8+
- Required Python packages (install via `pip install -r requirements.txt`)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd spark-size-job

# Install dependencies
pip install -r requirements.txt
```

### Running the Job

#### Basic Usage

```bash
./submit_job.sh --input-path /path/to/input --output-path /path/to/output
```

#### Advanced Usage

```bash
./submit_job.sh \
    --input-path /data/input.parquet \
    --output-path /data/results \
    --size-column file_size \
    --input-format parquet \
    --output-format csv \
    --spark-master yarn \
    --executor-memory 4g \
    --driver-memory 2g
```

#### Using Configuration File

```bash
./submit_job.sh --config custom_config.conf
```

### Configuration

The job can be configured via configuration files or command-line arguments. See `src/main/resources/application.conf` for the default configuration.

#### Configuration Sections

- **[spark]**: Spark session configuration
- **[data]**: Input/output data configuration  
- **[calculation]**: Size calculation parameters

#### Example Configuration

```ini
[spark]
app_name = SizeCalculationJob
master = yarn
executor_memory = 4g
driver_memory = 2g
executor_cores = 4

[data]
input_format = parquet
output_format = parquet
input_path = /data/input
output_path = /data/output

[calculation]
size_column = file_size
group_by_columns = department,category
calculate_percentiles = true
percentiles = 25,50,75,90,95,99
```

## Input Data Schema

The job expects input data with at least a numeric size column. Additional columns can be used for grouping analysis.

### Required Columns
- Size column (configurable name, default: "size")

### Optional Columns
- Any categorical columns for group analysis

### Example Schema
```
id: string
size: double
category: string
department: string
timestamp: timestamp
```

## Output

The job produces two types of output:

### 1. Structured Metrics (`/output/metrics/`)
Detailed metrics in the specified format (Parquet/CSV/JSON) containing:
- Basic statistics (total, average, min, max, count, stddev)
- Percentile values
- Formatted human-readable values

### 2. Summary Report (`/output/summary/`)
Human-readable text summary including:
- Overall size statistics
- Percentile analysis
- Group-based analysis (if configured)

## Testing

Run the test suite to verify functionality:

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src/main/python --cov-report=html

# Run specific test file
pytest tests/test_size_calculator.py -v
```

## Performance Tuning

### For Large Datasets

1. **Increase Resources**:
   ```bash
   ./submit_job.sh \
       --executor-memory 8g \
       --driver-memory 4g \
       --spark-master yarn
   ```

2. **Optimize Partitioning**:
   - Ensure input data is properly partitioned
   - Consider repartitioning if data is skewed

3. **Enable Compression**:
   - Use compressed input formats (Parquet with Snappy)
   - Configure Spark compression settings

### Memory Optimization

- Monitor Spark UI for memory usage patterns
- Adjust executor memory based on data size
- Use `.cache()` for DataFrames accessed multiple times

## Troubleshooting

### Common Issues

1. **Out of Memory Errors**:
   - Increase executor memory
   - Reduce number of executor cores
   - Check for data skew

2. **Slow Performance**:
   - Verify input data partitioning
   - Check for expensive operations in hot paths
   - Consider sampling for development/testing

3. **Schema Errors**:
   - Verify column names match configuration
   - Check data types are compatible
   - Validate input data format

## Development

### Project Structure

```
spark-size-job/
├── src/main/python/          # Source code
│   ├── size_calculator.py    # Main Spark job
│   ├── config.py            # Configuration management
│   └── utils.py             # Utility functions
├── src/main/resources/      # Configuration files
├── tests/                   # Unit tests
├── requirements.txt         # Python dependencies
├── submit_job.sh           # Job submission script
└── README.md               # This file
```

### Adding New Features

1. Implement new functionality in appropriate module
2. Add corresponding unit tests
3. Update configuration if needed
4. Update documentation

## Contributing

1. Follow PEP 8 style guidelines
2. Add unit tests for new functionality
3. Update documentation
4. Test with sample data before submitting

## License

[Add appropriate license information]