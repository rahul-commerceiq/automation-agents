"""
Unit tests for the Spark size calculation job.
"""

import pytest
import tempfile
import shutil
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'main', 'python'))

from size_calculator import SizeCalculationJob
from config import SparkJobConfig
from utils import validate_dataframe_schema, clean_numeric_data, validate_size_data


class TestSizeCalculationJob:
    """Test class for SizeCalculationJob."""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create a Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestSizeCalculation") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        yield spark
        spark.stop()
    
    @pytest.fixture
    def sample_data(self, spark_session):
        """Create sample data for testing."""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("size", DoubleType(), True),
            StructField("category", StringType(), True)
        ])
        
        data = [
            ("1", 100.0, "A"),
            ("2", 200.0, "A"),
            ("3", 300.0, "B"),
            ("4", 400.0, "B"),
            ("5", 150.0, "A"),
            ("6", 250.0, "C"),
            ("7", 350.0, "C"),
            ("8", 50.0, "A"),
            ("9", 500.0, "B"),
            ("10", 75.0, "C")
        ]
        
        return spark_session.createDataFrame(data, schema)
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def test_config(self, temp_dir):
        """Create a test configuration."""
        config = SparkJobConfig()
        config.config.set('data', 'input_path', f"{temp_dir}/input")
        config.config.set('data', 'output_path', f"{temp_dir}/output")
        return config
    
    def test_spark_session_creation(self, test_config):
        """Test Spark session creation."""
        job = SizeCalculationJob(test_config)
        spark = job.create_spark_session()
        
        assert spark is not None
        assert spark.sparkContext.appName == "SizeCalculationJob"
        
        spark.stop()
    
    def test_basic_metrics_calculation(self, spark_session, sample_data, test_config):
        """Test basic metrics calculation."""
        job = SizeCalculationJob(test_config)
        job.spark = spark_session
        
        metrics = job.calculate_basic_metrics(sample_data, "size")
        
        assert metrics['total_size'] == 2375.0  # Sum of all sizes
        assert metrics['total_records'] == 10
        assert metrics['min_size'] == 50.0
        assert metrics['max_size'] == 500.0
        assert metrics['average_size'] == 237.5
        assert metrics['stddev_size'] > 0
    
    def test_percentiles_calculation(self, spark_session, sample_data, test_config):
        """Test percentile calculation."""
        job = SizeCalculationJob(test_config)
        job.spark = spark_session
        
        percentiles = job.calculate_percentiles(sample_data, "size", [0.25, 0.5, 0.75])
        
        assert '25' in percentiles
        assert '50' in percentiles
        assert '75' in percentiles
        assert all(isinstance(v, float) for v in percentiles.values())
    
    def test_group_metrics_calculation(self, spark_session, sample_data, test_config):
        """Test group metrics calculation."""
        job = SizeCalculationJob(test_config)
        job.spark = spark_session
        
        group_metrics = job.calculate_group_metrics(sample_data, "size", ["category"])
        
        assert len(group_metrics) == 3  # Categories A, B, C
        assert 'A' in group_metrics
        assert 'B' in group_metrics
        assert 'C' in group_metrics
        
        # Check category A metrics (sizes: 100, 200, 150, 50)
        assert group_metrics['A']['count'] == 4
        assert group_metrics['A']['total'] == 500.0
        assert group_metrics['A']['average'] == 125.0
    
    def test_data_validation(self, spark_session):
        """Test data validation functions."""
        schema = StructType([
            StructField("size", DoubleType(), True),
            StructField("category", StringType(), True)
        ])
        
        df = spark_session.createDataFrame([
            (100.0, "A"),
            (None, "B"),
            (-50.0, "C"),
            (200.0, "A")
        ], schema)
        
        # Test schema validation
        assert validate_dataframe_schema(df, ["size", "category"])
        
        with pytest.raises(ValueError):
            validate_dataframe_schema(df, ["missing_column"])
        
        # Test data cleaning
        clean_df = clean_numeric_data(df, ["size"])
        sizes = [row['size'] for row in clean_df.select("size").collect()]
        
        # Should replace None with 0 and negative with 0
        assert 0.0 in sizes  # None replaced with 0
        assert 0.0 in sizes  # Negative replaced with 0
        assert 100.0 in sizes
        assert 200.0 in sizes
        assert all(s >= 0 for s in sizes)  # No negative values
    
    def test_size_data_validation(self, spark_session, sample_data):
        """Test size data validation."""
        validation_results = validate_size_data(sample_data, "size")
        
        assert validation_results['total_rows'] == 10
        assert validation_results['null_rows'] == 0
        assert validation_results['negative_rows'] == 0
        assert validation_results['valid_rows'] == 10
        assert 'basic_stats' in validation_results


class TestConfiguration:
    """Test class for configuration management."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = SparkJobConfig()
        
        spark_config = config.get_spark_config()
        assert spark_config['spark.app.name'] == 'SizeCalculationJob'
        assert spark_config['spark.master'] == 'local[*]'
        
        data_config = config.get_data_config()
        assert data_config['input_format'] == 'parquet'
        assert data_config['output_format'] == 'parquet'
        
        calc_config = config.get_calculation_config()
        assert calc_config['size_column'] == 'size'
        assert calc_config['calculate_percentiles'] == True
    
    def test_config_file_loading(self, tmp_path):
        """Test loading configuration from file."""
        config_file = tmp_path / "test.conf"
        config_file.write_text("""
[spark]
app_name = TestJob
master = yarn

[data]
input_path = /test/input
output_path = /test/output

[calculation]
size_column = file_size
        """)
        
        config = SparkJobConfig(str(config_file))
        
        spark_config = config.get_spark_config()
        assert spark_config['spark.app.name'] == 'TestJob'
        assert spark_config['spark.master'] == 'yarn'
        
        data_config = config.get_data_config()
        assert data_config['input_path'] == '/test/input'
        assert data_config['output_path'] == '/test/output'
        
        calc_config = config.get_calculation_config()
        assert calc_config['size_column'] == 'file_size'


if __name__ == "__main__":
    pytest.main([__file__])