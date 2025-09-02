"""
Unit tests for SizeCalculationJob
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from spark_jobs.size_calculation_job import SizeCalculationJob


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test-size-calculation") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


@pytest.fixture
def sample_config():
    """Sample configuration for testing"""
    return {
        'spark_app_name': 'test-size-job',
        'batch_size': '100',
        'metrics_enabled': 'true'
    }


@pytest.fixture
def sample_data(spark_session):
    """Create sample test data"""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("salary", DoubleType(), True)
    ])
    
    data = [
        (1, "John Doe", 25, "john@example.com", 50000.0),
        (2, "Jane Smith", 30, "jane@example.com", 60000.0),
        (3, "Bob Johnson", None, "bob@example.com", 55000.0),
        (4, None, 35, None, 70000.0),
        (5, "Alice Brown", 28, "alice@example.com", None)
    ]
    
    return spark_session.createDataFrame(data, schema)


class TestSizeCalculationJob:
    """Test cases for SizeCalculationJob"""
    
    def test_calculate_size_metrics(self, spark_session, sample_config, sample_data):
        """Test size metrics calculation"""
        job = SizeCalculationJob(spark_session, sample_config)
        
        metrics = job._calculate_size_metrics(sample_data, "test_table")
        
        assert metrics["table_name"] == "test_table"
        assert metrics["record_count"] == 5
        assert metrics["column_count"] == 5
        assert metrics["partition_count"] > 0
        assert metrics["avg_record_size_bytes"] > 0.0
        assert metrics["size_in_bytes"] > 0
        assert "timestamp" in metrics
    
    def test_calculate_column_metrics(self, spark_session, sample_config, sample_data):
        """Test column metrics calculation"""
        job = SizeCalculationJob(spark_session, sample_config)
        
        column_metrics = job._calculate_column_metrics(sample_data)
        
        assert len(column_metrics) == 5
        
        # Check specific column metrics
        name_metrics = next(m for m in column_metrics if m["column_name"] == "name")
        assert name_metrics["null_count"] == 1  # One null value
        assert name_metrics["distinct_count"] == 4  # 4 distinct non-null values
        assert name_metrics["avg_length"] is not None  # Should have average length
        
        id_metrics = next(m for m in column_metrics if m["column_name"] == "id")
        assert id_metrics["null_count"] == 1  # One null value
        assert id_metrics["distinct_count"] == 4  # 4 distinct non-null values
        assert id_metrics["avg_length"] is None  # No avg length for integers
    
    def test_handle_empty_dataset(self, spark_session, sample_config):
        """Test handling of empty datasets"""
        job = SizeCalculationJob(spark_session, sample_config)
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True)
        ])
        
        empty_df = spark_session.createDataFrame([], schema)
        
        metrics = job._calculate_size_metrics(empty_df, "empty_table")
        
        assert metrics["record_count"] == 0
        assert metrics["column_count"] == 2
        assert metrics["avg_record_size_bytes"] == 0.0
        assert metrics["size_in_bytes"] == 0
    
    def test_extract_table_name(self, spark_session, sample_config):
        """Test table name extraction from paths"""
        job = SizeCalculationJob(spark_session, sample_config)
        
        assert job._extract_table_name("path/to/my_table") == "my_table"
        assert job._extract_table_name("path/to/my_table/") == "my_table"
        assert job._extract_table_name("my_table") == "my_table"
        assert job._extract_table_name("") == "unknown_table"
    
    def test_full_job_run(self, spark_session, sample_config, sample_data):
        """Test complete job execution"""
        job = SizeCalculationJob(spark_session, sample_config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = f"{temp_dir}/input"
            output_path = f"{temp_dir}/output"
            
            # Save sample data
            sample_data.write.mode("overwrite").parquet(input_path)
            
            # Run the job
            job.run(input_path, output_path, "parquet", "parquet", "overwrite")
            
            # Verify output exists
            size_metrics_path = f"{output_path}/size_metrics"
            column_metrics_path = f"{output_path}/column_metrics"
            
            assert Path(size_metrics_path).exists()
            assert Path(column_metrics_path).exists()
            
            # Read and verify results
            size_results = spark_session.read.parquet(size_metrics_path)
            assert size_results.count() == 1
            assert size_results.select("record_count").collect()[0]["record_count"] == 5
            
            column_results = spark_session.read.parquet(column_metrics_path)
            assert column_results.count() == 5  # 5 columns
    
    def test_different_input_formats(self, spark_session, sample_config, sample_data):
        """Test different input formats"""
        job = SizeCalculationJob(spark_session, sample_config)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test JSON format
            json_input_path = f"{temp_dir}/json_input"
            json_output_path = f"{temp_dir}/json_output"
            
            sample_data.write.mode("overwrite").json(json_input_path)
            job.run(json_input_path, json_output_path, "json", "json", "overwrite")
            
            json_results = spark_session.read.json(f"{json_output_path}/size_metrics")
            assert json_results.count() == 1