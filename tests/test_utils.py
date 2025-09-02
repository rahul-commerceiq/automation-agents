"""
Unit tests for utility functions.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'main', 'python'))

from utils import format_size_bytes, create_summary_report, validate_dataframe_schema


class TestUtilityFunctions:
    """Test class for utility functions."""
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        """Create a Spark session for testing."""
        spark = SparkSession.builder \
            .appName("TestUtils") \
            .master("local[1]") \
            .getOrCreate()
        
        yield spark
        spark.stop()
    
    def test_format_size_bytes(self):
        """Test size formatting function."""
        assert format_size_bytes(0) == "0 B"
        assert format_size_bytes(512) == "512.00 B"
        assert format_size_bytes(1024) == "1.00 KB"
        assert format_size_bytes(1536) == "1.50 KB"
        assert format_size_bytes(1048576) == "1.00 MB"
        assert format_size_bytes(1073741824) == "1.00 GB"
        assert format_size_bytes(1099511627776) == "1.00 TB"
    
    def test_create_summary_report(self):
        """Test summary report creation."""
        results = {
            'total_records': 1000,
            'total_size': 1073741824,  # 1 GB
            'average_size': 1048576,   # 1 MB
            'min_size': 1024,          # 1 KB
            'max_size': 10485760,      # 10 MB
            'percentiles': {
                '50': 2097152,         # 2 MB
                '90': 5242880,         # 5 MB
                '95': 7340032          # 7 MB
            },
            'group_analysis': {
                'category_A': {
                    'total': 536870912,  # 512 MB
                    'count': 500,
                    'average': 1073741   # ~1 MB
                },
                'category_B': {
                    'total': 536870912,  # 512 MB
                    'count': 500,
                    'average': 1073741   # ~1 MB
                }
            }
        }
        
        report = create_summary_report(results)
        
        assert "Total Records Processed: 1,000" in report
        assert "Total Size: 1.00 GB" in report
        assert "Average Size: 1.00 MB" in report
        assert "50th percentile: 2.00 MB" in report
        assert "category_A: Total=512.00 MB" in report
    
    def test_validate_dataframe_schema_success(self, spark_session):
        """Test successful schema validation."""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("size", DoubleType(), True),
            StructField("category", StringType(), True)
        ])
        
        df = spark_session.createDataFrame([("1", 100.0, "A")], schema)
        
        # Should pass validation
        assert validate_dataframe_schema(df, ["id", "size"]) == True
        assert validate_dataframe_schema(df, ["size", "category"]) == True
    
    def test_validate_dataframe_schema_failure(self, spark_session):
        """Test schema validation failure."""
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("size", DoubleType(), True)
        ])
        
        df = spark_session.createDataFrame([("1", 100.0)], schema)
        
        # Should fail validation for missing column
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_dataframe_schema(df, ["id", "size", "missing_column"])


if __name__ == "__main__":
    pytest.main([__file__])