"""
Utility functions for Spark jobs
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, current_date, col, length, avg
from typing import Optional, Set


def estimate_dataframe_size(df: DataFrame, sample_fraction: float = 0.01) -> int:
    """
    Estimate the size of a DataFrame in bytes using sampling
    
    Args:
        df: Input DataFrame
        sample_fraction: Fraction of data to sample for estimation
        
    Returns:
        Estimated size in bytes
    """
    record_count = df.count()
    if record_count == 0:
        return 0
    
    # Sample the data
    sample_df = df.sample(False, sample_fraction, seed=42)
    sample_count = sample_df.count()
    
    if sample_count == 0:
        return 0
    
    # Calculate average row size from sample
    sample_data = sample_df.collect()
    total_sample_size = sum(
        len(str(row).encode('utf-8')) for row in sample_data
    )
    
    avg_row_size = total_sample_size / sample_count
    return int(avg_row_size * record_count)


def validate_dataframe(df: DataFrame, expected_columns: Optional[Set[str]] = None) -> None:
    """
    Validate that a DataFrame meets expected criteria
    
    Args:
        df: DataFrame to validate
        expected_columns: Optional set of expected column names
        
    Raises:
        ValueError: If validation fails
    """
    if df.count() == 0:
        raise ValueError("DataFrame is empty")
    
    if expected_columns:
        actual_columns = set(df.columns)
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            raise ValueError(f"Missing expected columns: {missing_columns}")


def add_processing_metadata(df: DataFrame) -> DataFrame:
    """
    Add metadata columns to track processing information
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with added metadata columns
    """
    return df.withColumn("processing_timestamp", current_timestamp()) \
             .withColumn("processing_date", current_date())


def calculate_column_statistics(df: DataFrame, column_name: str) -> dict:
    """
    Calculate detailed statistics for a specific column
    
    Args:
        df: Input DataFrame
        column_name: Name of column to analyze
        
    Returns:
        Dictionary with column statistics
    """
    column = col(column_name)
    data_type = str(df.schema[column_name].dataType)
    
    stats = {
        "column_name": column_name,
        "data_type": data_type,
        "total_count": df.count(),
        "null_count": df.filter(column.isNull()).count(),
        "distinct_count": df.select(column).distinct().count()
    }
    
    # Add string-specific statistics
    if "string" in data_type.lower():
        avg_len = df.select(avg(length(column)).alias("avg_length")).collect()[0]["avg_length"]
        stats["avg_length"] = float(avg_len) if avg_len is not None else None
        
        # Calculate min/max length
        lengths_df = df.select(length(column).alias("length")).filter(column.isNotNull())
        if lengths_df.count() > 0:
            length_stats = lengths_df.agg(
                {"length": "min"}, {"length": "max"}
            ).collect()[0]
            stats["min_length"] = length_stats["min(length)"]
            stats["max_length"] = length_stats["max(length)"]
    
    return stats


def get_dataframe_schema_info(df: DataFrame) -> dict:
    """
    Get comprehensive schema information for a DataFrame
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary with schema information
    """
    schema_info = {
        "column_count": len(df.columns),
        "columns": []
    }
    
    for field in df.schema.fields:
        column_info = {
            "name": field.name,
            "data_type": str(field.dataType),
            "nullable": field.nullable,
            "metadata": dict(field.metadata) if field.metadata else {}
        }
        schema_info["columns"].append(column_info)
    
    return schema_info