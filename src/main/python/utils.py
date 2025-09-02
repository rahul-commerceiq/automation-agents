"""
Utility functions for the Spark size calculation job.
"""

import logging
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull
from pyspark.sql.types import NumericType


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Set up logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def validate_dataframe_schema(df: DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that the DataFrame contains all required columns.
    
    Args:
        df: Input DataFrame
        required_columns: List of required column names
        
    Returns:
        bool: True if all required columns are present
    """
    df_columns = set(df.columns)
    required_columns_set = set(required_columns)
    
    missing_columns = required_columns_set - df_columns
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    return True


def clean_numeric_data(df: DataFrame, numeric_columns: List[str]) -> DataFrame:
    """
    Clean numeric data by handling null, NaN, and negative values.
    
    Args:
        df: Input DataFrame
        numeric_columns: List of numeric column names to clean
        
    Returns:
        DataFrame: Cleaned DataFrame
    """
    logger = logging.getLogger(__name__)
    
    for col_name in numeric_columns:
        if col_name in df.columns:
            # Check if column is numeric
            if not isinstance(df.schema[col_name].dataType, NumericType):
                logger.warning(f"Column {col_name} is not numeric, skipping cleaning")
                continue
                
            # Count null/NaN values before cleaning
            null_count = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
            if null_count > 0:
                logger.info(f"Found {null_count} null/NaN values in column {col_name}")
            
            # Replace null/NaN with 0 and ensure non-negative values for size calculations
            df = df.withColumn(
                col_name,
                when(col(col_name).isNull() | isnan(col(col_name)), 0)
                .when(col(col_name) < 0, 0)
                .otherwise(col(col_name))
            )
    
    return df


def validate_size_data(df: DataFrame, size_column: str) -> Dict[str, Any]:
    """
    Validate size data and return basic statistics.
    
    Args:
        df: Input DataFrame
        size_column: Name of the size column to validate
        
    Returns:
        Dict: Validation results and basic statistics
    """
    logger = logging.getLogger(__name__)
    
    # Basic validation
    total_rows = df.count()
    null_rows = df.filter(col(size_column).isNull()).count()
    negative_rows = df.filter(col(size_column) < 0).count()
    zero_rows = df.filter(col(size_column) == 0).count()
    
    # Calculate basic statistics
    stats = df.select(size_column).describe().collect()
    stats_dict = {row['summary']: float(row[size_column]) if row[size_column] else 0 
                  for row in stats}
    
    validation_results = {
        'total_rows': total_rows,
        'null_rows': null_rows,
        'negative_rows': negative_rows,
        'zero_rows': zero_rows,
        'valid_rows': total_rows - null_rows - negative_rows,
        'basic_stats': stats_dict
    }
    
    logger.info(f"Data validation results: {validation_results}")
    
    return validation_results


def format_size_bytes(size_bytes: float) -> str:
    """
    Format bytes into human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        str: Human-readable size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while size_bytes >= 1024.0 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.2f} {size_names[i]}"


def create_summary_report(results: Dict[str, Any]) -> str:
    """
    Create a summary report of the size calculation results.
    
    Args:
        results: Dictionary containing calculation results
        
    Returns:
        str: Formatted summary report
    """
    report_lines = [
        "=== Size Calculation Summary Report ===",
        f"Total Records Processed: {results.get('total_records', 0):,}",
        f"Total Size: {format_size_bytes(results.get('total_size', 0))}",
        f"Average Size: {format_size_bytes(results.get('average_size', 0))}",
        f"Minimum Size: {format_size_bytes(results.get('min_size', 0))}",
        f"Maximum Size: {format_size_bytes(results.get('max_size', 0))}",
        "",
        "=== Percentile Analysis ===",
    ]
    
    percentiles = results.get('percentiles', {})
    for percentile, value in percentiles.items():
        report_lines.append(f"{percentile}th percentile: {format_size_bytes(value)}")
    
    if 'group_analysis' in results:
        report_lines.extend([
            "",
            "=== Group Analysis ===",
        ])
        for group, stats in results['group_analysis'].items():
            report_lines.append(f"{group}: Total={format_size_bytes(stats.get('total', 0))}, "
                              f"Count={stats.get('count', 0):,}, "
                              f"Avg={format_size_bytes(stats.get('average', 0))}")
    
    return "\n".join(report_lines)