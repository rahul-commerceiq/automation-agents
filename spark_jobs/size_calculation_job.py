#!/usr/bin/env python3
"""
Spark Job for calculating data size metrics

This job processes input data and calculates various size-related metrics including:
- Total record count
- Data size in bytes  
- Column-level statistics
- Partition-level metrics

Usage:
    python size_calculation_job.py --input-path <path> --output-path <path>
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import configparser

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, length, when, 
    isnan, isnull, current_timestamp, current_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    LongType, DoubleType, TimestampType
)


class SizeCalculationJob:
    """Main Spark job class for calculating data size metrics"""
    
    def __init__(self, spark_session: SparkSession, config: Dict):
        self.spark = spark_session
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def run(self, input_path: str, output_path: str, 
            input_format: str = "parquet", output_format: str = "parquet",
            output_mode: str = "overwrite") -> None:
        """
        Main execution method for the size calculation job
        
        Args:
            input_path: Path to input data
            output_path: Path to save results
            input_format: Input data format (parquet, json, csv, delta)
            output_format: Output format for results
            output_mode: Output mode (overwrite, append, etc.)
        """
        try:
            self.logger.info("Starting Size Calculation Job")
            self.logger.info(f"Input path: {input_path}")
            self.logger.info(f"Output path: {output_path}")
            
            # Load input data
            self.logger.info("Loading input data")
            input_data = self._load_data(input_path, input_format)
            
            # Calculate size metrics
            self.logger.info("Calculating size metrics")
            size_metrics = self._calculate_size_metrics(input_data, input_path)
            
            # Calculate column metrics
            self.logger.info("Calculating column metrics")
            column_metrics = self._calculate_column_metrics(input_data)
            
            # Save results
            self.logger.info("Saving results")
            self._save_results(size_metrics, column_metrics, output_path, 
                             output_format, output_mode)
            
            self.logger.info("Size calculation completed successfully")
            
        except Exception as e:
            self.logger.error(f"Job failed with exception: {e}")
            raise
    
    def _load_data(self, path: str, format_type: str) -> DataFrame:
        """Load data from the specified path and format"""
        try:
            if format_type.lower() == "parquet":
                df = self.spark.read.parquet(path)
            elif format_type.lower() == "json":
                df = self.spark.read.json(path)
            elif format_type.lower() == "csv":
                df = self.spark.read.option("header", "true").csv(path)
            elif format_type.lower() == "delta":
                df = self.spark.read.format("delta").load(path)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
            
            self.logger.info(f"Successfully loaded data from {path}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load data from {path}: {e}")
            raise
    
    def _calculate_size_metrics(self, df: DataFrame, table_name: str) -> Dict:
        """Calculate table-level size metrics"""
        record_count = df.count()
        column_count = len(df.columns)
        partition_count = df.rdd.getNumPartitions()
        
        # Estimate size in bytes using sampling for large datasets
        if record_count > 0:
            sample_size = min(1000, record_count)
            sample_fraction = sample_size / record_count if record_count > 0 else 0
            
            if sample_fraction < 1.0:
                sample_df = df.sample(False, sample_fraction, seed=42)
            else:
                sample_df = df
                
            # Estimate average record size
            sample_data = sample_df.collect()
            if sample_data:
                total_sample_size = sum(
                    len(str(row).encode('utf-8')) for row in sample_data
                )
                avg_record_size_bytes = total_sample_size / len(sample_data)
            else:
                avg_record_size_bytes = 0.0
        else:
            avg_record_size_bytes = 0.0
        
        estimated_size_bytes = int(avg_record_size_bytes * record_count)
        
        return {
            "table_name": self._extract_table_name(table_name),
            "record_count": record_count,
            "size_in_bytes": estimated_size_bytes,
            "column_count": column_count,
            "partition_count": partition_count,
            "avg_record_size_bytes": avg_record_size_bytes,
            "timestamp": datetime.now().isoformat()
        }
    
    def _calculate_column_metrics(self, df: DataFrame) -> List[Dict]:
        """Calculate column-level metrics"""
        column_metrics = []
        
        for col_name in df.columns:
            column = col(col_name)
            data_type = str(df.schema[col_name].dataType)
            
            # Calculate null count
            null_count = df.select(
                spark_sum(when(column.isNull(), 1).otherwise(0)).alias("null_count")
            ).collect()[0]["null_count"]
            
            # Calculate distinct count
            distinct_count = df.select(column).distinct().count()
            
            # Calculate average length for string columns
            avg_length = None
            if "string" in data_type.lower():
                avg_len_result = df.select(
                    avg(length(column)).alias("avg_length")
                ).collect()[0]["avg_length"]
                avg_length = float(avg_len_result) if avg_len_result is not None else None
            
            column_metrics.append({
                "column_name": col_name,
                "data_type": data_type,
                "null_count": null_count,
                "distinct_count": distinct_count,
                "avg_length": avg_length
            })
        
        return column_metrics
    
    def _save_results(self, size_metrics: Dict, column_metrics: List[Dict],
                     output_path: str, format_type: str, mode: str) -> None:
        """Save calculation results"""
        
        # Create DataFrames from results
        size_metrics_df = self.spark.createDataFrame([size_metrics])
        column_metrics_df = self.spark.createDataFrame(column_metrics)
        
        # Save size metrics
        size_output_path = f"{output_path}/size_metrics"
        self._save_dataframe(size_metrics_df, size_output_path, format_type, mode)
        
        # Save column metrics  
        column_output_path = f"{output_path}/column_metrics"
        self._save_dataframe(column_metrics_df, column_output_path, format_type, mode)
        
        self.logger.info(f"Results saved to {output_path}")
    
    def _save_dataframe(self, df: DataFrame, path: str, format_type: str, mode: str) -> None:
        """Save a DataFrame to the specified path and format"""
        try:
            writer = df.write.mode(mode)
            
            if format_type.lower() == "parquet":
                writer.parquet(path)
            elif format_type.lower() == "json":
                writer.json(path)
            elif format_type.lower() == "csv":
                writer.option("header", "true").csv(path)
            elif format_type.lower() == "delta":
                writer.format("delta").save(path)
            else:
                raise ValueError(f"Unsupported output format: {format_type}")
            
            self.logger.info(f"Successfully saved data to {path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save data to {path}: {e}")
            raise
    
    def _extract_table_name(self, path: str) -> str:
        """Extract table name from file path"""
        clean_path = path.rstrip('/')
        return Path(clean_path).name if clean_path else "unknown_table"


def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/spark_size_job.log', mode='a')
        ]
    )


def load_config(config_path: str = "config/job.conf") -> Dict:
    """Load configuration from file"""
    config = configparser.ConfigParser()
    
    # Set defaults
    defaults = {
        'spark_app_name': 'size-calculation-job',
        'spark_master': 'local[*]',
        'input_path': 'input/',
        'input_format': 'parquet',
        'output_path': 'output/',
        'output_format': 'parquet',
        'output_mode': 'overwrite',
        'batch_size': '10000',
        'metrics_enabled': 'true'
    }
    
    # Try to read config file if it exists
    if Path(config_path).exists():
        config.read(config_path)
        
    # Merge with environment variables and defaults
    final_config = {}
    for key, default_value in defaults.items():
        env_key = key.upper()
        if config.has_option('DEFAULT', key):
            final_config[key] = config.get('DEFAULT', key)
        else:
            final_config[key] = default_value
    
    return final_config


def create_spark_session(config: Dict) -> SparkSession:
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(config['spark_app_name']) \
        .master(config['spark_master']) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Spark Size Calculation Job')
    parser.add_argument('--input-path', required=True, help='Input data path')
    parser.add_argument('--output-path', required=True, help='Output results path')
    parser.add_argument('--input-format', default='parquet', 
                       choices=['parquet', 'json', 'csv', 'delta'],
                       help='Input data format')
    parser.add_argument('--output-format', default='parquet',
                       choices=['parquet', 'json', 'csv', 'delta'], 
                       help='Output format')
    parser.add_argument('--output-mode', default='overwrite',
                       choices=['overwrite', 'append', 'ignore', 'error'],
                       help='Output mode')
    parser.add_argument('--config', default='config/job.conf',
                       help='Configuration file path')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    # Setup logging
    Path('logs').mkdir(exist_ok=True)
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Create Spark session
        spark = create_spark_session(config)
        
        # Create and run job
        job = SizeCalculationJob(spark, config)
        job.run(
            input_path=args.input_path,
            output_path=args.output_path,
            input_format=args.input_format,
            output_format=args.output_format,
            output_mode=args.output_mode
        )
        
        logger.info("Job completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()