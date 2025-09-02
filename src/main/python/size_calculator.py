"""
Main Spark job for calculating size metrics.
This job processes input data to compute various size-related statistics.
"""

import sys
import argparse
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, min as spark_min, max as spark_max, 
    count, stddev, variance, expr, when, isnan, isnull
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from config import SparkJobConfig
from utils import (
    setup_logging, validate_dataframe_schema, clean_numeric_data,
    validate_size_data, create_summary_report
)


class SizeCalculationJob:
    """Main class for Spark size calculation job."""
    
    def __init__(self, config: SparkJobConfig):
        self.config = config
        self.logger = setup_logging()
        self.spark = None
        
    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        spark_config = self.config.get_spark_config()
        
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)
            
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.logger.info(f"Created Spark session: {spark_config['spark.app.name']}")
        return self.spark
    
    def read_input_data(self) -> DataFrame:
        """Read input data based on configuration."""
        data_config = self.config.get_data_config()
        input_path = data_config['input_path']
        input_format = data_config['input_format'].lower()
        
        self.logger.info(f"Reading {input_format} data from {input_path}")
        
        try:
            if input_format == 'parquet':
                df = self.spark.read.parquet(input_path)
            elif input_format == 'csv':
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
            elif input_format == 'json':
                df = self.spark.read.json(input_path)
            else:
                raise ValueError(f"Unsupported input format: {input_format}")
                
            self.logger.info(f"Successfully read {df.count()} rows with {len(df.columns)} columns")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read input data: {str(e)}")
            raise
    
    def calculate_basic_metrics(self, df: DataFrame, size_column: str) -> Dict[str, float]:
        """Calculate basic size metrics."""
        self.logger.info("Calculating basic size metrics")
        
        # Calculate basic statistics
        stats = df.select(
            spark_sum(size_column).alias('total_size'),
            avg(size_column).alias('average_size'),
            spark_min(size_column).alias('min_size'),
            spark_max(size_column).alias('max_size'),
            count(size_column).alias('count'),
            stddev(size_column).alias('stddev_size'),
            variance(size_column).alias('variance_size')
        ).collect()[0]
        
        return {
            'total_size': float(stats['total_size'] or 0),
            'average_size': float(stats['average_size'] or 0),
            'min_size': float(stats['min_size'] or 0),
            'max_size': float(stats['max_size'] or 0),
            'total_records': int(stats['count'] or 0),
            'stddev_size': float(stats['stddev_size'] or 0),
            'variance_size': float(stats['variance_size'] or 0)
        }
    
    def calculate_percentiles(self, df: DataFrame, size_column: str, percentiles: List[float]) -> Dict[str, float]:
        """Calculate percentile statistics."""
        self.logger.info(f"Calculating percentiles: {[p*100 for p in percentiles]}")
        
        percentile_results = {}
        
        for p in percentiles:
            percentile_value = df.select(
                expr(f"percentile_approx({size_column}, {p})").alias(f'p{int(p*100)}')
            ).collect()[0][0]
            
            percentile_results[f'{int(p*100)}'] = float(percentile_value or 0)
        
        return percentile_results
    
    def calculate_group_metrics(self, df: DataFrame, size_column: str, group_columns: List[str]) -> Dict[str, Dict[str, float]]:
        """Calculate size metrics grouped by specified columns."""
        self.logger.info(f"Calculating group metrics by: {group_columns}")
        
        # Filter out empty group columns
        valid_group_columns = [col for col in group_columns if col.strip() and col in df.columns]
        
        if not valid_group_columns:
            self.logger.warning("No valid group columns found, skipping group analysis")
            return {}
        
        # Group by specified columns and calculate metrics
        grouped_stats = df.groupBy(*valid_group_columns).agg(
            spark_sum(size_column).alias('total_size'),
            avg(size_column).alias('average_size'),
            spark_min(size_column).alias('min_size'),
            spark_max(size_column).alias('max_size'),
            count(size_column).alias('count')
        ).collect()
        
        group_results = {}
        for row in grouped_stats:
            # Create group key from group column values
            group_key = "_".join([str(row[col]) for col in valid_group_columns])
            
            group_results[group_key] = {
                'total': float(row['total_size'] or 0),
                'average': float(row['average_size'] or 0),
                'min': float(row['min_size'] or 0),
                'max': float(row['max_size'] or 0),
                'count': int(row['count'] or 0)
            }
        
        return group_results
    
    def create_results_dataframe(self, results: Dict[str, Any]) -> DataFrame:
        """Create a DataFrame with calculation results for output."""
        
        # Create summary statistics DataFrame
        summary_schema = StructType([
            StructField("metric", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("formatted_value", StringType(), True)
        ])
        
        summary_data = [
            ("total_size", results['basic_metrics']['total_size'], f"{results['basic_metrics']['total_size']:.2f}"),
            ("average_size", results['basic_metrics']['average_size'], f"{results['basic_metrics']['average_size']:.2f}"),
            ("min_size", results['basic_metrics']['min_size'], f"{results['basic_metrics']['min_size']:.2f}"),
            ("max_size", results['basic_metrics']['max_size'], f"{results['basic_metrics']['max_size']:.2f}"),
            ("total_records", results['basic_metrics']['total_records'], f"{results['basic_metrics']['total_records']}"),
            ("stddev_size", results['basic_metrics']['stddev_size'], f"{results['basic_metrics']['stddev_size']:.2f}"),
        ]
        
        # Add percentiles
        for percentile, value in results.get('percentiles', {}).items():
            summary_data.append((f"p{percentile}", value, f"{value:.2f}"))
        
        return self.spark.createDataFrame(summary_data, summary_schema)
    
    def write_results(self, results_df: DataFrame, summary_report: str):
        """Write calculation results to output destination."""
        data_config = self.config.get_data_config()
        output_path = data_config['output_path']
        output_format = data_config['output_format'].lower()
        
        self.logger.info(f"Writing results to {output_path} in {output_format} format")
        
        try:
            # Write structured results
            if output_format == 'parquet':
                results_df.coalesce(1).write.mode('overwrite').parquet(f"{output_path}/metrics")
            elif output_format == 'csv':
                results_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(f"{output_path}/metrics")
            elif output_format == 'json':
                results_df.coalesce(1).write.mode('overwrite').json(f"{output_path}/metrics")
            
            # Write summary report
            summary_df = self.spark.createDataFrame([(summary_report,)], ["summary_report"])
            summary_df.coalesce(1).write.mode('overwrite').text(f"{output_path}/summary")
            
            self.logger.info("Successfully wrote results")
            
        except Exception as e:
            self.logger.error(f"Failed to write results: {str(e)}")
            raise
    
    def run(self) -> Dict[str, Any]:
        """Main execution method for the size calculation job."""
        self.logger.info("Starting size calculation job")
        
        try:
            # Create Spark session
            self.create_spark_session()
            
            # Get configuration
            calc_config = self.config.get_calculation_config()
            size_column = calc_config['size_column']
            group_columns = calc_config['group_by_columns']
            
            # Read and validate input data
            df = self.read_input_data()
            validate_dataframe_schema(df, [size_column])
            
            # Clean the data
            df_clean = clean_numeric_data(df, [size_column])
            
            # Validate size data
            validation_results = validate_size_data(df_clean, size_column)
            
            # Calculate basic metrics
            basic_metrics = self.calculate_basic_metrics(df_clean, size_column)
            
            # Calculate percentiles if enabled
            percentiles = {}
            if calc_config['calculate_percentiles']:
                percentiles = self.calculate_percentiles(df_clean, size_column, calc_config['percentiles'])
            
            # Calculate group metrics
            group_metrics = self.calculate_group_metrics(df_clean, size_column, group_columns)
            
            # Compile results
            results = {
                'basic_metrics': basic_metrics,
                'percentiles': percentiles,
                'group_analysis': group_metrics,
                'validation': validation_results
            }
            
            # Create summary report
            summary_report = create_summary_report(results)
            self.logger.info(f"Job completed successfully:\n{summary_report}")
            
            # Create results DataFrame and write output
            results_df = self.create_results_dataframe(results)
            self.write_results(results_df, summary_report)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Job failed with error: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main():
    """Main entry point for the Spark job."""
    parser = argparse.ArgumentParser(description='Spark Size Calculation Job')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--input-path', type=str, help='Input data path')
    parser.add_argument('--output-path', type=str, help='Output results path')
    parser.add_argument('--size-column', type=str, default='size', help='Name of size column')
    parser.add_argument('--input-format', type=str, default='parquet', 
                       choices=['parquet', 'csv', 'json'], help='Input data format')
    parser.add_argument('--output-format', type=str, default='parquet',
                       choices=['parquet', 'csv', 'json'], help='Output data format')
    
    args = parser.parse_args()
    
    # Initialize configuration
    config = SparkJobConfig(args.config)
    
    # Override configuration with command line arguments
    if args.input_path:
        config.config.set('data', 'input_path', args.input_path)
    if args.output_path:
        config.config.set('data', 'output_path', args.output_path)
    if args.size_column:
        config.config.set('calculation', 'size_column', args.size_column)
    if args.input_format:
        config.config.set('data', 'input_format', args.input_format)
    if args.output_format:
        config.config.set('data', 'output_format', args.output_format)
    
    # Run the job
    job = SizeCalculationJob(config)
    results = job.run()
    
    print("Size calculation job completed successfully!")
    return results


if __name__ == "__main__":
    main()