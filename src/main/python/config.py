"""
Configuration management for the Spark size calculation job.
"""

import configparser
import os
from typing import Dict, Any


class SparkJobConfig:
    """Configuration class for Spark size calculation job."""
    
    def __init__(self, config_file: str = None):
        self.config = configparser.ConfigParser()
        
        # Default configuration
        self.defaults = {
            'spark': {
                'app_name': 'SizeCalculationJob',
                'master': 'local[*]',
                'executor_memory': '2g',
                'driver_memory': '1g',
                'executor_cores': '2'
            },
            'data': {
                'input_format': 'parquet',
                'output_format': 'parquet',
                'input_path': '/tmp/input',
                'output_path': '/tmp/output'
            },
            'calculation': {
                'size_column': 'size',
                'group_by_columns': 'category',
                'calculate_percentiles': 'true',
                'percentiles': '25,50,75,90,95,99'
            }
        }
        
        # Load configuration from file if provided
        if config_file and os.path.exists(config_file):
            self.config.read(config_file)
        
        # Set defaults for missing sections
        for section, options in self.defaults.items():
            if not self.config.has_section(section):
                self.config.add_section(section)
            for key, value in options.items():
                if not self.config.has_option(section, key):
                    self.config.set(section, key, value)
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark session configuration."""
        return {
            'spark.app.name': self.config.get('spark', 'app_name'),
            'spark.master': self.config.get('spark', 'master'),
            'spark.executor.memory': self.config.get('spark', 'executor_memory'),
            'spark.driver.memory': self.config.get('spark', 'driver_memory'),
            'spark.executor.cores': self.config.get('spark', 'executor_cores')
        }
    
    def get_data_config(self) -> Dict[str, str]:
        """Get data input/output configuration."""
        return {
            'input_format': self.config.get('data', 'input_format'),
            'output_format': self.config.get('data', 'output_format'),
            'input_path': self.config.get('data', 'input_path'),
            'output_path': self.config.get('data', 'output_path')
        }
    
    def get_calculation_config(self) -> Dict[str, Any]:
        """Get calculation configuration."""
        percentiles_str = self.config.get('calculation', 'percentiles')
        percentiles = [float(p.strip()) / 100.0 for p in percentiles_str.split(',')]
        
        return {
            'size_column': self.config.get('calculation', 'size_column'),
            'group_by_columns': self.config.get('calculation', 'group_by_columns').split(','),
            'calculate_percentiles': self.config.getboolean('calculation', 'calculate_percentiles'),
            'percentiles': percentiles
        }