"""
Example runner for the Spark size calculation job.
This script generates sample data and runs the size calculation job.
"""

import os
import sys
import tempfile
import shutil
from generate_sample_data import generate_sample_data

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src', 'main', 'python'))

from size_calculator import SizeCalculationJob
from config import SparkJobConfig


def run_example():
    """Run an example of the size calculation job."""
    
    # Create temporary directories
    temp_dir = tempfile.mkdtemp()
    input_path = os.path.join(temp_dir, "input.parquet")
    output_path = os.path.join(temp_dir, "output")
    
    try:
        print("=== Spark Size Calculation Job Example ===")
        print(f"Temporary directory: {temp_dir}")
        
        # Generate sample data
        print("\n1. Generating sample data...")
        generate_sample_data(num_records=1000, output_path=input_path)
        
        # Configure the job
        print("\n2. Configuring Spark job...")
        config = SparkJobConfig()
        config.config.set('data', 'input_path', input_path)
        config.config.set('data', 'output_path', output_path)
        config.config.set('calculation', 'group_by_columns', 'category,department')
        
        # Run the job
        print("\n3. Running Spark size calculation job...")
        job = SizeCalculationJob(config)
        results = job.run()
        
        # Display results
        print("\n4. Job completed successfully!")
        print(f"Results written to: {output_path}")
        
        # Show basic metrics
        basic_metrics = results['basic_metrics']
        print(f"\nBasic Metrics:")
        print(f"  Total records: {basic_metrics['total_records']:,}")
        print(f"  Total size: {basic_metrics['total_size']:,.0f} bytes")
        print(f"  Average size: {basic_metrics['average_size']:,.2f} bytes")
        print(f"  Min size: {basic_metrics['min_size']:,.0f} bytes")
        print(f"  Max size: {basic_metrics['max_size']:,.0f} bytes")
        
        # Show percentiles
        if results['percentiles']:
            print(f"\nPercentiles:")
            for p, value in results['percentiles'].items():
                print(f"  {p}th percentile: {value:,.0f} bytes")
        
        # Show group analysis
        if results['group_analysis']:
            print(f"\nTop 5 Groups by Total Size:")
            sorted_groups = sorted(results['group_analysis'].items(), 
                                 key=lambda x: x[1]['total'], reverse=True)[:5]
            for group, stats in sorted_groups:
                print(f"  {group}: {stats['total']:,.0f} bytes ({stats['count']} files)")
        
        print(f"\nDetailed results available in: {output_path}")
        
    except Exception as e:
        print(f"Error running example: {str(e)}")
        raise
    finally:
        # Cleanup
        print(f"\nCleaning up temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    run_example()