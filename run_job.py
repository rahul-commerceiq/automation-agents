#!/usr/bin/env python3
"""
Simple runner script for the Spark Size Calculation Job
"""

import sys
import subprocess
from pathlib import Path


def run_size_calculation_job(input_path: str = "sample_data/employees.parquet",
                           output_path: str = "output",
                           input_format: str = "parquet",
                           output_format: str = "parquet"):
    """
    Run the size calculation job with specified parameters
    """
    
    # Ensure output directory exists
    Path(output_path).mkdir(exist_ok=True)
    
    cmd = [
        sys.executable, "spark_jobs/size_calculation_job.py",
        "--input-path", input_path,
        "--output-path", output_path,
        "--input-format", input_format,
        "--output-format", output_format,
        "--log-level", "INFO"
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Job completed successfully!")
        print("STDOUT:", result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Job failed with exit code {e.returncode}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return False


def main():
    """Main entry point"""
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        input_path = "sample_data/employees.parquet"
    
    if len(sys.argv) > 2:
        output_path = sys.argv[2]
    else:
        output_path = "output"
    
    success = run_size_calculation_job(input_path, output_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()