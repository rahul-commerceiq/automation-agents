#!/usr/bin/env python3
"""
Script to create sample data for testing the Spark size calculation job
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType


def create_sample_data():
    """Create sample data for testing"""
    
    spark = SparkSession.builder \
        .appName("CreateSampleData") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set random seed for reproducibility
    random.seed(42)
    
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("join_date", StringType(), True)
    ])
    
    # Sample data arrays
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"]
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", 
                   "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Paul"]
    last_names = ["Smith", "Johnson", "Brown", "Davis", "Miller", "Wilson", "Moore", 
                  "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris"]
    
    # Generate sample data
    sample_data = []
    for i in range(1, 10001):  # 10,000 records
        # Introduce some nulls for testing
        name = None if i % 100 == 0 else f"{random.choice(first_names)} {random.choice(last_names)}"
        email = None if name is None else f"{name.lower().replace(' ', '.')}@company.com"
        age = None if i % 50 == 0 else random.randint(22, 65)
        salary = None if i % 75 == 0 else round(random.uniform(40000, 150000), 2)
        department = random.choice(departments)
        is_active = random.choice([True, False])
        
        # Random join date in the last 5 years
        base_date = datetime(2019, 1, 1)
        random_days = random.randint(0, 5*365)
        join_date = (base_date + timedelta(days=random_days)).strftime("%Y-%m-%d")
        
        sample_data.append((i, name, email, age, salary, department, is_active, join_date))
    
    # Create DataFrame
    df = spark.createDataFrame(sample_data, schema)
    
    # Create output directories
    Path("sample_data").mkdir(exist_ok=True)
    
    # Save in different formats
    print("Saving sample data...")
    df.write.mode("overwrite").parquet("sample_data/employees.parquet")
    df.write.mode("overwrite").json("sample_data/employees.json")
    df.write.mode("overwrite").option("header", "true").csv("sample_data/employees.csv")
    
    print(f"Created sample data with {df.count()} records")
    print("Sample data saved to:")
    print("  - sample_data/employees.parquet")
    print("  - sample_data/employees.json") 
    print("  - sample_data/employees.csv")
    
    # Show sample of the data
    print("\nSample of created data:")
    df.show(10)
    
    # Show data summary
    print("\nData summary:")
    df.describe().show()
    
    spark.stop()


if __name__ == "__main__":
    create_sample_data()