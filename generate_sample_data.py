"""
Generate sample data for testing the Spark size calculation job.
"""

import random
import pandas as pd
from datetime import datetime, timedelta


def generate_sample_data(num_records: int = 10000, output_path: str = "sample_data.parquet"):
    """
    Generate sample data for testing the size calculation job.
    
    Args:
        num_records: Number of records to generate
        output_path: Path to save the sample data
    """
    
    # Define categories and departments for grouping
    categories = ['Documents', 'Images', 'Videos', 'Audio', 'Archives', 'Code']
    departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance']
    
    # Generate random data
    data = []
    base_date = datetime.now() - timedelta(days=365)
    
    for i in range(num_records):
        # Generate realistic file sizes (in bytes)
        category = random.choice(categories)
        
        # Different size distributions for different categories
        if category == 'Documents':
            size = random.lognormvariate(10, 1.5)  # Smaller files
        elif category == 'Images':
            size = random.lognormvariate(15, 1.2)  # Medium files
        elif category == 'Videos':
            size = random.lognormvariate(20, 1.8)  # Large files
        elif category == 'Audio':
            size = random.lognormvariate(17, 1.0)  # Medium-large files
        elif category == 'Archives':
            size = random.lognormvariate(18, 2.0)  # Variable sizes
        else:  # Code
            size = random.lognormvariate(8, 1.0)   # Small files
        
        # Ensure minimum size and convert to integer
        size = max(1, int(size))
        
        # Generate other fields
        department = random.choice(departments)
        timestamp = base_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Occasionally add some edge cases
        if random.random() < 0.01:  # 1% chance
            size = 0  # Zero size files
        elif random.random() < 0.005:  # 0.5% chance
            size = None  # Missing size data
        
        data.append({
            'id': f'file_{i+1:06d}',
            'size': size,
            'category': category,
            'department': department,
            'filename': f'{category.lower()}_file_{i+1}.{category.lower()[:3]}',
            'created_date': timestamp.strftime('%Y-%m-%d'),
            'created_timestamp': timestamp
        })
    
    # Create DataFrame and save
    df = pd.DataFrame(data)
    
    print(f"Generated {len(df)} records")
    print(f"Categories: {df['category'].value_counts().to_dict()}")
    print(f"Departments: {df['department'].value_counts().to_dict()}")
    print(f"Size statistics:")
    print(df['size'].describe())
    
    # Save as Parquet
    df.to_parquet(output_path, index=False)
    print(f"Sample data saved to {output_path}")
    
    # Also save as CSV for easier inspection
    csv_path = output_path.replace('.parquet', '.csv')
    df.to_csv(csv_path, index=False)
    print(f"Sample data also saved to {csv_path}")
    
    return df


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate sample data for size calculation job')
    parser.add_argument('--records', type=int, default=10000, help='Number of records to generate')
    parser.add_argument('--output', type=str, default='sample_data.parquet', help='Output file path')
    
    args = parser.parse_args()
    
    generate_sample_data(args.records, args.output)