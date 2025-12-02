import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

def generate_nyc311_sample(n_rows=25000, output_file="./data/nyc_311_2023_sample.csv"):
    """Generate realistic NYC 311 sample data matching your schema."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    random.seed(42)
    np.random.seed(42)
    
    boroughs = ["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"]
    agencies = ["NYPD", "DOT", "DEP", "DSNY", "DOHMH"]
    complaint_types = [
        "Noise - Residential", "Illegal Parking", "HEATING", "Street Condition",
        "Noise - Street/Sidewalk", "Blocked Driveway", "Request Large Assemblages"
    ]
    
    # Generate ALL data as Python lists with exactly n_rows items
    base_date = datetime(2023, 1, 1)
    
    data = {
        "unique_key": list(range(1, n_rows + 1)),
        "created_date": [
            base_date + timedelta(days=random.randint(0, 365)) for _ in range(n_rows)
        ],
        "closed_date": [
            base_date + timedelta(days=random.randint(0, 370), hours=random.randint(0, 24)) 
            for _ in range(n_rows)
        ],
        "agency": random.choices(agencies, k=n_rows),
        "complaint_type": random.choices(complaint_types, k=n_rows),
        "descriptor": ["Brief description of issue", "General complaint", "Specific problem noted"] * ((n_rows // 3) + 1),
        "borough": random.choices(boroughs, k=n_rows),
        "latitude": [40.7128 + random.uniform(-0.1, 0.1) for _ in range(n_rows)],
        "longitude": [-74.0060 + random.uniform(-0.1, 0.1) for _ in range(n_rows)],
        "incident_zip": [f"10{random.randint(0,5):02d}" for _ in range(n_rows)]
    }
    
    # Truncate descriptor list to exact length
    data["descriptor"] = data["descriptor"][:n_rows]
    
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    print(f"[✅] Generated {n_rows:,} synthetic NYC 311 rows → {output_file}")
    print(f"[📊] Shape: {df.shape}")
    print(df[['borough', 'agency', 'complaint_type']].head())
    return output_file

if __name__ == "__main__":
    generate_nyc311_sample()
