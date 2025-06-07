import pandas as pd
import numpy as np
import os

fileName = r'c:\GitHub\NPIBigDataSetWrangling\NPI May 2025 Record Samples 200 Records.csv'
output_file = r'c:\GitHub\NPIBigDataSetWrangling\NPI_May_2025_10GB.csv'

# Load the original data
df = pd.read_csv(fileName)

# Estimate how many times to repeat the data
sample_size = os.path.getsize(fileName)
repeat_factor = int((10 * 1024**3) // sample_size) + 1  # 10GB target

# Create a generator to write in chunks to avoid memory issues
chunk_size = 100000  # Adjust based on your RAM
total_rows = len(df) * repeat_factor

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    # Write header
    df.head(0).to_csv(f, index=False)
    for i in range(repeat_factor):
        chunk = df.copy()
        # Optionally, modify a column to ensure uniqueness (e.g., NPI)
        if 'NPI' in chunk.columns:
            chunk['NPI'] = chunk['NPI'].astype(str) + f"{i}"
        chunk.to_csv(f, index=False, header=False)
        if (i+1) * len(df) >= total_rows:
            break

print(f"Finished writing {output_file}")