fileName = r'c:\GitHub\NPIBigDataSetWrangling\NPI_May_2025_10GB.csv'

with open(fileName, 'r', encoding='utf-8') as f:
    row_count = sum(1 for _ in f) - 1  # Subtract 1 if there is a header row

print(f"Total records in file: {row_count}")