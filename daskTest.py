import pandas as pd
import numpy as np
from datetime import datetime
import dask.dataframe as ddf

fileName = r'c:\GitHub\NPIBigDataSetWrangling\NPI May 2025 Record Samples 200 Records.csv'
theCols = ['NPI','Entity Type Code','Provider Organization Name (Legal Business Name)','Provider Last Name (Legal Name)','Provider First Name',
           'Provider Credential Text','Provider First Line Business Mailing Address','Provider Second Line Business Mailing Address','Provider Business Mailing Address City Name',
           'Provider Business Mailing Address State Name','Provider Business Mailing Address Postal Code','Provider Business Mailing Address Country Code (If outside U.S.)',
           'Provider Business Mailing Address Telephone Number','Provider Business Mailing Address Fax Number','Provider First Line Business Practice Location Address',
           'Provider Second Line Business Practice Location Address','Provider Business Practice Location Address City Name','Provider Business Practice Location Address State Name',
           'Provider Business Practice Location Address Postal Code','Provider Business Practice Location Address Country Code (If outside U.S.)',
           'Provider Business Practice Location Address Telephone Number','Provider Enumeration Date','Provider Sex Code','Authorized Official Last Name',
           'Authorized Official First Name','Authorized Official Title or Position','Authorized Official Telephone Number','Healthcare Provider Taxonomy Code_1',
           'Provider License Number_1','Provider License Number State Code_1','Healthcare Provider Taxonomy Code_2','Healthcare Provider Taxonomy Code_3',
           'Healthcare Provider Taxonomy Code_4','Healthcare Provider Taxonomy Code_5','Healthcare Provider Taxonomy Code_6','Healthcare Provider Taxonomy Code_7',
           'Healthcare Provider Taxonomy Code_8','Healthcare Provider Taxonomy Code_9','Healthcare Provider Taxonomy Code_10','Healthcare Provider Taxonomy Code_11',
           'Healthcare Provider Taxonomy Code_12','Healthcare Provider Taxonomy Code_13','Healthcare Provider Taxonomy Code_14','Healthcare Provider Taxonomy Code_15',
           'Is Sole Proprietor','Authorized Official Credential Text']

df = ddf.read_csv(fileName, usecols=theCols, assume_missing=True)
# Display the first few rows of the Dask DataFrame

columns_to_keep = pd.read_excel("NPI Workbook.xlsx", sheet_name="Columns to Keep")
# Filter columns where the value contains "X"
columns_to_keep_filtered = columns_to_keep[columns_to_keep.iloc[:, 0].str.contains("X", na=False)]
# Extract column names to keep
columns_to_keep_list = columns_to_keep_filtered.iloc[:, 1].tolist()
# Keep only the specified columns in the DataFrame
df = df[columns_to_keep_list]
print(f"Step 2: Filtered DataFrame to keep only specified columns: {columns_to_keep_list}")
