# Step 1: Loaded variable 'df' from URI: c:\GitHub\NPIBigDataSetWrangling\NPI May 2025 Record Samples 200 Records.csv
##########################################################################################
import pandas as pd
import numpy as np
from datetime import datetime
#import dask.dataframe as ddf

fileName = r'c:\GitHub\NPIBigDataSetWrangling\NPI_May_2025_10GB.csv'
# fileName = r'c:\GitHub\NPIBigDataSetWrangling\NPI May 2025 Record Samples 200 Records.csv'
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
## Load only the fields you are going to work with in the scripts
changeDTypes = {'NPI': 'float64','Entity Type Code': 'float64','Provider Organization Name (Legal Business Name)': 'string[pyarrow]','Provider Last Name (Legal Name)': 'string[pyarrow]',
'Provider First Name': 'string[pyarrow]','Provider Credential Text': 'string[pyarrow]','Provider First Line Business Mailing Address': 'string[pyarrow]',
'Provider Second Line Business Mailing Address': 'string[pyarrow]','Provider Business Mailing Address City Name': 'string[pyarrow]','Provider Business Mailing Address State Name': 'string[pyarrow]',
'Provider Business Mailing Address Postal Code': 'float64','Provider Business Mailing Address Country Code (If outside U.S.)': 'string[pyarrow]',
'Provider Business Mailing Address Telephone Number': 'float64','Provider Business Mailing Address Fax Number': 'float64','Provider First Line Business Practice Location Address': 'string[pyarrow]',
'Provider Second Line Business Practice Location Address': 'string[pyarrow]','Provider Business Practice Location Address City Name': 'string[pyarrow]',
'Provider Business Practice Location Address State Name': 'string[pyarrow]','Provider Business Practice Location Address Postal Code': 'float64',
'Provider Business Practice Location Address Country Code (If outside U.S.)': 'string[pyarrow]','Provider Business Practice Location Address Telephone Number': 'float64',
'Provider Enumeration Date': 'string[pyarrow]','Provider Sex Code': 'string[pyarrow]','Authorized Official Last Name': 'string[pyarrow]','Authorized Official First Name': 'string[pyarrow]',
'Authorized Official Title or Position': 'string[pyarrow]','Authorized Official Telephone Number': 'float64','Healthcare Provider Taxonomy Code_1': 'string[pyarrow]','Provider License Number_1': 'string[pyarrow]','Provider License Number State Code_1': 'string[pyarrow]','Healthcare Provider Taxonomy Code_2': 'string[pyarrow]','Healthcare Provider Taxonomy Code_3': 'string[pyarrow]',
'Healthcare Provider Taxonomy Code_4': 'string[pyarrow]','Healthcare Provider Taxonomy Code_5': 'string[pyarrow]','Healthcare Provider Taxonomy Code_6': 'string[pyarrow]',
'Healthcare Provider Taxonomy Code_7': 'string[pyarrow]','Healthcare Provider Taxonomy Code_8': 'string[pyarrow]','Healthcare Provider Taxonomy Code_9': 'string[pyarrow]',
'Healthcare Provider Taxonomy Code_10': 'string[pyarrow]','Healthcare Provider Taxonomy Code_11': 'string[pyarrow]','Healthcare Provider Taxonomy Code_12': 'string[pyarrow]',
'Healthcare Provider Taxonomy Code_13': 'string[pyarrow]','Healthcare Provider Taxonomy Code_14': 'string[pyarrow]','Healthcare Provider Taxonomy Code_15': 'string[pyarrow]',
'Is Sole Proprietor': 'string[pyarrow]','Authorized Official Credential Text': 'string[pyarrow]'
                }
## load those fields with specific data types to minimise memory usage for instance replacing in most cases "object" with "category"
rowsToProcess=20000000
print(f"Starting {rowsToProcess} rows at {datetime.now().strftime('%H:%M:%S')}")

df = pd.read_csv(fileName, dtype_backend='pyarrow', low_memory=True, dtype=changeDTypes, usecols=theCols, nrows=rowsToProcess)

# Step 2: Load the "Columns to Keep" sheet from the Excel workbook
##########################################################################################
columns_to_keep = pd.read_excel("NPI Workbook.xlsx", sheet_name="Columns to Keep")
# Filter columns where the value contains "X"
columns_to_keep_filtered = columns_to_keep[columns_to_keep.iloc[:, 0].str.contains("X", na=False)]
# Extract column names to keep
columns_to_keep_list = columns_to_keep_filtered.iloc[:, 1].tolist()
# Keep only the specified columns in the DataFrame
df = df[columns_to_keep_list]

print(f"Step 2: Filtered DataFrame to keep only specified columns: at {datetime.now().strftime('%H:%M:%S')}")

# Step 3: Rename column "Is Sole Proprietor" to "Sole Proprietor"
###########################################################################################
if "Is Sole Proprietor" in df.columns:
    # pandas
    #df.rename(columns={"Is Sole Proprietor": "Sole Proprietor"}, inplace=True)
    # dask
    df = df.rename(columns={"Is Sole Proprietor": "Sole Proprietor"})

#df.compute()
print(f"Step 3: Renamed column 'Is Sole Proprietor' to 'Sole Proprietor' at {datetime.now().strftime('%H:%M:%S')}")	

# Step 4: Add "Address Type" column based on "Provider First Line Business Mailing Address"
###########################################################################################
df['Address Type'] = df['Provider First Line Business Mailing Address'].apply(
    lambda x: 'BM' if pd.notnull(x) and x.strip() != '' else 'UNK'
)
#df.compute()
print(f"Step 4: Added 'Address Type' column based on 'Provider First Line Business Mailing Address' at {datetime.now().strftime('%H:%M:%S')}")

# Step 5: Create new columns Address1, Address2, City, State, Zip, Country, Phone
###########################################################################################
df = df.assign(
    Address1=df["Provider First Line Business Mailing Address"],
    Address2=df["Provider Second Line Business Mailing Address"],
    City=df["Provider Business Mailing Address City Name"],
    State=df["Provider Business Mailing Address State Name"],
    Zip=df["Provider Business Mailing Address Postal Code"],
    Country=df["Provider Business Mailing Address Country Code (If outside U.S.)"],
    Phone=df["Provider Business Mailing Address Telephone Number"]
)
#df.compute()
print(f"Step 5: Created new columns Address1, Address2, City, State, Zip, Country, Phone from mailing address fields. at {datetime.now().strftime('%H:%M:%S')}")

# Step 6: Duplicate rows where mailing and practice addresses differ, set "Address Type" to "BP", and update Address1
###########################################################################################
df_diff = df[df["Provider First Line Business Mailing Address"] != df["Provider First Line Business Practice Location Address"]].copy()
df_diff.loc[:, "Address Type"] = "BP"
df_diff.loc[:, "Address1"] = df_diff["Provider First Line Business Practice Location Address"]
df_diff.loc[:, "Address2"] = df_diff["Provider Second Line Business Practice Location Address"]
df_diff.loc[:, "City"] = df_diff["Provider Business Practice Location Address City Name"]
df_diff.loc[:, "State"] = df_diff["Provider Business Practice Location Address State Name"]
df_diff.loc[:, "Zip"] = df_diff["Provider Business Practice Location Address Postal Code"]
df_diff.loc[:, "Country"] = df_diff["Provider Business Practice Location Address Country Code (If outside U.S.)"]
df_diff.loc[:, "Phone"] = df_diff["Provider Business Practice Location Address Telephone Number"]
df = pd.concat([df, df_diff], ignore_index=True)

#df.compute()
print(f"Step 6: Duplicated rows where mailing and practice addresses differ, set 'Address Type' to 'BP', and updated Address1. at {datetime.now().strftime('%H:%M:%S')}")

# Step 7: Default empty names to authorized names
###########################################################################################
df['Provider Last Name (Legal Name)'] = df['Provider Last Name (Legal Name)'].fillna(df['Authorized Official Last Name'])
df['Provider First Name'] = df['Provider First Name'].fillna(df['Authorized Official First Name'])
#df.compute()
print(f"Step 7: Defaulted empty names to authorized names for 'Provider Last Name (Legal Name)' and 'Provider First Name' at {datetime.now().strftime('%H:%M:%S')}")

# Step 8: Remove specified columns from df
###########################################################################################
columns_to_remove = [
    "Provider First Line Business Mailing Address",
    "Provider Second Line Business Mailing Address",
    "Provider Business Mailing Address City Name",
    "Provider Business Mailing Address State Name",
    "Provider Business Mailing Address Postal Code",
    "Provider Business Mailing Address Country Code (If outside U.S.)",
    "Provider Business Mailing Address Telephone Number",
    "Provider First Line Business Practice Location Address",
    "Provider Second Line Business Practice Location Address",
    "Provider Business Practice Location Address City Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Provider Business Practice Location Address Country Code (If outside U.S.)",
    "Provider Business Practice Location Address Telephone Number",
    "Authorized Official Last Name",
    "Authorized Official First Name",
    "Authorized Official Telephone Number"
]
df = df.drop(columns=columns_to_remove)
#df.compute()
print(f"Step 8: Removed specified columns from DataFrame: at {datetime.now().strftime('%H:%M:%S')}")	
	
# Step 9: Load the mapping from the Excel worksheet
###########################################################################################
mapping = pd.read_excel("NPI Workbook.xlsx", sheet_name="New Column Names")
# Create a dictionary for renaming columns
rename_dict = dict(zip(mapping.iloc[:, 0], mapping.iloc[:, 1]))
#print(f"Mapping for renaming columns: {rename_dict}")
# Rename columns in the dataframe
df = df.rename(columns=rename_dict)
#df.compute()
print(f"Step 9: Renamed columns based on mapping from 'New Column Names' sheet. at {datetime.now().strftime('%H:%M:%S')}")

# Step 10: Python code to shift non-empty values in "Taxonomy Code 1" to "Taxonomy Code 15" columns upward.
###########################################################################################
# taxonomy_columns = [f"Taxonomy Code {i}" for i in range(1, 16)]
# df[taxonomy_columns] = df[taxonomy_columns].apply(lambda row: pd.Series([val for val in row if pd.notna(val)] + [None] * (15 - len([val for val in row if pd.notna(val)]))), axis=1)
# print(f"Step 10: Shifted non-empty values in 'Taxonomy Code 1' to 'Taxonomy Code 15' columns upward")

taxonomy_columns = [f"Taxonomy Code {i}" for i in range(1, 16)]
arr = df[taxonomy_columns].to_numpy()

# Mask for non-null values
mask = pd.notna(arr)

# Get the non-null values per row, pad with None to length 15
def compress_row(row, mask_row):
    vals = row[mask_row]
    return np.concatenate([vals, np.full(15 - len(vals), None)])

compressed = np.array([compress_row(row, mask_row) for row, mask_row in zip(arr, mask)], dtype=object)

df[taxonomy_columns] = compressed
print(f"Step 10: Shifted non-empty values in 'Taxonomy Code 1' to 'Taxonomy Code 15' columns upward. at {datetime.now().strftime('%H:%M:%S')}")

# Step 11: Load the column order from the Excel file
###########################################################################################
column_order = pd.read_excel("NPI Workbook.xlsx", sheet_name="Column Order").iloc[:, 1].tolist()
# Reorder the columns in the dataframe

df = df[column_order]

#df.compute()
print(f"Step 11: Reordered DataFrame columns based on 'Column Order' sheet. at {datetime.now().strftime('%H:%M:%S')}")

# Step 12:Order rows by "NPI" column
###########################################################################################
df = df.sort_values(by="NPI")
#df.compute()
print(f"Step 12: Ordered DataFrame rows by 'NPI' column. at {datetime.now().strftime('%H:%M:%S')}")

# Step 13: Write DataFrame to CSV file
###########################################################################################
#df.compute()  # Ensure all computations are done before writing to CSV
df.to_csv("NPI Test Output.csv", index=False)
print(f"Step 13: Wrote DataFrame to 'NPI Test Output.csv' at {datetime.now().strftime('%H:%M:%S')}")


# 1588305650, 1225101249, 1518689132, 1508380130, 1295566073, 1114729944