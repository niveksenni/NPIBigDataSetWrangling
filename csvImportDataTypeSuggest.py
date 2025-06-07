#import pandas as pd
#import numpy as np
import dask.dataframe as ddf

# This script is used to load a large CSV file and analyze the data types of its columns.
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
changeDTypes = {'NPI': 'int32[pyarrow]', 'Entity Type Code': 'int32[pyarrow]', 
                'Provider Business Mailing Address Postal Code': 'int32[pyarrow]',
                'Provider Business Practice Location Address Postal Code': 'int32[pyarrow]'
                }
# Apply specific data types to reduce memory usage

#df = pd.read_csv(fileName, dtype_backend='pyarrow', low_memory=False, usecols=theCols)
df = ddf.read_csv(fileName, usecols=theCols, assume_missing=True)

# Display the first few rows of the DataFrame
## Load only the fields you are going to work  with in the scripts
## load those fields with specific data types to minimise memoery usage
#### 

for col in df.columns:
    col_data = df[col]
    suggestion = None
    if pd.api.types.is_integer_dtype(col_data):
        min_val, max_val = col_data.min(), col_data.max()
        for dtype in [np.int8, np.int16, np.int32, np.int64]:
            if min_val >= np.iinfo(dtype).min and max_val <= np.iinfo(dtype).max:
                if col_data.dtype != dtype:
                    suggestion = dtype.__name__
                break
    elif pd.api.types.is_float_dtype(col_data):
        if col_data.dtype != np.float32:
            suggestion = "float32"
    elif pd.api.types.is_object_dtype(col_data):
        unique_vals = col_data.nunique(dropna=False)
        if unique_vals / len(col_data) < 0.5:
            suggestion = "category"
        else:
            suggestion = None  # keep as object
    # Only print if a suggestion is made
    if suggestion:
        print(f"Column: {col}")
        print(f"  Current dtype: {col_data.dtype}")
        print(f"  Suggested dtype: {suggestion}")