

# %% Defining the recursive parser
def recursive_parser(entry, data_dict, col_name=""):
    if isinstance(entry, dict):
        for key, value in entry.items():
            extended_col_name = f"{col_name}_{key}" if col_name else key
            recursive_parser(value, data_dict, extended_col_name)
    elif isinstance(entry, list) or isinstance(entry, tuple):
        for i, value in enumerate(entry):
            extended_col_name = f"{col_name}_{i}" if col_name else str(i)
            recursive_parser(value, data_dict, extended_col_name)
    else:
        data_dict[col_name].append(entry)

# %% Importing the libraries    
import pandas as pd
import json
from collections import defaultdict

# %% Read the JSON file and parse it into a list of dictionaries
data = []
with open('/app/data/dataset.idea') as f:
    for i, line in enumerate(f):
        if i >= 1000:
            break
        extracted_data = json.loads(line)
        data.append(extracted_data)



# %% Parse each dictionary using the recursive_parser function
parsed_data = defaultdict(list)
for entry in data:
    recursive_parser(entry, parsed_data, "")

# Modify the parsed_data dictionary to ensure all arrays are of the same length
max_len = max([len(v) for v in parsed_data.values()])
for k, v in parsed_data.items():
    parsed_data[k] = v + [''] * (max_len - len(v))

# %% Print the first 5 elements of the parsed data  
print({k: v[:5] for k, v in parsed_data.items()})

# %% Convert the parsed data into a pandas DataFrame
df = pd.DataFrame.from_dict(parsed_data)

# %% Print the head of the DataFrame
print(df.head())

# %%

print("Script finished running successfully!")
