import pandas as pd
import json

# Define the path to the IDEA dataset
dataset_path = 'dataset.idea'

# Define the subset size for manual annotation
subset_size = 10000  # Adjust as needed

# Define columns for the tabular format
columns = ['DetectTime', 'Node_Type', 'Node_Name', 'EventTime', 'Target_IP4', 'Target_Proto', 'ConnCount', 'Format', 'Category', 'CreateTime', 'Source_Hostname', 'Source_IP4', 'Source_Proto', 'ID']

# Create an empty DataFrame to store the extracted data
extracted_data = pd.DataFrame(columns=columns)

# Open the dataset file and process it in chunks
with open(dataset_path, 'r') as file:
    chunk_size = 1000  # Adjust chunk size based on system memory
    for chunk in pd.read_json(file, lines=True, chunksize=chunk_size):
        for index, row in chunk.iterrows():
            # Extract relevant information from the JSON structure
            detect_time = row['DetectTime']
            node_type = row['Node'][0]['Type'][0] if row['Node'] else None
            node_name = row['Node'][0]['Name'] if row['Node'] else None
            event_time = row['EventTime']
            target_ip4 = row['Target'][0]['IP4'] if row['Target'] else None
            target_proto = row['Target'][0]['Proto'][0] if row['Target'] else None
            conn_count = row['ConnCount']
            data_format = row['Format']
            category = row['Category'][0] if row['Category'] else None
            create_time = row['CreateTime']
            source_hostname = row['Source'][0]['Hostname'][0] if row['Source'] else None
            source_ip4 = row['Source'][0]['IP4'][0] if row['Source'] else None
            source_proto = row['Source'][0]['Proto'][0] if row['Source'] else None
            alert_id = row['ID']

            # Append the extracted data to the DataFrame
            extracted_data = extracted_data.append({
                'DetectTime': detect_time,
                'Node_Type': node_type,
                'Node_Name': node_name,
                'EventTime': event_time,
                'Target_IP4': target_ip4,
                'Target_Proto': target_proto,
                'ConnCount': conn_count,
                'Format': data_format,
                'Category': category,
                'CreateTime': create_time,
                'Source_Hostname': source_hostname,
                'Source_IP4': source_ip4,
                'Source_Proto': source_proto,
                'ID': alert_id
            }, ignore_index=True)

            # Check if the subset size for manual annotation is reached
            if len(extracted_data) >= subset_size:
                break

        # Check if the subset size for manual annotation is reached
        if len(extracted_data) >= subset_size:
            break

# Save the extracted data to a CSV file for manual annotation
extracted_data.to_csv('/app/data/extracted_data_subset.csv', index=False)