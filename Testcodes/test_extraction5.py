import json
import pandas as pd
from tqdm import tqdm
import random

file_path = '/app/data/dataset.idea'
max_alerts = 1000  # set to -1 to process all alerts

alert_data = []

with open(file_path, 'r') as f:
    for i, line in enumerate(tqdm(f, desc='Reading JSON data')):
        if max_alerts > 0 and i >= max_alerts:
            break

        try:
            alert = json.loads(line)
            detect_time = alert.get('DetectTime')
            event_time = alert.get('EventTime')
            category = alert.get('Category')
            source_ip = alert.get('SourceIP')
            target_ip = alert.get('TargetIP')
            conn_count = alert.get('ConnCount')
            node_name = alert.get('NodeName')
            source_port = alert.get('SourcePort')
            target_port = alert.get('TargetPort')

            alert_data.append({
                'DetectTime': detect_time,
                'EventTime': event_time,
                'Category': category,
                'SourceIP': source_ip,
                'TargetIP': target_ip,
                'ConnCount': conn_count,
                'NodeName': node_name,
                'SourcePort': source_port,
                'TargetPort': target_port
            })

        except json.JSONDecodeError:
            print(f"Failed to parse JSON for line: {i}")

# Convert the list of alert data to a DataFrame
df_alerts = pd.DataFrame(alert_data)

# Split the comma-separated values and expand the rows
for column in ['Category', 'SourceIP', 'SourcePort', 'TargetPort']:
    df_alerts = df_alerts.assign(**{column: df_alerts[column].str.split(',')}).explode(column)

# Randomly select the desired number of alerts
#random.seed(123)  # for reproducibility
#randomalerts = 1000
#df_alerts = df_alerts.sample(n=randomalerts)

# Display the first few rows to verify the DataFrame
print(df_alerts.head())

# Save the DataFrame to a CSV file for manual annotation
csv_file_path = '/app/data/dataset_subset.csv'
df_alerts.to_csv(csv_file_path, index=False)

print(f"Saved a subset of {len(df_alerts)} alerts for annotation to '{csv_file_path}'")