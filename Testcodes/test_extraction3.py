import json
import pandas as pd
import dask.bag as db
import random
import numpy as np
import dask

# Define the path to your IDEA dataset file
file_path = '/app/data/dataset.idea'

# Define the maximum number of alerts you want to process for annotation
max_alerts = 10000  # This can be adjusted based on how many you want to manually annotate

def main():
    # Create a Dask bag from the file
    lines = db.read_text(file_path)

    # Convert the Dask Bag to a list
    lines_list = lines.compute()

    # Randomly sample the desired number of alerts
    random.seed(0)  # for reproducibility
    sampled_lines = np.random.choice(lines_list, size=max_alerts)

    # Convert the sampled lines back to a Dask Bag
    sampled_lines = db.from_sequence(sampled_lines)

    # Define a function to parse and extract features from an alert
    def process_alert(line):
        alert = json.loads(line)
        detect_time = alert.get('DetectTime')
        event_time = alert.get('EventTime')
        category = alert.get('Category', [None])[0]  # Taking the first category, if multiple
        source_ip = alert.get('Source', [{}])[0].get('IP4', [None])[0]  # Taking the first IP, if multiple
        target_ip = alert.get('Target', [{}])[0].get('IP4', [None])[0]  # Taking the first IP, if multiple
        conn_count = alert.get('ConnCount')
        node_name = alert.get('Node', [{}])[0].get('Name')
        source_port = alert.get('Source', [{}])[0].get('Port', [None])[0]  # Taking the first port, if multiple
        target_port = alert.get('Target', [{}])[0].get('Port', [None])[0]  # Taking the first port, if multiple
        protocol = alert.get('Target', [{}])[0].get('Proto', [None])[0]  # Taking the first protocol, if multiple
        return {
            'DetectTime': detect_time,
            'EventTime': event_time,
            'Category': category,
            'SourceIP': source_ip,
            'TargetIP': target_ip,
            'ConnCount': conn_count,
            'NodeName': node_name,
            'SourcePort': source_port,
            'TargetPort': target_port,
            'Protocol': protocol
        }


    # Map the function to the sampled alerts
    processed_alerts = sampled_lines.map(process_alert)

    # Compute the result in parallel using Dask
    with dask.config.set(scheduler='threads', num_workers=8):  # use 8 threads
        result = processed_alerts.compute()

    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(result)

    # Display the first few rows to verify the DataFrame
    print(df.head())

if __name__ == '__main__':
    main()