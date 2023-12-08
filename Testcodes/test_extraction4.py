import dask.dataframe as dd
from dask.distributed import Client
import json
import pandas as pd


def main():
    # Initialize a Dask client
    client = Client()

    # Define the path to your IDEA dataset file
    file_path = '/app/data/dataset.idea'

    # Define the number of alerts you want to randomly sample
    num_samples = 10000
    estimated_total_alerts = 12000000  # You've given this estimate
    sample_fraction = num_samples / estimated_total_alerts

    # Read the dataset using Dask
    # Since the file is a JSON lines file, we use read_text
    dd_lines = dd.read_text(file_path, blocksize="100MB")

    # Define a function to parse JSON and extract features
    def parse_and_extract_features(line):
        try:
            # Convert the string line to a dictionary
            json_obj = json.loads(line)
            
            # Extract the desired features
            detect_time = json_obj.get('DetectTime')
            event_time = json_obj.get('EventTime')
            category = json_obj.get('Category', [None])[0]
            source_ip = json_obj.get('Source', [{}])[0].get('IP4', [None])[0]
            target_ip = json_obj.get('Target', [{}])[0].get('IP4', [None])[0]
            conn_count = json_obj.get('ConnCount')
            flow_count = json_obj.get('FlowCount')
            node_name = json_obj.get('Node', [{}])[0].get('Name')

            return pd.Series([detect_time, event_time, category, source_ip, target_ip, conn_count, flow_count, node_name])
        except json.JSONDecodeError:
            # Return None or a series of Nones if the line couldn't be parsed
            return pd.Series([None]*8)

    # Apply the parsing and feature extraction function to each line
    feature_columns = ['DetectTime', 'EventTime', 'Category', 'SourceIP', 'TargetIP', 'ConnCount', 'FlowCount', 'NodeName']
    dd_extracted_features = dd_lines.map(parse_and_extract_features, meta=pd.Series(feature_columns))

    # Randomly sample the alerts
    dd_sampled = dd_extracted_features.sample(frac=sample_fraction)

    # Compute the sampled Dask DataFrame to a pandas DataFrame
    # Note: Depending on the size of the sample and your system's memory, this may still be a large operation.
    # If the sample is too large to fit into memory, consider alternatives such as saving directly from Dask to CSV in partitions.
    df_sampled = dd_sampled.compute()

    # Now that we have a pandas DataFrame, we can save it to a CSV file
    csv_file_path = '/app/data/dataset_subset1.csv'
    df_sampled.to_csv(csv_file_path, index=False)

    print(f"Saved a random sample of {len(df_sampled)} alerts for analysis to '{csv_file_path}'")

    # Don't forget to close the Dask client when you're done
    client.close()

if __name__ == '__main__':
    main()