import json
import pandas as pd
from tqdm import tqdm
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine
import math

def extract_alert_data(file_path, max_alerts):
    # List to hold extracted alert data
    alert_data = []

    # Open the dataset file and process line by line
    with open(file_path, 'r') as file:
        for i, line in tqdm(enumerate(file), desc='Reading JSON file', unit=' lines'):
            # Stop processing if we've reached the desired number of alerts
            if i >= max_alerts:
                break
            try:
                # Parse the JSON data for the current alert
                alert = json.loads(line)

                # Extract relevant features from the alert here
                # For example:
                detect_time = alert.get('DetectTime')
                event_time = alert.get('EventTime')
                win_start_time = alert.get('WinStartTime')
                win_end_time = alert.get('WinEndTime')
                category = alert.get('Category', [None])[0]  # Taking the first category, if multiple
                source_ip = alert.get('Source', [{}])[0].get('IP4', [None])  # Taking the first IP, if multiple
                target_ip = alert.get('Target', [{}])[0].get('IP4', [None])  # Taking the first IP, if multiple
                conn_count = alert.get('ConnCount', 0)
                flow_count = alert.get('FlowCount', 0)
                node_name = alert.get('Node', [{}])[0].get('Name')
                node_type = alert.get('Node', [{}])[0].get('Type')
                source_port = alert.get('Source', [{}])[0].get('Port', [None])  # Taking the first port, if multiple
                target_port = alert.get('Target', [{}])[0].get('Port', [None])  # Taking the first port, if multiple
                source_protocol = alert.get('Source', [{}])[0].get('Proto', [None])  # Taking the first protocol, if multiple
                target_protocol = alert.get('Target', [{}])[0].get('Proto', [None])  # Taking the first protocol, if multiple

                # Add the extracted data to our list
                alert_data.append({
                    'DetectTime': detect_time,
                    'EventTime': event_time,
                    'WinStartTime': alert.get('WinStartTime'),
                    'WinEndTime': alert.get('WinEndTime'),
                    'Category': category,
                    'SourceIP': source_ip,
                    'TargetIP': target_ip,
                    'ConnCount': conn_count,
                    'FlowCount': flow_count,
                    'NodeName': node_name,
                    'NodeType': node_type,
                    'SourcePort': source_port,
                    'TargetPort': target_port,
                    'SourceProtocol': source_protocol,
                    'TargetProtocol': target_protocol
                })

            except json.JSONDecodeError:
                print(f"Failed to parse JSON for line: {i}")

    return alert_data

def save_alerts_to_csv(df_alerts, csv_file_path):
    # Save the DataFrame to a CSV file for visualization
    # You can choose a different file format if you prefer
    df_alerts.to_csv(csv_file_path, index=True)
    print(f"Saved alerts to '{csv_file_path}'")

""" def save_alerts_to_sql(df_alerts, db_file_path):
    # Connect to SQL Database
    print('Connecting to SQL Database')
    engine = create_engine(f'sqlite:///{db_file_path}', echo=False)

    # Insert Data into SQL Database
    print('Inserting Data into SQL Database')
    df_alerts.to_sql('alerts', con=engine, if_exists='replace', index=False)
    print('Data Inserted into SQL Database Successfully') """

def perform_stratified_sampling(df_alerts, test_size, random_state):
    # Perform Stratified Sampling to get a subset of alerts for annotation
    print('Performing Stratified Sampling to get a subset of alerts for annotation')
    # This ensures that the distribution of categories in the subset is similar to the distribution in the full dataset
    df_subset, _ = train_test_split(df_alerts, test_size=test_size, stratify=df_alerts['Category'], random_state=random_state)
    return df_subset

def save_alerts_to_sql(df_alerts, db_file_path, batch_size=1000):
    
    # Calculate the number of batches
    num_batches = math.ceil(len(df_alerts) / batch_size)
    # Insert Data into SQL Database in batches
    print('Inserting Data into SQL Database')
    for i in range(num_batches):
              
        # Connect to SQL Database
        print('Connecting to SQL Database')
        engine = create_engine(f'sqlite:///{db_file_path}', echo=False, pool_pre_ping=True)
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, len(df_alerts))
        df_batch = df_alerts[start_index:end_index]
        # Convert list to string
        for col in df_batch.columns:
            if df_batch[col].apply(type).eq(list).any():
                df_batch[col] = df_batch[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        
        df_batch.to_sql('alerts', con=engine, if_exists='append', index=False )
        print(f'Data Inserted into SQL Database Successfully ' + str(end_index))
    
    print('All Data Inserted into SQL Database Successfully')

def main():
    # Define the path to your IDEA dataset file
    file_path = '/app/data/rawdata/dataset.idea'

    # Define the maximum number of alerts you want to process for annotation
    max_alerts = 10000000  # Select all alerts for annotation

    # Extract alert data
    alert_data = extract_alert_data(file_path, max_alerts)

    # Convert the list of alert data to a DataFrame
    df_alerts = pd.DataFrame(alert_data)

    # Display the first few rows to verify the DataFrame
    print(df_alerts.head())

    # specify the columns for the input stratification purpose
    target_label_column = 'Category'
    # Save the DataFrame to a CSV file for manual annotation
    csv_file_path = '/app/data/documents/dataset_subset.csv'
    df_subset = perform_stratified_sampling(df_alerts, test_size=0.99, random_state=42)
    save_alerts_to_csv(df_subset, csv_file_path)

    label_count = df_subset[target_label_column].value_counts()
    print(label_count)

    # Save the DataFrame to a SQL database
    db_file_path = '/app/data/database/alerts.db'
    save_alerts_to_sql(df_alerts, db_file_path)

if __name__ == "__main__":
    main()
                    
