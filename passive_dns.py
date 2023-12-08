import math
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

def save_passive_dns_to_sql(df_passive_dns, db_file_path, batch_size=1000):
    """
    Save Passive DNS data to SQLite database using SQLAlchemy.

    Parameters:
    - df_passive_dns (pd.DataFrame): DataFrame containing Passive DNS data.
    - db_file_path (str): Path to the SQLite database file.
    - batch_size (int): Number of rows to insert in each batch.
    """
    # Calculate the number of batches
    num_batches = math.ceil(len(df_passive_dns) / batch_size)

    # Insert data into the SQLite database in batches
    print('Inserting Data into SQL Database....')
    print('Connecting to SQL Database....')
    for i in range(num_batches):
        # Connect to the SQLite database
        
        engine = create_engine(f'sqlite:///{db_file_path}', echo=False, pool_pre_ping=True)

        # Determine the batch start and end indices
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, len(df_passive_dns))
        
        # Get the current batch
        df_batch = df_passive_dns.iloc[start_index:end_index]

        # Convert lists to strings
        for col in df_batch.columns:
            if df_batch[col].apply(type).eq(list).any():
                df_batch[col] = df_batch[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        # Insert the current batch into the database
        df_batch.to_sql('passiveDNS_data', con=engine, if_exists='append', index=False)

        print(f'Data Inserted into SQL Database Successfully ' + str(end_index))

    print('All Data Inserted into SQL Database Successfully')




def main():
    # Define the path to your Passive DNS dataset file
    file_path = '/app/data/rawdata/Aux_2_PassiveDNS.csv'

    # Read the first line to determine the number of columns
    with open(file_path, 'r') as file:
        first_line = file.readline().strip()
        num_columns = len(first_line.split(','))

    # Define column names based on the number of columns
    column_names = [f'col{i}' for i in range(num_columns)]

    # Import Passive DNS data from the dataset file with specified column names and only the necessary number of data points
    df_passive_dns = pd.read_csv(file_path, skiprows=1, names=column_names, usecols=range(num_columns), na_values=['None'], sep=',')

    # list of column names 

    column_names = ['ip', 'numrecords', 'avgdomainsinrecords', 'stddevdomainsinrecords', 'maxdomainsinrecords',
        'mediandomainsinrecords', 'avglenrecords', 'stddevlenrecords', 'maxlenrecords', 'medianlenrecords',
        'avgsimilarityrecords', 'stddevsimilarityrecords', 'maxsimilarityrecords', 'mediansimilarityrecords',
        'avgentropyrecords', 'stddeventropyrecords', 'maxentropyrecords', 'medianentropyrecords',
        'avgmaxconsecutivecharsrecords', 'stddevmaxconsecutivecharsrecords', 'maxmaxconsecutivecharsrecords',
        'medianmaxconsecutivecharsrecords', 'avglenlowleveldomains', 'stddevlenlowleveldomains',
        'maxlenlowleveldomains', 'medianlenlowleveldomains', 'avgsimilaritylowleveldomains',
        'stddevsimilaritylowleveldomains', 'maxsimilaritylowleveldomains', 'mediansimilaritylowleveldomains',
        'avgentropylowleveldomains', 'stddeventropylowleveldomains', 'maxentropylowleveldomains',
        'medianentropylowleveldomains', 'avgmaxconsecutiecharslowleveldomains',
        'stddevmaxconsecutiecharslowleveldomains', 'maxmaxconsecutiecharslowleveldomains',
        'medianmaxconsecutiecharslowleveldomains','Unknonwn']

    # Rename the columns
    df_passive_dns.columns = column_names

    # Drop the Unknonwn column
    df_passive_dns.drop(columns=['Unknonwn'], inplace=True)


    # Display the first few rows to verify the DataFrame
    print(df_passive_dns.head())

    # Save the DataFrame to a SQLite database
    db_file_path = '/app/data/database/passive_dns.db'
    save_passive_dns_to_sql(df_passive_dns, db_file_path)






if __name__ == '__main__':
    main()

