

# Import the required libraries
import gc
from tqdm import tqdm
import os


import pandas as pd


from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# ignore warnings
import warnings
warnings.filterwarnings('ignore')

# Suppress distributed.utils_perf - WARNING - full garbage collections took x seconds
import logging
logging.getLogger('distributed.utils_perf').setLevel(logging.ERROR)

# Set the display options
pd.set_option('display.max_colwidth', None)

# Load the dataset into a pandas dataframe
path_to_data = "/app/data/documents/dataset_subset.csv"

df = pd.read_csv(path_to_data)


# Create a credential object using the DefaultAzureCredential class
# This will automatically use the Managed Identity if your code is running on an Azure service that supports Managed Identities
credential = DefaultAzureCredential()
# Create a SecretClient object using the credential and the URL of your Key Vault
secret_client = SecretClient(vault_url="https://cybersecuritykey.vault.azure.net/", credential=credential)
# Retrieve the secret
secret = secret_client.get_secret("cybersecurityDataConnectionString")
# Now you can use the secret in your code
connection_string = secret.value
# The name of your blob container
container_name = "cybersecuritydata"
# Create a blob service client
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
# Get a reference to the blob container
blob_container_client = blob_service_client.get_container_client(container_name)
# Upload the DataFrames to the blob container
""" try:
    # Convert the DataFrames to Parquet format
    df_parquet = df.to_parquet("data.parquet",index=False)
    node_df_parquet = node_df.to_parquet("node.parquet",index=False)
    target_df_parquet = target_df.to_parquet("target.parquet",index=False)
    source_df_parquet = source_df.to_parquet("source.parquet",index=False)
    category_df_parquet = category_df.to_parquet("category.parquet",index=False)
    # Upload the Parquet files to the blob container
    blob_container_client.upload_blob("data.parquet", df_parquet, overwrite=True)
    blob_container_client.upload_blob("node.parquet", node_df_parquet, overwrite=True)
    blob_container_client.upload_blob("target.parquet", target_df_parquet, overwrite=True)
    blob_container_client.upload_blob("source.parquet", source_df_parquet, overwrite=True)
    blob_container_client.upload_blob("category.parquet", category_df_parquet, overwrite=True)
    print("Data uploaded successfully")
except Exception as e:
    print(f"Failed to upload data: {e}") """


try:
    # convert the DataFrame to csv format
    df = df.to_csv(index=False)
    # Upload the csv files ot the blob container
    blob_container_client.upload_blob("dataset_subset.csv", df, overwrite=True)
    print("Data uploaded successfully")
except Exception as e:
    print(f"Failed to upload data: {e}")

# Delete the DataFrames to free up memory
del df

gc.collect()

print("script finished.....")