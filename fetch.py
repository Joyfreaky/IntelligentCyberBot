from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import pandas as pd
import csv

# The URL to your Key Vault
key_vault_url = "https://cybersecuritykey.vault.azure.net/"

# The name of your secret
secret_name = "cybersecurityDataConnectionString"

# The name of your blob container
container_name = "cybersecuritydata"

# The name of the blob
blob_name = "cybersecurity_data.csv"

# Create a credential object using the DefaultAzureCredential class
credential = DefaultAzureCredential()

# Create a secret client using the credential
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve the secret
secret = secret_client.get_secret(secret_name)

# Create a blob service client using the secret
blob_service_client = BlobServiceClient.from_connection_string(secret.value)

# Get a reference to the blob container
blob_container_client = blob_service_client.get_container_client(container_name)

# Get a reference to the blob
blob_client = blob_container_client.get_blob_client(blob_name)

# Download the blob to a local file 
with open("cybersecurity_data.csv", "wb") as my_blob:
    download_stream = blob_client.download_blob()
    my_blob.write(download_stream.readall())

# Load the CSV file into a Pandas DataFrame
df = pd.read_csv("cybersecurity_data.csv")

# Print the DataFrame
print(df)