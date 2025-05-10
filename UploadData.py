##Make sure you are connected to Azure in the terminal by using az login. You may have to install Azure tools to execute this.
#Install azure-storage-blob & azure-identity to connect to the cloud and modify files.
'''Source: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=managed-identity%2Croles-azure-portal%2Csign-in-visual-studio-code&pivots=blob-storage-quickstart-scratch'''
import os, uuid, io
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

import pandas as pd
import datetime
from sodapy import Socrata

try:
        print("Azure Blob Storage Python quickstart sample")

        # Quickstart code goes here
        account_url = "https://rhomework2025.blob.core.windows.net"
        default_credential = DefaultAzureCredential()

        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient(account_url, credential=default_credential)

        # Create a unique name for the container
        container_name = str("busincidentstorage")

        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name)

        # Create the container if it doesn't exist
        if not container_client.exists():
                container_client.create_container()

        # Create a local directory to hold blob data
        local_path = "./data"
        try:
                os.mkdir(local_path)
        except:
                print("/data already exists")

        ## Create a file in the local data directory to upload and download
        
        #Initialize CSV to be put in RAM
        csv_buffer = io.StringIO()
        
        '''Source: https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset'''

        # make sure to install these packages before running:
        # pip install pandas
        # pip install sodapy

        # Unauthenticated client only works with public data sets. Note 'None'
        # in place of application token, and no username or password:
        client = Socrata("data.cityofnewyork.us", None)

        #Set arbitrary limit to ensure we obtain all rows.
        results = client.get("ez4e-fazm", limit=999999999)

        # Convert to pandas DataFrame
        results_df = pd.DataFrame.from_records(results)

        #Create CSV in memory
        results_df.to_csv(csv_buffer)
        csv_buffer.seek(0)

        local_file_name = str("BusIncidents_050125") + ".csv"
        #upload_file_path = os.path.join(local_path, results_df.to_csv('BusIncidents_050825.csv'))

        #     # Write text to the file
        #     file = open(file=upload_file_path, mode='w')
        #     file.write(str(f'Uploaded on {datetime.time}'))
        #     file.close()

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        
except Exception as ex:
        print('Exception:')
        print(ex)