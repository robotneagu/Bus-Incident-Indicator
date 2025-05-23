##Make sure you are connected to Azure in the terminal by using az login. You may have to install Azure tools to execute this.
#Install azure-storage-blob & azure-identity to connect to the cloud and modify files.
'''Source: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=managed-identity%2Croles-azure-portal%2Csign-in-visual-studio-code&pivots=blob-storage-quickstart-scratch'''
import os, uuid, io
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

import pandas as pd
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

        ####Transformation####
        #Remove duplicate index & incident_number which has too many NULLs
        results_df = results_df.drop(columns=['incident_number'])

        #Delete all rows with NULL values
        results_df = results_df.dropna()
        results_df.shape
        
        #Reset index
        df_reset = results_df.reset_index(drop=True)
        df_reset.head()
        
        #Change the types for each column
        results_df = results_df.convert_dtypes()

        #Fix date columns
        #occurred, created, informed, last updated

        date_cols = ['occurred_on', 'created_on', 'informed_on', 'last_updated_on']
        results_df[date_cols] = results_df[date_cols].apply(pd.to_datetime)

        results_df.dtypes
        
        #Transforming dates to their correct format if applicable
        results_df[date_cols] = results_df[date_cols].apply(lambda x: x.dt.strftime('%Y-%m-%d'))
        
        #Reset index
        results_df.reset_index(drop=True)

        #Making as many dimensions non-strings
        bool_cols_yes = ['has_contractor_notified_schools', 'has_contractor_notified_parents', 'have_you_alerted_opt']
        num_cols = ['busbreakdown_id', 'number_of_students_on_the_bus']

        #Fix inputs from drivers to be only integers
        results_df['how_long_delayed'] = results_df['how_long_delayed'].str[:2]
        results_df.rename(columns={'how_long_delayed':'how_long_delayed_in_min'}, inplace=True)
        
        #Binarize columns
        results_df[bool_cols_yes] = results_df[bool_cols_yes].map(lambda x: True if x == 'Yes' else False)

        results_df['breakdown_or_running_late'] = results_df['breakdown_or_running_late'].apply(lambda x: True if x == 'Breakdown' else False)
        results_df.rename(columns={'breakdown_or_running_late':'breakdown'}, inplace=True)

        results_df['school_age_or_prek'] = results_df['school_age_or_prek'].apply(lambda x: True if x == 'Pre-K' else False)
        results_df.rename(columns={'school_age_or_prek':'pre_k'}, inplace=True)
        
        results_df = pd.DataFrame.convert_dtypes(results_df)
        results_df.reset_index(drop=True)
        results_df[date_cols] = results_df[date_cols].apply(pd.to_datetime)
        results_df.dtypes

        #Splitting occurred_on into quarter, month, etc.
        results_df['occurred_on_quarter'] = results_df['occurred_on'].dt.quarter
        results_df['occurred_on_month'] = results_df['occurred_on'].dt.month
        results_df['occurred_on_day'] = results_df['occurred_on'].dt.day
        results_df['occurred_on_hour'] = results_df['occurred_on'].dt.hour
        results_df['occurred_on_day_of_week'] = results_df['occurred_on'].dt.day_of_week
        results_df['occurred_on_week_of_month'] = results_df['occurred_on'].apply(lambda d: (d.day - 1) // 7 + 1)
        results_df['week_of_year'] = results_df['occurred_on'].dt.isocalendar().week
        import holidays
        results_df['occurred_on_holiday'] = results_df['occurred_on'].dt.date.apply(lambda x: x in holidays.US())
        results_df['occurred_on_weekend'] = results_df['occurred_on'].dt.dayofweek >= 5
        results_df['occurred_on_month_name'] = results_df['occurred_on'].dt.month_name()
        results_df['occurred_on_day_name'] = results_df['occurred_on'].dt.day_name()

        #Make sure new columns listed are strings
        results_df['occurred_on_month_name'] = results_df['occurred_on_month_name'].astype(str)
        results_df['occurred_on_day_name'] = results_df['occurred_on_day_name'].astype(str)

        local_file_name = str("BusIncidents_050125") + ".csv"

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
               
except Exception as ex:
        print('Exception:\n', ex)