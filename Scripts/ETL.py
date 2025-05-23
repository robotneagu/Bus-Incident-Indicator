#Read the CSV file
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

import pandas as pd
import os

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

try:
        df = pd.read_csv('https://rhomework2025.blob.core.windows.net/busincidentstorage/BusIncidents_050125.csv')
        
        #Remove unnecessary columns
        df.drop(columns=['incident_number'], inplace=True)
        
        #Delete all rows with NULL values
        df = df.dropna()

        #Reset index
        df.reset_index(drop=True)
        
        #Change the types for each column
        df = df.convert_dtypes()

        #Fix date columns
        #occurred, created, informed, last updated

        date_cols = ['occurred_on', 'created_on', 'informed_on', 'last_updated_on']
        df[date_cols] = df[date_cols].apply(pd.to_datetime)

        #Transforming dates to their correct format if applicable
        df[date_cols] = df[date_cols].apply(lambda x: x.dt.strftime('%Y-%m-%d'))

        #Reset index
        df.reset_index(drop=True)
        
        #Initialize numeric and bool columns
        bool_cols_yes = ['has_contractor_notified_schools', 'has_contractor_notified_parents', 'have_you_alerted_opt']
        num_cols = ['busbreakdown_id', 'number_of_students_on_the_bus']

        #Fix inputs from drivers to be only integers
        df['how_long_delayed'] = df['how_long_delayed'].str[:2]
        df.rename(columns={'how_long_delayed':'how_long_delayed_in_min'}, inplace=True)
        
        #Binarize columns
        df[bool_cols_yes] = df[bool_cols_yes].map(lambda x: True if x == 'Yes' else False)

        df['breakdown_or_running_late'] = df['breakdown_or_running_late'].apply(lambda x: True if x == 'Breakdown' else False)
        df.rename(columns={'breakdown_or_running_late':'breakdown'}, inplace=True)

        df['school_age_or_prek'] = df['school_age_or_prek'].apply(lambda x: True if x == 'Pre-K' else False)
        df.rename(columns={'school_age_or_prek':'pre_k'}, inplace=True)
        
        #Fix date columns
        df = pd.DataFrame.convert_dtypes(df)
        df.reset_index(drop=True)
        df[date_cols] = df[date_cols].apply(pd.to_datetime)
        
        #Splitting occurred_on into quarter, month, etc.
        df['occurred_on_quarter'] = df['occurred_on'].dt.quarter
        df['occurred_on_month'] = df['occurred_on'].dt.month
        df['occurred_on_day'] = df['occurred_on'].dt.day
        df['occurred_on_hour'] = df['occurred_on'].dt.hour
        df['occurred_on_day_of_week'] = df['occurred_on'].dt.day_of_week
        df['occurred_on_week_of_month'] = df['occurred_on'].apply(lambda x: (x.day - 1) // 7 + 1)
        df['week_of_year'] = df['occurred_on'].dt.isocalendar().week
        import holidays
        df['occurred_on_holiday'] = df['occurred_on'].dt.date.apply(lambda x: x in holidays.US())
        df['occurred_on_weekend'] = df['occurred_on'].dt.dayofweek >= 5
        df['occurred_on_month_name'] = df['occurred_on'].dt.month_name()
        df['occurred_on_day_name'] = df['occurred_on'].dt.day_name()
        
        df = df.convert_dtypes()
        
        #Create date dimension
        times = ['occurred_on_quarter', 'occurred_on_month', 'occurred_on_day', 'occurred_on_hour', 'occurred_on_day_of_week', 'occurred_on_week_of_month', 'week_of_year', 'occurred_on_holiday', 'occurred_on_weekend', 'occurred_on_month_name']

        dim_date = pd.DataFrame(df[times]).drop_duplicates()
        dim_date['unique_id'] = range(1, len(dim_date)+1)
        dim_date.reset_index(drop=True, inplace=True)

        #Create foreign key for Date dimension
        df['Unique_ID'] = pd.factorize(df[times].astype(str).agg('-'.join, axis=1))[0]+1
        
        #Create route dimension (boro, route)
        unique_route = df['route_number'].unique()

        dim_route = pd.DataFrame(unique_route, columns=['route_number'])
        dim_route['route_id'] = pd.factorize(dim_route['route_number'])[0]+1

        #Create Route_ID foreign key
        df['Route_ID'] = pd.factorize(df['route_number'].astype(str).transform('-'.join))[0]+1
        
        #Create bus_incident dimension
        dim_incident = df[['run_type', 'reason']].drop_duplicates()
        dim_incident['incident_number'] = range(1, len(dim_incident)+1)
        dim_incident.reset_index(drop=True, inplace=True)
        
        #Create bus_info dimension ID(bus_no, bus_comp, schools serviced)
        #BusID will be based on the bus number

        unique_num = df['bus_no'].unique()

        dim_bus_info = pd.DataFrame(unique_num, columns=['bus_no'])
        dim_bus_info['bus_id'] = pd.factorize(dim_bus_info['bus_no'])[0]+1

        dim_bus_info = pd.merge(dim_bus_info[['bus_no', 'bus_id']], df[['bus_no', 'bus_company_name', 'schools_serviced']], on='bus_no', how='left').drop_duplicates(subset=['bus_no'])
        dim_bus_info.reset_index(drop=True, inplace=True)

        #Create Bus_ID foreign key
        df['Bus_ID'] = pd.factorize(df['bus_no'].astype(str).transform('-'.join))[0]+1
        
        #Creating bus facts dimension (main dimension). Mainly used for bus_incident dimension

        df['Facts_ID'] = pd.factorize(df[['run_type', 'reason']].astype(str).agg('-'.join, axis=1))[0]+1

        df_facts = df[['number_of_students_on_the_bus', 'has_contractor_notified_schools', 'has_contractor_notified_parents', 'have_you_alerted_opt', 'breakdown', 'pre_k', 'how_long_delayed_in_min', 'Unique_ID', 'Route_ID', 'Facts_ID', 'Bus_ID']]

        #Saving all schema files as CSV
        
        dataframes = [
        (dim_bus_info, 'bus_info.csv'),
        (dim_date, 'bus_date.csv'),
        (dim_incident, 'bus_incident.csv'),
        (dim_route, 'bus_route.csv'),
        (df_facts, 'facts.csv')
]
        #Loop through all dataframes and upload to the container
        for dfs, filename in dataframes:
                print(type(dfs))
                print("\nUploading to Azure Storage as blob:\n\t" + filename)
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
                #Credit:https://stackoverflow.com/a/62880745
                blob_client.upload_blob(data=dfs.to_csv(index=False), overwrite=True)
                
except Exception as ex:
        print('Exception:\n', ex)