import requests
import os
import io
from dotenv import load_dotenv
from datetime import date, timedelta
import json
import polars as pl
import pyarrow.parquet as pq
from adlfs import AzureBlobFileSystem

from dagster import AssetExecutionContext, MetadataValue, asset, AssetOut, multi_asset, AssetKey, DataVersion, Output

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
#from azure.core.credentials import AzureKeyCredential

load_dotenv()

solar_key = os.getenv('SOLAR_EDGE_API_KEY')
solar_site = os.getenv('SOLAR_EDGE_SITE_ID')
storage_creds = os.getenv('AZURE_STORAGE_KEY')
storage_account = os.getenv('AZURE_STORAGE_ACCOUNT')
storage_container = 'solar'



def upload_blob(filename,data):
    blob_service = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client(storage_container)
    if not blob_container.exists():
        blob_container.create_container()
    blob_container.upload_blob(filename, data, overwrite=True)

def download_blob(filename):
    blob_service = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=storage_creds)
    blob_client = blob_service.get_blob_client(container=storage_container, blob=filename)
    # readinto() downloads the blob contents to a stream and returns the number of bytes read
    download_stream = blob_client.download_blob()
    return download_stream

def list_processed():
    blob_service = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client(storage_container)
    blob_list = blob_container.list_blobs()
    return [blob.name[-15:-5] for blob in blob_list if blob.name[:27]=='raw/power_detail/processed/']

def list_unprocessed():
    blob_service = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client(storage_container)
    blob_list = blob_container.list_blobs()
    return [blob.name for blob in blob_list if blob.name[:26]=='raw/power_detail/incoming/']

def list_blobs(path):
    blob_service = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client(storage_container)
    blob_list = blob_container.list_blobs()
    return [blob.name for blob in blob_list if blob.name[:len(path)]==path]

def delete_blob(pathname):
    blob_service = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=storage_creds)
    blob_client = blob_service.get_blob_client(container=storage_container, blob=pathname)
    blob_client.delete_blob()

#  generate list of dates between 7/1/2023 and T-1 remove any dates for files in processed
def get_target_dates():
    end=date.today() - timedelta(days = 1)
    dates = pl.date_range(date(2023,6,1),end,eager=True).cast(pl.Utf8)
    processed = list_processed()
    return [dt for dt in dates if dt not in processed]

# download a json for all missing dates between 2023-07-01 and T-1 and upload to incoming
@asset(code_version='1')
def power_detail_raw(context: AssetExecutionContext):
    load_dates = get_target_dates()
    path = '/raw/power_detail/incoming/'
    for target_date in load_dates:
        start_time = str(target_date) + '%2000:00:00'
        end_time = str(target_date) + '%2023:59:59'
        power_detail_url = f'https://monitoringapi.solaredge.com/site/{solar_site}/powerDetails?startTime={start_time}&endTime={end_time}&api_key={solar_key}'

        r = requests.get(power_detail_url)
        data = r.json()
        filename = str(target_date)+'.json'
        data_string = json.dumps(data)
        upload_blob(path+filename,data_string)
    context.add_output_metadata({
            "text_metadata": "Raw detail json files from Solar Edge API.  One file per date.",
            "remote_path": MetadataValue.path(path)
            #"dashboard_url": MetadataValue.url(
            #    "http://mycoolsite.com/url_for_my_data"
            #),
    })
    
    #return DataVersion(','.join(list_unprocessed()))
    output = ','.join(list_unprocessed())
    data_version = str(hash(output))
    return Output(output,data_version = DataVersion(data_version))

# reads each meter type and stores in parquet file
# saves blob in processed directory and deletes from incoming
@multi_asset(deps=[power_detail_raw],
             outs={
                 "power_consumption" : AssetOut(),
                 "power_selfconsumption" : AssetOut(),
                 "power_production" : AssetOut(),
                 "power_feedin" : AssetOut(),
                 "power_purchased" : AssetOut()
             }
             ,code_version='1')
def split_detail():
    incoming_blobs = list_unprocessed()
    #file = download_blob(path+start_date+'.json')
    table_list = ['power_consumption','power_selfconsumption','power_production','power_feedin','power_purchsed']
    for blob in incoming_blobs:
        file = download_blob(blob)
        date_string = blob[-15:-5]
        data = json.loads(file.read())
        meters = data['powerDetails']['meters']
        for meter in meters:
            filename = f"/data/power_{meter['type'].lower()}/{date_string}.parquet"
            df = pl.DataFrame(meter['values'])
            stream = io.BytesIO()
            df.write_parquet(stream)
            upload_blob(filename,stream.getbuffer())
        processed_blob = blob.replace('/incoming/','/processed/')
        upload_blob(processed_blob,json.dumps(data))
        delete_blob(blob)
    output = []
    for table in table_list:
        single_output = ','.join(list_blobs(f"data/{table}/"))
        data_version = str(hash(single_output))
        output.append(Output(single_output,data_version=DataVersion(data_version)))
    return tuple(output)

@asset(deps=[AssetKey("power_consumption"),AssetKey("power_production")]
       ,code_version='1')
def power_net_production():
    abfs = AzureBlobFileSystem(account_name=storage_account,account_key=storage_creds)
    consumption_path = '/data/power_consumption/'
    production_path = '/data/power_production/'
    consumption = pl.from_arrow(pq.read_table(storage_container+consumption_path,filesystem=abfs))
    production = pl.from_arrow(pq.read_table(storage_container+production_path,filesystem=abfs))
    consumption = consumption.rename({'value':'consumption'})
    production = production.rename({'value':'production'})
    df = consumption.join(production,on='date',how='outer')
    df = df.rename({'date':'timestamp'})
    df = df.with_columns((pl.col('production') - pl.col('consumption')).alias('net_production'),pl.col('timestamp').str.slice(0,length=10).alias('date'))
    pq.write_table(df.to_arrow(),storage_container+'/data/power_net_production/data.parquet',filesystem=abfs)

@asset(deps=[power_net_production],
       code_version='1')
def net_production_dashboard(context: AssetExecutionContext):
    abfs = AzureBlobFileSystem(account_name=storage_account,account_key=storage_creds)
    path = '/data/power_net_production/'
    net_production = pl.from_arrow(pq.read_table(storage_container+path,filesystem=abfs))
    output_file = '/data/net_production.csv'
    net_production.write_csv(output_file)
    context.add_output_metadata({
        "text_metadata": "Chart.js dashboard hosted on Github pages"
        #"dashboard_url": MetadataValue.url(
        #    "http://githubpages url"
        #),
    })
    os.system(f'git add {output_file}')
    os.system(f'git commit -m "data sync {str(date.today())}"')
    os.system('git push origin main')