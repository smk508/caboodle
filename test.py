from caboodle.gcs import *
demultiplexed_path = "gs://covid-bronx/run1/multiplexed" 
local_multiplexed = "/tmp/input/multiplexed"
bucket, gcs_path = parse_gcs_path(demultiplexed_path)
client = get_storage_client()
download_folder_to_path(bucket, gcs_path, local_multiplexed, storage_client=client, asynchronous=True)