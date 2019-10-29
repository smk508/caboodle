# Imports the Google Cloud client library
from google.cloud import storage
from typing import List, Tuple, Union
import io
# Instantiates a client
try:
    storage_client = storage.Client()
except Exception as e:
    print("{0}\n\n Try setting the environment variable GOOGLE_APPLICATION_CREDENTIALS to point to the file containing your service account key.".format(e))
    raise e

import os

def printv(*args, verbose=True, **kwargs):
    if verbose:
        print(*args, **kwargs)

def upload_all(path, bucket_name, folder_name, verbose=True, replace=True, use_filepaths=True):
    """ 
    This uploads all files under the given path. If path is a directory, this function will
    traverse it; if path points to a file, only that file will be uploaded.
    This uses the Google Cloud storage client referred to by the environment variable 
    GOOGLE_APPLICATION_CREDENTIALS
    Args:
        path: Path to upload from. When uploading, the directory names will be stripped except for the last one.
        bucket_name: Name of bucket to use
        folder_name: Name of folder to upload under
        verbose (default True): Whether or not to print info about upload.
        replace (default True): If False, then all files that already exist in the bucket will not be uploaded.
    """

    # Get bucket and blob from client
    bucket = storage_client.get_bucket(bucket_name)
    depth = len(path.split('/'))
    stripped_path = path.split('/')[-1]
    if os.path.isfile(path):
        # Upload just this file
        if use_filepaths:
            blob = bucket.blob(os.path.join(folder_name, stripped_path)) 
        else:
            blob = bucket.blob(folder_name)
        blob.upload_from_filename(path)
    elif os.path.isdir(path):
        # Traverse folder and upload files
        for r, d, f in os.walk(path):
            for filename in f:
                full_filename = os.path.join(r, filename) # Path to file on disk
                base = os.path.join(*r.split('/')[depth-1:]) # Strip away preceding foldernames
                if use_filepaths:
                    relative_filename = os.path.join(folder_name, base, filename) # Path to file in bucket
                else:
                    relative_filename = os.path.join(folder_name, filename) # Path to file in bucket
                printv("Uploading {0}".format(full_filename), verbose=verbose)
                blob = bucket.blob(relative_filename)
                if not replace:
                    if blob is not None and blob.exists(): # Blob already exists
                        print("Skipping {0}".format(relative_filename))
                        continue
                blob.upload_from_filename(full_filename)
    else:
        raise ValueError("The provided path does not point to a file or directory: {0}".format(path))

    printv("Uploaded all files in {0} for bucket {1} under folder {2}".format(path, bucket_name, folder_name))

def upload_string(string, bucket_name, path, verbose=True, replace=True):
    """
    Uploads the contents of string to a GCS bucket at the given path.
    """
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(string)

def download_file_to_memory(bucket_name, file_name):
    """ Downloads a file hosted in a bucket into a StringIO buffer. """
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    buffer = io.BytesIO()
    storage_client.download_blob_to_file(blob, buffer)
    string_buffer = io.StringIO(buffer.getvalue().decode('utf-8'))

    return string_buffer

def download_file_to_path(bucket_name, file_name, path):
    """ Downloads a file hosted in a bucket to the chosen path. """
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    with open(path, 'wb') as f:
        storage_client.download_blob_to_file(blob, f)