# Imports the Google Cloud client library
from google.cloud import storage
from google.resumable_media import DataCorruption
from gcloud.aio.storage import Storage
from typing import List, Tuple, Union
import io
import warnings
import os
from tqdm import tqdm
import asyncio
import aiohttp
import aiofiles
import time
import uvloop
import itertools
from itertools import count

uvloop.install()

def printv(*args, verbose=True, **kwargs):
    if verbose:
        print(*args, **kwargs)

def get_storage_client():
    """
    Instantiates a storage client by reading the environment variable
    GOOGLE_APPLICATION_CREDENTIALS.
    """
    # Instantiates a client
    try:
        storage_client = storage.Client()
    except Exception as e:
        print("Could not instantiate a storage client. \
            Try setting the environment variable GOOGLE_APPLICATION_CREDENTIALS to point to \
            the file containing your service account key."
            )
        raise e
    
    return storage_client


def upload_all(
    path: str,
    bucket_name: str, 
    folder_name: str, 
    verbose: bool = True, 
    replace: bool = True, 
    use_filepaths: bool = True,
    storage_client = None
    ):
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
    storage_client = storage_client or get_storage_client()
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

def upload_string(
    string: str, 
    bucket_name: str,
    path: str, 
    verbose: bool=True, 
    replace: bool=True,
    storage_client = None,
    ):
    """
    Uploads the contents of string to a GCS bucket at the given path.
    """
    storage_client = storage_client or get_storage_client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(path)
    blob.upload_from_string(string)

def download_file_to_memory(
    bucket_name: str, 
    file_name: str, 
    buffer_type: str=None,
    storage_client = None,
    ):
    """ Downloads a file hosted in a bucket into a buffer. """
    storage_client = storage_client or get_storage_client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    buffer = io.BytesIO()
    storage_client.download_blob_to_file(blob, buffer)
    buffer.seek(0)
    if buffer_type == 'string':
        string_buffer = io.StringIO(buffer.getvalue().decode('utf-8'))
        return string_buffer
    else:
        return buffer

def download_file_to_path(
    bucket_name: str, 
    file_name: str, 
    path: str):
    storage_client = storage_client or get_storage_client()
    """ Downloads a file hosted in a bucket to the chosen path. """
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    with open(path, 'wb') as f:
        storage_client.download_blob_to_file(blob, f)

def _make_parent_dirs(filename: str):
    """ Attempts to create directories for any parent directories in filename. """
    components = os.path.split(filename)[0].split("/")
    for i in range(len(components)):
        subpath = "/".join(components[0:i+1])
        if subpath != '':
            if not os.path.isdir(subpath):
                try:
                    print(subpath)
                    os.mkdir(subpath)
                except IOError:
                    raise IOError("Could not create subdirectory {0} when downloading file {1}. Make sure you have the right permissions.".format(subpath, filename))
                    
    
def download_folder_to_path(
    bucket_name: str,
    folder: str,
    path: str,
    suffix: str=None,
    storage_client = None,
    flatten=False,
    asynchronous=False,
    ):
    """ 
    Downloads a folder hosted in a bucket to the chosen path.
    If flatten is set to True, then the hierarchy structure of the cloud folder
    is ignored and all files are downloaded to a single directory.
    """

    storage_client = storage_client or get_storage_client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=folder))
    if suffix:
        blobs = [b for b in blobs if b.name.endswith(suffix)]
    #if not os.path.isdir(path):
    #    raise ValueError("You must first create a folder at {0} before running
    #    this command.".format(path))
    if folder.startswith("/"):
        sublength = len(folder.split("/")) - 1
    else:
        sublength = len(folder.split("/"))
    if asynchronous:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(_download_blobs_async(blobs, flatten, sublength, path))
    else:
        _download_blobs(blobs, flatten, sublength, path, storage_client)

def _download_blobs(blobs, flatten, sublength, path, storage_client=None):

    storage_client = storage_client or get_storage_client()
    for blob in tqdm(blobs):
        if flatten:
            filename = blob.name.split('/')[-1]
        else:
            filename = os.path.join(*blob.name.split('/')[sublength:])
            # Create a folder for the parts of subpath that extend beyond the folder.
            components = blob.name.split('/')

        full_filename = os.path.join(path, filename)
        _make_parent_dirs(full_filename)
        with open(os.path.join(full_filename), 'wb') as f:
            print("Downloading {0} to {1}".format(blob.name, full_filename))
            try:
                storage_client.download_blob_to_file(blob, f)         
            except DataCorruption: # Sometimes there is an error with the MD5 hash, so retry.
                storage_client.download_blob_to_file(blob, f)


def grouper_it(n, iterable):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)

async def _download_blobs_async(blobs, flatten, sublength, path, max_concurrency=10, chunk_size=20):

    #max_queue = min(max_queue, len(blobs))
    max_queue = len(blobs)
    max_concurrency = min(max_concurrency, max_queue)
    semaphore = asyncio.Semaphore(max_concurrency)
    async with aiohttp.ClientSession() as session:
        storage_client = Storage(session=session)
        progress = tqdm(total=len(blobs))
        num_chunks = int(len(blobs) / chunk_size) + 1
        for group in grouper_it(num_chunks, blobs):
            tasks = []
            for blob in group:
                tasks.append(_download_blob_async(blob, semaphore, flatten, sublength, path, storage_client))
            for task in tqdm(tasks):
                await task
                progress.update(1)
            
async def _download_blob_async(blob, semaphore, flatten, sublength, path, storage_client):

        await semaphore.acquire()
        if flatten:
            filename = blob.name.split('/')[-1]
        else:
            filename = os.path.join(*blob.name.split('/')[sublength:])
            # Create a folder for the parts of subpath that extend beyond the folder.
            components = blob.name.split('/')

        full_filename = os.path.join(path, filename)
        _make_parent_dirs(full_filename)
        print("Downloading {0} to {1}".format(blob.name, full_filename))
        try:
            response = await storage_client.download(blob.bucket.name, blob.name, timeout=20000000)
        except:
            response = await storage_client.download(blob.bucket.name, blob.name, timeout=20000000)
        async with aiofiles.open(os.path.join(full_filename), "wb") as af:
            await af.write(response)    
            print("Downloaded {0} to {1}".format(blob.name, full_filename))
        semaphore.release()

        return True

def parse_gcs_path(gcs_path:str) -> Tuple[str,str]:
    """ Parses a gcs path string of the form gs://{bucket-name}/{path} into bucket and path components. """

    # Clean up input
    gcs_path = gcs_path.replace('"', '').replace("'", '')

    if not gcs_path.startswith('gs://'):
        raise ValueError("Argument must be a gcs path string of the form gs://{bucket-name}/{path}")
    
    components = gcs_path.split('/')
    bucket_name = components[2]
    path = os.path.join(*components[3:])
    
    return bucket_name, path

def check_for_files(
    gcs_path: str,
    artifact_names: list,
    storage_client = None):
    """ Checks to see if the specified file names are present in the gcs directory. """
    storage_client = storage_client or get_storage_client()
    bucket_name, path = parse_gcs_path(gcs_path)
    bucket = storage_client.get_bucket(bucket_name)
    names = set(b.name.split('/')[-1] for b in bucket.list_blobs(prefix=path))
    artifact_names = set(artifact_names)
    return artifact_names.issubset(names)

def list_blobs(
    gcs_path, 
    storage_client = None):
    """ Returns a list of names of blobs in the given GCS path. """
    storage_client = storage_client or get_storage_client()
    bucket_name, gcs_folder = parse_gcs_path(gcs_path)
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=gcs_folder)
    return [b.name for b in blobs]
