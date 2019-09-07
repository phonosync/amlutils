import os
import glob
import json
from io import BytesIO
from azure.storage.common import CloudStorageAccount


def get_blob_service():
    account = CloudStorageAccount(account_name=os.environ.get('ACCOUNT_NAME'),
                                  account_key=os.environ.get('ACCOUNT_ACCESS_KEY'),
                                  is_emulated=(os.environ.get('IS_EMULATED') == 'True'))
    return account.create_block_blob_service()

def get_file_blob_to_path(container, blob_name, dest_fp):
    block_blob_service = get_blob_service()
    return block_blob_service.get_blob_to_path(container, blob_name, dest_fp)

def get_binary_blob(container, blob_name):
    block_blob_service = get_blob_service()
    io_object = BytesIO(block_blob_service.get_blob_to_bytes(container_name=container,blob_name=blob_name).content)
    return io_object

def get_json_blob(container, blob_name):
    block_blob_service = get_blob_service()
    b = block_blob_service.get_blob_to_text(container, blob_name)
    return json.loads(b.content)

def upload_files(container, file_or_wildcard, path):

    block_blob_service = get_blob_service()
    files = glob.glob(os.path.join(path, file_or_wildcard))
    print(files)
    for file in files:
        print('attempting to upload '+file)
        block_blob_service.create_blob_from_path(container, os.path.basename(file), file)
    print('after attempt')

def list_blobs_in_container(container_name, filter_arg=None):
    block_blob_service = get_blob_service()
    print(container_name)
    bs = block_blob_service.list_blobs(container_name)

    return [b.name for b in bs]