import glob
from google.cloud import storage
import os

client = storage.Client()

local_path = '/home/susurendhar/BEAD/newsArticles/news-please/cc_store'
bucket = client.get_bucket('bead-data')
BUCKET_FOLDER_DIR = 'bead-data/newsArticles'

def upload_local_directory_to_gcs(local_path, bucket, gcs_path):
    assert os.path.isdir(local_path)
    for local_file in glob.glob(local_path + '/**'):
        if not os.path.isfile(local_file):
           upload_local_directory_to_gcs(local_file, bucket, gcs_path + "/" + os.path.basename(local_file))
    else:
        remote_path = os.path.join(gcs_path, local_file[1 + len(local_path):])
        blob = bucket.blob(remote_path)
        blob.upload_from_filename(local_file)



upload_local_directory_to_gcs(local_path, bucket, BUCKET_FOLDER_DIR)