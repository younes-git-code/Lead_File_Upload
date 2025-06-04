from configs import storage_client, bucket_name, UPLOAD_DIR
from datetime import datetime

def get_list_of_files():
    blobs = storage_client.list_blobs(bucket_name)
    files = []
    for blob in blobs:
        files.append(blob.name)
    return files

def upload_file(uploaded_file):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{UPLOAD_DIR}/lead_{timestamp}.csv"
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob.upload_from_filename(file_path)
    return file_path

def download_file(file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.download_to_filename(file_name)
    blob.reload()
    return
