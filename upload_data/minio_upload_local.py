from minio import Minio
from minio.error import S3Error
import os

# Set your Minio credentials and endpoint
minio_credentials_local = {
    'endpoint': 'localhost:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
}

# 
minio_client = Minio(
    minio_credentials_local['endpoint'],
    access_key=minio_credentials_local['access_key'],
    secret_key=minio_credentials_local['secret_key'],
    secure=False,
    region=''
)

# Set local directory path
local_directory = "./data"

# List all files in the local directory
files_to_upload = [f for f in os.listdir(local_directory) if f.endswith('.parquet')]

# Set the Minio bucket name
minio_bucket = 'parquetfiles'

# Create the Minio bucket if it doesn't exist
if not minio_client.bucket_exists(minio_bucket):
    minio_client.make_bucket(minio_bucket)
    print(f'Bucket created: {minio_bucket}')

# Upload each .parquet file to Minio
for file_to_upload in files_to_upload:
    local_file_path = os.path.join(local_directory, file_to_upload)
    object_name = file_to_upload

    try:
        # Upload the file
        minio_client.fput_object(minio_bucket, object_name, local_file_path)
        print(f'Successfully uploaded: {object_name}')
    except S3Error as e:
        print(f'Error uploading {object_name}: {e}')

print('Upload process completed!')
