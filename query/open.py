from minio import Minio
from minio.error import S3Error
import pyarrow.parquet as pq
import pandas as pd
import io

# Set your Minio credentials and endpoint
minio_credentials_local = {
    'endpoint': 'localhost:9000',  # Update the endpoint to your local Minio address
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
}

minio_client = Minio(
    minio_credentials_local['endpoint'],
    access_key=minio_credentials_local['access_key'],
    secret_key=minio_credentials_local['secret_key'],
    secure=False,
)

# Set the Minio bucket name
minio_bucket = 'parquetfiles'

# Set the query condition
query_condition = 'Age > 30 and Gender == "Male"' 

try:
    # Set the Parquet file name for the query result
    result_parquet_object_name = 'result_query.parquet'

    # Read the original Parquet file from Minio into a Pandas DataFrame
    original_parquet_object_name = 'healthcare_dataset.csv.parquet'
    original_object = minio_client.get_object(minio_bucket, original_parquet_object_name)
    original_df = pd.read_parquet(io.BytesIO(original_object.read()))

    # Perform the query on the data
    result_df = original_df.query(query_condition)

    # Save the result DataFrame to a new Parquet file
    with io.BytesIO() as buffer:
        result_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Upload the new Parquet file to Minio
        minio_client.put_object(
            minio_bucket,
            result_parquet_object_name,
            buffer,
            len(buffer.getvalue()),
            'application/octet-stream'
        )

    print(f'Successfully saved query result to {minio_bucket}/{result_parquet_object_name}')

except S3Error as err:
    print(f'Minio error: {err}')

except Exception as e:
    print(f'An error occurred: {e}')

from minio import Minio
from minio.error import S3Error
import pyarrow.parquet as pq
import pandas as pd
import io

# Set your Minio credentials and endpoint
minio_credentials_local = {
    'endpoint': 'localhost:9000',  # Update the endpoint to your local Minio address
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
}

minio_client = Minio(
    minio_credentials_local['endpoint'],
    access_key=minio_credentials_local['access_key'],
    secret_key=minio_credentials_local['secret_key'],
    secure=False,
)

# Set the Minio bucket name
minio_bucket = 'parquetfiles'

# Set the query condition
query_condition = 'Age > 30 and Gender == "Male"' 

try:
    # Set the Parquet file name for the query result
    result_parquet_object_name = 'result_query.parquet'

    # Read the original Parquet file from Minio into a Pandas DataFrame
    original_parquet_object_name = 'healthcare_dataset.csv.parquet'
    original_object = minio_client.get_object(minio_bucket, original_parquet_object_name)
    original_df = pd.read_parquet(io.BytesIO(original_object.read()))

    # Perform the query on the data
    result_df = original_df.query(query_condition)

    # Save the result DataFrame to a new Parquet file
    with io.BytesIO() as buffer:
        result_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # Upload the new Parquet file to Minio
        minio_client.put_object(
            minio_bucket,
            result_parquet_object_name,
            buffer,
            len(buffer.getvalue()),
            'application/octet-stream'
        )

    print(f'Successfully saved query result to {minio_bucket}/{result_parquet_object_name}')

except S3Error as err:
    print(f'Minio error: {err}')

except Exception as e:
    print(f'An error occurred: {e}')
