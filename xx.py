import io
import pandas as pd
from datetime import timedelta
from minio.error import S3Error
from minio import Minio
from minio_config import config


def main():
    client = Minio(
        'localhost:9000',
        access_key=config['access_key'],
        secret_key=config['secret_key'],
        secure=False
    )
    bucket_name = 'bronze'
    objects = client.list_objects(bucket_name, recursive=True)

    for obj in objects:
        if 'nyc_taxi_data' in obj.object_name:
            url = client.get_presigned_url(
                'GET',
                bucket_name,
                obj.object_name,
                expires=timedelta(hours=1)
            )

            # Read the Parquet file from the s3 bucket url
            data = pd.read_parquet(url)

            for index, row in data.iterrows():
                print(row)
                break
        break


if __name__ == '__main__':
    try:
        main()
    except S3Error as e:
        print(f'Something Went Wrong !: {e}')
