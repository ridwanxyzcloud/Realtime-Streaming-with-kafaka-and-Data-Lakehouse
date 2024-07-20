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
                vendor_id = str(row['VendorID'])
                formatted_pickup_datetime = row['tpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S')
                formatted_dropoff_datetime = row['tpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S')
                PULocationID = str(row['PULocationID'])
                DOLocationID = str(row['DOLocationID'])
                trip_distance = str(row['trip_distance'])
                fare_amount = str(row['fare_amount'])
                total_amount = str(row['total_amount'])
                file_name = f'trip_{vendor_id}_{formatted_pickup_datetime}.json'

                record = row.to_json()
                record_bytes = record.encode('utf-8')
                record_stream = io.BytesIO(record_bytes)
                record_stream_len = len(record_bytes)

                client.put_object(
                    'nyc-taxi-records',
                    f'nyc_taxi_data/{file_name}',
                    data=record_stream,
                    length=record_stream_len,
                    content_type='application/json'
                )

                print(f'Uploaded {file_name} to Minio')

                break
        break


if __name__ == '__main__':
    try:
        main()
    except S3Error as e:
        print(f'Something Went Wrong !: {e}')