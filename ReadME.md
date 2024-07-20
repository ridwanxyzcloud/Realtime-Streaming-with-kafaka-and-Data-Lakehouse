https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

producer.py - python script that reads data and sends to bucket 

 for obj in objects:
        print(obj.object_name, '\n')
 

the presigned_url givres duration to a file for a duration of time 
# this is a security protection on files on s3 bucket

meta_data = { 
     
VendorID                                   2
tpep_pickup_datetime     2023-01-01 00:32:10
tpep_dropoff_datetime    2023-01-01 00:40:36
passenger_count                          1.0
trip_distance                           0.97
RatecodeID                               1.0
store_and_fwd_flag                         N
PULocationID                             161
DOLocationID                             141
payment_type                               2
fare_amount                              9.3
extra                                    1.0
mta_tax                                  0.5
tip_amount                               0.0
tolls_amount                             0.0
improvement_surcharge                    1.0
total_amount                            14.3
congestion_surcharge                     2.5
airport_fee                              0.0

}

