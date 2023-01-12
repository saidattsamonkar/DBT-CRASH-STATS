# ELT pipeline for NYC Collisions Dimensional Model using DBT, Snowflake and AWS

## Introduction
The aim of this project is to build a Dimensional Model for NYC vehicle collisions dataset available on NYC OpenData. We Use 3 Datasets:
- Motor Vehicle Collisions - Crashes (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95)
- Motor Vehicle Collisions - Person (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Person/f55k-p6yu)
- Motor Vehicle Collisions - Vehicles (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4)

Python and AWS S3 is used to load data into Snowflake. Then we use DBT on top of Snowflake to help us with the transformations. The resulting dimesional model has 26 dimensions and 7 fact tables.

## Final DBT Project DAG
![DAG](https://github.com/saidattsamonkar/DBT-CRASH-STATS/blob/main/DBT_SC.png)

## Step 1 - Data Extraction
The data is fetched using python requests module to make API calls to OpenData API, get the data in JSON format and upload it to an S3 bucket using boto3 client library.

```
import requests
import boto3
import json

# List of api paths
api_urls = {'Crahes':'https://data.cityofnewyork.us/resource/h9gi-nx95.json',
            'Person':'https://data.cityofnewyork.us/resource/f55k-p6yu.json',
            'Vehicles':'https://data.cityofnewyork.us/resource/bm4k-52h4.json'}

bucket_name = 'nyc-collisions'

# Create S3 client
s3 = boto3.client('s3', aws_access_key_id='ACCESS_KEY_ID', aws_secret_access_key='SECRET_ACCESS_KEY')

for key in api_urls.keys():

    # Make the API request
    response = requests.get(api_urls[key])
    
    # Parse the JSON data
    data = json.loads(response.text)
    
    # Upload the JSON data to the S3 bucket
    s3.put_object(Bucket=bucket_name, Key=key+'.json', Body=json.dumps(data))
```

## Step 2 - Set up CRON job
We use AWS EventBridge to periodically run a Lambda function containing the python code

## Step 3 - Set up SnowFlake external table
First we set up the s3 bucket as an external stage in Snowflake
```
CREATE STAGE my_stage
URL = 's3://nyc-collisions'
CREDENTIALS = (AWS_KEY_ID='my_access_key' AWS_SECRET_KEY='my_secret_key');
```

## Step 4 - Set up Snowpipe
We set up Snow Pipe to run COPY INTO command to load files to our Snowflake internal tables and delete the file from our external table using:
```
CREATE PIPE my_pipe
AUTO_INGEST = TRUE
AS 
BEGIN;
COPY INTO nyc_mv_collision_crashes
FROM @my_stage/Crashes.json
FILE_FORMAT = (TYPE = 'JSON' NULL_IF = ('""'))
ON_ERROR = 'CONTINUE';
REMOVE @my_stage/Crashes.json;
COMMIT;
```
Then we run the ```RESUME``` command to start the Snow Pipe

## Step 5 - Transform using DBT
Now we can use DBT to transform the data in the snowflake tables and create a dimensional model either as a materialized view or a table. The steps are:
- Set up a connection to Snowflake either through Snowflake partner connect or setting up a connection from inside DBT
- Configure a ```.yml``` file to set the tables as sources
```
version: 2
sources:
  - name: snow
    database: DBT_CRASH
    schema: DBT
    tables:
      - name: nyc_mv_collision_persons
      - name: nyc_mv_collision_crashes
      - name: nyc_mv_collision_vehicles
```
- Set up 3 Staging tables from the three source tables. example  for stg_crashes 
```
with stg_table as (

    select 
    
    unique_key as COLLISION_ID,
    crash_date as collision_day,
    crash_time as collision_time,
    cast(extract(hour from crash_time) as int) as collision_hour,
    cast(extract(dayofweek from crash_date) as int) as collision_dayoftheweek,
    * exclude(unique_key,crash_date,crash_time),
    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from {{ source('snow', 'nyc_mv_collision_crashes') }}

)

select * from stg_table
```
- Create dimension tables for all dimensions, some of which are spread between multiple tables
- Create fact tables using the staging tables and dimension tables
