# ELT pipeline for NYC Collisions Dimensional Model using DBT, Snowflake and AWS
The aim of this project is to build a Dimensional Model for NYC vehicle collisions dataset available on NYC OpenData. We Use 3 Datasets:
- Motor Vehicle Collisions - Crashes (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95)
- Motor Vehicle Collisions - Person (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Person/f55k-p6yu)
- Motor Vehicle Collisions - Vehicles (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4)

Python and AWS S3 storage service is used to load data into Snowflake. Then we use DBT on top of Snowflake to help us with the transformations.The resulting dimesional model has 26 dimensions and 7 fact tables

## Final DBT Project DAG
![DAG](https://github.com/saidattsamonkar/DBT-CRASH-STATS/blob/main/DBT_SC.png)

## Step 1 - Data Extraction
The data was fetched using python requests module to make API calls to OpenData API, get the data in JSON format and upload it to an S3 bucket using boto3 client library

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
First we need to set up Snow Pipe to run COPY INTO command to load files to our Snowflake internal tables and delete the file from our external table
```
CREATE PIPE my_pipe
AUTO_INGEST = TRUE
AS 
BEGIN;
COPY INTO nyc_collisions_crashes
FROM @my_stage/Crashes.json
FILE_FORMAT = (TYPE = 'JSON' NULL_IF = ('""'))
ON_ERROR = 'CONTINUE';
REMOVE @my_stage/Crashes.json;
COMMIT;
```

Then we set up a task to check for new files in our Snowflake external stage

```
CREATE TASK my_task
  WAREHOUSE = my_warehouse
  SCHEDULE = '5 minutes'
  AS
    BEGIN;
    IF EXISTS (SELECT 1 FROM table(INFORMATION_SCHEMA.FILES) WHERE file_name = 'Crashes.json' and stage_name = 'my_stage') 
    THEN
      ALTER PIPE my_pipe RESUME; 
      REMOVE @my_stage/Crashes.json;
    END IF;
    COMMIT;
```

## Step 5 - Transform using DBT
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
