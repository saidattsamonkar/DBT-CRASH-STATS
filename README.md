# ELT pipeline for NYC Collisions Dimensional Model using DBT, Snowflake and AWS
The aim of this project is to build a Dimensional Model for NYC vehicle collisions dataset available on NYC OpenData. We Use 3 Datasets:
- Motor Vehicle Collisions - Crashes (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95)
- Motor Vehicle Collisions - Person (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Person/f55k-p6yu)
- Motor Vehicle Collisions - Vehicles (https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4)

Python and AWS S3 storage service is used to load data into Snowflake. Then we use DBT on top of Snowflake to help us with the transformations.The resulting dimesional model has 26 dimensions and 7 fact tables

### Final DBT Project DAG
![DAG](https://github.com/saidattsamonkar/DBT-CRASH-STATS/blob/main/DBT_SC.png)

## Step 1 - Data Extraction
The data was fetched using python requests module to make API calls to OpenData API, get the data in JSON format and upload it to an S3 bucket using boto3 client library

```
import requests
import boto3
import json

# URLs of the APIs to fetch data from
api_urls = ['https://www.aaa.com', 'https://www.bbb.com', 'https://www.ccc.com']

# S3 bucket name
bucket_name = 'nyc-collisions-a'

# Create an S3 client
s3 = boto3.client('s3, aws_access_key_id='ACCESS_KEY_ID', aws_secret_access_key='SECRET_ACCESS_KEY')

for url in api_urls:
    # Make the API request
    response = requests.get(url)
    # Parse the JSON data
    data = json.loads(response.text)
    # Create a unique key for the JSON data
    key = url.split('/')[-1] + '.json'
    # Upload the JSON data to the S3 bucket
    s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
```

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](http://community.getbdt.com/) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
