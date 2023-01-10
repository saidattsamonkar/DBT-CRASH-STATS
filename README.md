# ELT pipeline for NYC Collisions Dimensional Model using DBT, Snowflake and AWS


### Final DBT Project DAG
![DAG](https://github.com/saidattsamonkar/DBT-CRASH-STATS/blob/main/DBT_SC.png)

## Step 1 - Data Extraction
The data was fetched using python requests module to make API calls to OpenData API, get the data in JSON format and upload it to an S3 bucket using boto3 client library

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](http://community.getbdt.com/) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
