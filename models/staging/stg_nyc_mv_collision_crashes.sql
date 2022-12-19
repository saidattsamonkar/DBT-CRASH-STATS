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
