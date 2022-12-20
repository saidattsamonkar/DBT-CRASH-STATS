with stg_table as (

    select *,
    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from {{ source('snow', 'nyc_mv_collision_persons') }}

)

select * from stg_table