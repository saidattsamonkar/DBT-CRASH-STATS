with dim_table as(

    select row_number() over (order by complaint) as complaint_sk, 
    
    lower(complaint) as complaint,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct complaint
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

    where NOT complaint IS NULL

)

select * from dim_table
order by complaint_sk