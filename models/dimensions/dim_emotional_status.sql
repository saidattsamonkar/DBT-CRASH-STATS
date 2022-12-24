with dim_table as(

    select row_number() over (order by emotional_status) as emotional_status_sk, 
    
     lower(emotional_status) as emotional_status,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct emotional_status
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

    where emotional_status IS NOT NULL

)

select * from dim_table
order by emotional_status_sk