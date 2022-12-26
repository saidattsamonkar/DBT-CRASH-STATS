with dim_table as(

    select row_number() over (order by pre_crash) as pre_crash_sk, 
    
    (case
    when pre_crash is NULL then 'Unknown'
    else pre_crash
    end) as pre_crash,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct pre_crash
        from {{ ref('stg_nyc_mv_collision_vehicles') }}
    ) 
   
)

select * from dim_table
order by pre_crash_sk