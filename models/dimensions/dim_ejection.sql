with dim_table as(

    select row_number() over (order by ejection) as ejection_sk, 
    
     lower(ejection) as ejection,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct ejection
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

    where ejection IS NOT NULL

)

select * from dim_table
order by ejection_sk