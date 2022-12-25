with dim_table as(

    select row_number() over (order by ped_location) as ped_location_sk, 
    
    ped_location,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct ped_location
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

    where ped_location IS NOT NULL

)

select * from dim_table
order by ped_location_sk