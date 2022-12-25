with dim_table as(

    select row_number() over (order by position_in_vehicle) as position_in_vehicle_sk, 
    
    lower(position_in_vehicle),

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct position_in_vehicle
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

    where position_in_vehicle is not NULL
    
)

select * from dim_table
order by position_in_vehicle_sk