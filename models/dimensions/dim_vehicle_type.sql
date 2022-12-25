with dim_table as(

    select row_number() over (order by vehicle_type) as vehicle_type_sk, 
    
    vehicle_type,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct vehicle_type
        from {{ ref('stg_nyc_mv_collision_vehicles') }}
    ) 
   
)

select * from dim_table
order by vehicle_type_sk
