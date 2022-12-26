

with dim_table as(

    select row_number() over (order by vehicle_make) as vehicle_make_sk, 
    
    (case
    when vehicle_make is null then 'Unknown'
    else vehicle_make
    end) as vehicle_make,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct vehicle_make
        from {{ ref('stg_nyc_mv_collision_vehicles') }}
    ) 
)

select * from dim_table
order by vehicle_make_sk
