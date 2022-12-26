with dim_table as(

    select row_number() over (order by vehicle_model) as vehicle_model_sk, 
    
    (case
    when vehicle_model is null then 'Unknown'
    else vehicle_model
    end) as vehicle_model,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct vehicle_model
        from {{ ref('stg_nyc_mv_collision_vehicles') }}
    ) 
   
)

select * from dim_table
order by vehicle_model_sk
