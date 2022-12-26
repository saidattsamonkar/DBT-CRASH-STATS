with dim_table as(

    select row_number() over (order by driver_license_status) as driver_license_status_sk, 
    
    (case
        when driver_license_status IS NULL then 'Unknown'
        else driver_license_status
    end
    ) as driver_license_status,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct driver_license_status
        from {{ ref('stg_nyc_mv_collision_vehicles') }}
    ) 

)

select * from dim_table
order by driver_license_status_sk