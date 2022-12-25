with dim_table as(

    select row_number() over (order by state_registration) as state_registration_sk, 
    
    state_registration,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct state_registration
        from {{ ref('stg_nyc_mv_collision_vehicles') }}
    ) 

    where state_registration is not NULL
   
)

select * from dim_table
order by state_registration_sk
