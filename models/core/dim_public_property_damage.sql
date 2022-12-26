with dim_table as(

    select row_number() over (order by public_property_damage) as public_property_damage_sk, 
    
    public_property_damage,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct public_property_damage
        from {{ ref('stg_nyc_mv_collision_vehicles') }}
    ) 

    where public_property_damage is not NULL
   
)

select * from dim_table
order by public_property_damage_sk