with vehicles as(

    select *  from {{ ref('stg_nyc_mv_collision_vehicles') }}

),

temp as(

    (select vehicle_damage from vehicles)
    UNION
    (select vehicle_damage_1 from vehicles)
    UNION
    (select vehicle_damage_2 from vehicles)
    UNION
    (select vehicle_damage_3 from vehicles)
    
),


dim_table as(

    select row_number() over (order by vehicle_damage) as vehicle_damage_sk, 
    
    (case
        when vehicle_damage is NULL then 'unknown'
        else lower(vehicle_damage)
        end) as vehicle_damage,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct vehicle_damage
        from temp
    ) 
)

select * from dim_table
order by vehicle_damage_sk
