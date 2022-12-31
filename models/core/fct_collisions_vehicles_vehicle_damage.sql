with vehicles as(
    select unique_id, collision_id, vehicle_id, 
    vehicle_damage as vehicle_damage_1,
    vehicle_damage_1 as vehicle_damage_2,
    vehicle_damage_2 as vehicle_damage_3,
    vehicle_damage_3 as vehicle_damage_4

    from {{ ref('stg_nyc_mv_collision_vehicles') }}
),


vehicles_vd as (
    {% for i in range(1,5) %}
        
    {% if i==1 %}
    select unique_id, collision_id, vehicle_id, 
        (case
        when vehicle_damage_{{i}} is null then 'unknown'
        else lower(vehicle_damage_{{i}})
        end) as vehicle_damage,
        {{i}} as factor_number
    from vehicles
    {%else%}
    union
    select unique_id, collision_id, vehicle_id, lower(vehicle_damage_{{i}}) as vehicle_damage, {{i}} as factor_number
    from vehicles
    where vehicle_damage_{{i}} is not null 
    and ({% for j in range(i-1,0,-1) %}{%if not loop.first%}
    and {% endif %}lower(vehicle_damage_{{i}})!=lower(vehicle_damage_{{j}}){% endfor %})

    {% endif %}
    {% endfor %}
),

temp_table as (

    select vvd.unique_id, vvd.collision_id, vvd.vehicle_id, vvd.factor_number, dvd.vehicle_damage_sk
    from vehicles_vd as vvd
    left join {{ ref('dim_vehicle_damage') }} as dvd
    on vvd.vehicle_damage = dvd.vehicle_damage
    
),

final as(

    select row_number() over (order by unique_id) as table_sk,

    unique_id, collision_id, vehicle_id, factor_number, vehicle_damage_sk,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from temp_table

    order by table_sk

)


select * from final

