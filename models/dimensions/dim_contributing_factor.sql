with persons as(

select contributing_factor_1, contributing_factor_2
from {{ ref('stg_nyc_mv_collision_persons') }}

),

vehicles as(

    select contributing_factor_1, contributing_factor_2 
    from {{ ref('stg_nyc_mv_collision_vehicles') }}

),

crashes as(

    select contributing_factor_vehicle_1, contributing_factor_vehicle_2, contributing_factor_vehicle_3,
    contributing_factor_vehicle_4, contributing_factor_vehicle_5 
    from {{ ref('stg_nyc_mv_collision_crashes') }}

),

int_contributing_factors as(

    (select contributing_factor_1 from persons)
    UNION
    (select contributing_factor_2 from persons)
    UNION
    (select contributing_factor_1 from vehicles)
    UNION
    (select contributing_factor_2 from vehicles)
    UNION
    (select contributing_factor_vehicle_1 from crashes)
    UNION
    (select contributing_factor_vehicle_2 from crashes)
    UNION
    (select contributing_factor_vehicle_3 from crashes)
    UNION
    (select contributing_factor_vehicle_4 from crashes)
    UNION
    (select contributing_factor_vehicle_5 from crashes)
    
),


int_contributing_factors_2 as (

    select (case
        when contributing_factor_1 IS NULL then 'unspecified'
        when lower(contributing_factor_1) = 'illnes' then 'illness'
        when contributing_factor_1 regexp '[0-9]+' then 'unspecified'
        else lower(contributing_factor_1)
    end
    ) as contributing_factor
    from int_contributing_factors
),


final as(

    select row_number() over (order by contributing_factor) as contributing_factor_sk,

    contributing_factor,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
    select distinct contributing_factor
    from int_contributing_factors_2
    ) 

)


select * from final
order by contributing_factor_sk



