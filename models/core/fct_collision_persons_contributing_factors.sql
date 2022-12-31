with persons_cf as (
    select unique_id, collision_id, person_id, 
    
    (case
    when contributing_factor_1 is null then 'unspecified'
    else lower(contributing_factor_1)
    end) as contributing_factor,
    1 as factor_number
    
    from {{ ref('stg_nyc_mv_collision_persons') }}

    union

    select unique_id, collision_id, person_id, lower(contributing_factor_2) as contributing_factor, 2 as factor_number
    
    from {{ ref('stg_nyc_mv_collision_persons') }}

    where contributing_factor_2 is not null 
    and (lower(contributing_factor_2)!=lower(contributing_factor_1))
),

temp_table as (

    select pcf.unique_id, pcf.collision_id, pcf.person_id, pcf.factor_number, dcf.contributing_factor_sk
    from persons_cf as pcf
    left join {{ ref('dim_contributing_factor') }} as dcf
    on pcf.contributing_factor = dcf.contributing_factor
    
),

final as(

    select row_number() over (order by unique_id) as table_sk,

    unique_id, collision_id, person_id, factor_number, contributing_factor_sk,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from temp_table

    order by table_sk

)


select * from final

