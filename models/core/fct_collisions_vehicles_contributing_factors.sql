with vehicles_cf as (
    select unique_id, collision_id, vehicle_id, 
    
    (case
    when contributing_factor_1 is null then 'unspecified'
    else lower(contributing_factor_1)
    end) as contributing_factor,
    1 as factor_number
    
    from {{ ref('stg_nyc_mv_collision_vehicles') }}

    union

    select unique_id, collision_id, vehicle_id, lower(contributing_factor_2) as contributing_factor, 2 as factor_number
    
    from {{ ref('stg_nyc_mv_collision_vehicles') }}

    where contributing_factor_2 is not null 
    and (lower(contributing_factor_2)!=lower(contributing_factor_1))
),

temp_table as (

    select vcf.unique_id, vcf.collision_id, vcf.vehicle_id, vcf.factor_number, dcf.contributing_factor_sk
    from vehicles_cf as vcf
    left join {{ ref('dim_contributing_factor') }} as dcf
    on vcf.contributing_factor = dcf.contributing_factor
    
),

final as(

    select row_number() over (order by unique_id) as table_sk,

    unique_id, collision_id, vehicle_id, factor_number, contributing_factor_sk,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from temp_table

    order by table_sk

)


select * from final

