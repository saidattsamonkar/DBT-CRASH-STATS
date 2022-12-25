with
    dim_table as (

        select
            row_number() over (order by person_type) as person_type_sk,

            lower(person_type),

            '{{invocation_id}}' as jobid,
            current_timestamp() as di_create_date_name

        from
            (select distinct person_type from {{ ref("stg_nyc_mv_collision_persons") }})

    )

select *
from dim_table
order by person_type_sk
