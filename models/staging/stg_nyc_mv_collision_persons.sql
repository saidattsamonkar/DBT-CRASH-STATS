with stg_table as (

    select * from {{ source('snow', 'nyc_mv_collision_persons') }}

)

select * from stg_table