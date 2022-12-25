with temp_table as (

    select 
    (case
        when travel_direction = 'S' then 'south'
        when travel_direction = 'E' then 'east'
        when travel_direction = 'W' then 'west'
        when travel_direction = 'N' then 'north'
        when travel_direction is NULL or travel_direction = '-' or travel_direction = 'U' then 'unknown'
        else lower(travel_direction)
    end) as travel_direction

    from {{ ref('stg_nyc_mv_collision_vehicles') }}

),

dim_table as(

    select row_number() over (order by travel_direction) as travel_direction_sk, 
    
    travel_direction,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct travel_direction
        from temp_table
    ) 
   
)

select * from dim_table
order by travel_direction_sk
