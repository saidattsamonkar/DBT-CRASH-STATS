with dim_table as(

    select row_number() over (order by bodily_injury) as bodily_injury_sk, 
    
    bodily_injury,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct bodily_injury
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

    where NOT bodily_injury IS NULL

)

select * from dim_table
order by bodily_injury_sk
