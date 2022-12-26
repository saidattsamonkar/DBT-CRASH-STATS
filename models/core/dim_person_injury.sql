with dim_table as(

    select row_number() over (order by person_injury) as person_injury_sk, 
    
    person_injury,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct person_injury
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

)

select * from dim_table
order by person_injury_sk