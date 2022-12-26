with dim_table as(

    select row_number() over (order by person_sex) as person_sex_sk, 
    
    person_sex,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct person_sex
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

    where person_sex is not NULL
    
)

select * from dim_table
order by person_sex_sk