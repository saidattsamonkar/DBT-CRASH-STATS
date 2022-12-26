with crashes as(

    select *  from {{ ref('stg_nyc_mv_collision_crashes') }}

),

temp as(

    (select vehicle_type_code1 from crashes)
    UNION
    (select vehicle_type_code2 from crashes)
    UNION
    (select vehicle_type_code_3 from crashes)
    UNION
    (select vehicle_type_code_4 from crashes)
    UNION
    (select vehicle_type_code_5 from crashes)
    
),

dim_table as(

    select row_number() over (order by vehicle_type_code1) as vehicle_type_code_sk, 
    
    vehicle_type_code1 as vehicle_type_code,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct vehicle_type_code1
        from temp
    ) 
   
)

select * from dim_table
order by vehicle_type_code_sk