with dim_table as(

    select row_number() over (order by safety_equipment) as safety_equipment_sk, 
    
    safety_equipment,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct safety_equipment
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

    where safety_equipment is not NULL and safety_equipment != '-'
   
)

select * from dim_table
order by safety_equipment_sk