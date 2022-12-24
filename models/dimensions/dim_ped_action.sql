with dim_table as(

    select row_number() over (order by ped_action) as ped_action_sk, 
    
     lower(ped_action) as ped_action,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct ped_action
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

    where ped_action IS NOT NULL

)

select * from dim_table
order by ped_action_sk