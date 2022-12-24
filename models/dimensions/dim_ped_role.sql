with dim_table as(

    select row_number() over (order by ped_role) as ped_role_sk, 
    
    (case
        when ped_role IS NULL then 'unknown'
        else lower(ped_role)
    end
    ) as ped_role,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct ped_role
        from {{ ref('stg_nyc_mv_collision_persons') }}
    ) 

)

select * from dim_table
order by ped_role_sk