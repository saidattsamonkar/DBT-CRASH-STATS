with dim_table as(

    select row_number() over (order by point_of_impact) as point_of_impact_sk, 
    
    (case
        when point_of_impact is NULL then 'Unknown'
        else point_of_impact
    end) as point_of_impact,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct point_of_impact
        from {{ ref('stg_nyc_mv_collision_vehicles') }}
    ) 
    
)

select * from dim_table
order by point_of_impact_sk