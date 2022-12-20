with dim_table as(

    select row_number() over (order by borough) as borough_sk, 
    
    (case
        when borough IS NULL then 'unknown'
        else lower(borough)
    end
    ) as borough,

    (case
        when lower(borough) = 'manhattan' then '1'
        when lower(borough) = 'bronx' then '2'
        when lower(borough) = 'brooklyn' then '3'
        when lower(borough) = 'queens' then '4'
        when lower(borough) = 'staten island' then '5'
        else '6'
    end
    ) as boro_code,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
    select distinct borough
    from {{ ref('stg_nyc_mv_collision_crashes') }}
    ) 

)

select * from dim_table
order by borough_sk



