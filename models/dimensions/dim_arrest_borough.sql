with dim_table as(

    select row_number() over (order by borough) as borough_sk, 

    (case
        when borough IS NULL then 'UNDEFINED'
        else upper(borough)
    end
    ) as borough,

    (case
        when borough = 'MANHATTAN' then '1'
        when borough = 'BRONX' then '2'
        when borough = 'BROOKLYN' then '3'
        when borough = 'QUEENS' then '4'
        when borough = 'STATEN ISLAND' then '5'
        else '6'
    end
    ) as boro_code
    
    from (
    select distinct borough
    from {{ ref('stg_nyc_mv_collision_crashes') }}
    ) 

)
select * from dim_table



