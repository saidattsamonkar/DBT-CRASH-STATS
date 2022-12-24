with temp_table as(

    select (case 
        when upper(driver_license_jurisdiction) in ('AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
            'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MP', 'MS','MT',
            'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 
            'UM', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY', 'NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB', 'SK', 'AB',
            'BC', 'YT', 'NT', 'NU') then upper(driver_license_jurisdiction) 
        else 'UNKNOWN'
        end) as driver_license_jurisdiction
    from {{ ref('stg_nyc_mv_collision_vehicles') }}
),

dim_table as(

    select row_number() over (order by driver_license_jurisdiction) as driver_license_jurisdiction_sk, 
    
    driver_license_jurisdiction,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        select distinct driver_license_jurisdiction
        from temp_table
    )
)

select * from dim_table
order by driver_license_jurisdiction_sk