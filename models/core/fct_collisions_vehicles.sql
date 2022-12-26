with temp_vehicles as(

    select unique_id, collision_id, vehicle_id,

    (case
        when state_registration is null then 'ZZ'
        else state_registration
    end) as state_registration,

    (case
    when vehicle_type is null then 'Unknown'
    else vehicle_type
    end) as vehicle_type,

    (case
    when vehicle_make is null then 'Unknown'
    else vehicle_make
    end) as vehicle_make,

    (case
    when vehicle_model is null then 'Unknown'
    else vehicle_model
    end) as vehicle_model,

    (case
        when vehicle_year is null then -9999
        else vehicle_year
    end) as vehicle_year,

    (case
        when travel_direction = 'S' then 'South'
        when travel_direction = 'E' then 'East'
        when travel_direction = 'W' then 'West'
        when travel_direction = 'N' then 'North'
        when travel_direction is NULL or travel_direction = '-' or travel_direction = 'U' then 'Unknown'
        else travel_direction
    end) as travel_direction,

    (case
    when vehicle_occupants >= 100 or vehicle_occupants < 0 or vehicle_occupants is NULL then -99 
    else vehicle_occupants
    end)as vehicle_occupants,

    (case
        when driver_sex is null then 'U'
        else driver_sex
    end) as person_sex,

    (case
        when driver_license_status IS NULL then 'Unknown'
        else driver_license_status
    end) as driver_license_status,
    
    (case 
        when upper(driver_license_jurisdiction) in ('AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
            'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MP', 'MS','MT',
            'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 
            'UM', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY', 'NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB', 'SK', 'AB',
            'BC', 'YT', 'NT', 'NU') then upper(driver_license_jurisdiction) 
    else 'ZZ'
    end) as driver_license_jurisdiction,

    (case
        when pre_crash is NULL then 'Unknown'
        else pre_crash
    end) as pre_crash,

    (case
        when point_of_impact is NULL then 'Unknown'
        else point_of_impact
    end) as point_of_impact,

    (case
        when public_property_damage is null then 'Unspecified'
        else public_property_damage
    end) as public_property_damage,

    (case
        when public_property_damage_type is null then 'unspecified'
        else lower(public_property_damage_type)
    end) as public_property_damage_type
    
    from {{ ref('stg_nyc_mv_collision_vehicles') }}
),


dim_state_registration as (
    select * from {{ ref('dim_state_registration') }}
),

dim_vehicle_type as (
    select * from {{ ref('dim_vehicle_type') }}
),

dim_vehicle_make as (
    select * from {{ ref('dim_vehicle_make') }}
),

dim_vehicle_model as (
    select * from {{ ref('dim_vehicle_model') }}
),

dim_travel_direction as (
    select * from {{ ref('dim_travel_direction') }}
),

dim_person_sex as (
    select * from {{ ref('dim_person_sex') }}
),

dim_driver_license_status as (
    select * from {{ ref('dim_driver_license_status') }}
),

dim_driver_license_jurisdiction as (
    select * from {{ ref('dim_driver_license_jurisdiction') }}
),

dim_pre_crash as (
    select * from {{ ref('dim_pre_crash') }}
),

dim_point_of_impact as (
    select * from {{ ref('dim_point_of_impact') }}
),

dim_public_property_damage as (
    select * from {{ ref('dim_public_property_damage') }}
),

temp_table as(

    select tv.unique_id, tv.collision_id, tv.vehicle_id, sr.state_registration_sk, vt.vehicle_type_sk,
    vm.vehicle_make_sk, vmo.vehicle_model_sk, tv.vehicle_year, td.travel_direction_sk, tv.vehicle_occupants,
    ps.person_sex_sk, dls.driver_license_status_sk, dlj.driver_license_jurisdiction_sk, pc.pre_crash_sk,
    poi.point_of_impact_sk, ppd.public_property_damage_sk, tv.public_property_damage_type

    from temp_vehicles as tv

    left join dim_state_registration as sr
    on tv.state_registration = sr.state_registration

    left join dim_vehicle_type as vt
    on tv.vehicle_type = vt.vehicle_type

    left join dim_vehicle_make as vm
    on tv.vehicle_make = vm.vehicle_make

    left join dim_vehicle_model as vmo
    on tv.vehicle_model = vmo.vehicle_model

    left join dim_travel_direction as td
    on tv.travel_direction = td.travel_direction

    left join dim_person_sex as ps
    on tv.person_sex = ps.person_sex

    left join dim_driver_license_status as dls
    on tv.driver_license_status = dls.driver_license_status

    left join dim_driver_license_jurisdiction as dlj
    on tv.driver_license_jurisdiction = dlj.driver_license_jurisdiction

    left join dim_pre_crash as pc
    on tv.pre_crash = pc.pre_crash

    left join dim_point_of_impact as poi
    on tv.point_of_impact = poi.point_of_impact

    left join dim_public_property_damage as ppd
    on tv.public_property_damage = ppd.public_property_damage
    
),

final as(

    select row_number() over(order by unique_id) as table_sk, *,

    '{{invocation_id}}' as jobId,
    current_timestamp() as DI_Create_Date_Name
    
    from (
        temp_table
    ) 
)

select * from final
