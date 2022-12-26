with temp_persons as(

    select unique_id, collision_id, person_id,

    person_type,
    person_injury,
    vehicle_id,
    
    (case
        when person_age <= 0 then -99
        when person_age > 120 then -99
        when person_age is null then -99
        else person_age
    end) as person_age,

    (case
        when ejection is null then 'Unknown'
        else ejection
    end) as ejection,

    (case
        when emotional_status is null then 'Unknown'
        else emotional_status
    end) as emotional_status,

    (case
        when bodily_injury is null then 'Unknown'
        else bodily_injury
    end) as bodily_injury,
    
    (case
        when position_in_vehicle is null then 'Unknown'
        else position_in_vehicle
    end) as position_in_vehicle,

    (case
        when safety_equipment is null or safety_equipment = '-' then 'Unknown'
        else safety_equipment
    end) as safety_equipment,

    (case
        when ped_location is null then 'Unknown'
        else ped_location
    end) as ped_location,

    (case
        when ped_action is null then 'Unknown'
        else ped_action
    end) as ped_action,

    (case
        when complaint is null then 'Unknown'
        else complaint
    end) as complaint,

    (case
        when ped_role is null then 'Unknown'
        else ped_role
    end) as ped_role,

    (case
        when person_sex is null then 'U'
        else person_sex
    end) as person_sex
    
    from {{ ref('stg_nyc_mv_collision_persons') }}
),


dim_ejection as (
    select * from {{ ref('dim_ejection') }}
),

dim_emotional_status as (
    select * from {{ ref('dim_emotional_status') }}
),

dim_bodily_injury as (
    select * from {{ ref('dim_bodily_injury') }}
),

dim_position_in_vehicle as (
    select * from {{ ref('dim_position_in_vehicle') }}
),

dim_safety_equipment as (
    select * from {{ ref('dim_safety_equipment') }}
),

dim_ped_location as (
    select * from {{ ref('dim_ped_location') }}
),

dim_complaint as (
    select * from {{ ref('dim_complaint') }}
),

dim_ped_action as (
    select * from {{ ref('dim_ped_action') }}
),

dim_ped_role as (
    select * from {{ ref('dim_ped_role') }}
),

dim_person_sex as (
    select * from {{ ref('dim_person_sex') }}
),

temp_table as(

    select tp.unique_id, tp.collision_id, tp.vehicle_id, de.ejection_sk, es.emotional_status_sk, bi.bodily_injury_sk, piv.position_in_vehicle_sk,
    se.safety_equipment_sk, pl.ped_location_sk, pa.ped_action_sk, c.complaint_sk, pr.ped_role_sk, ps.person_sex_sk

    from temp_persons tp

    left join dim_ejection de
    on tp.ejection = de.ejection

    left join dim_emotional_status es
    on tp.emotional_status = es.emotional_status

    left join dim_bodily_injury bi
    on tp.bodily_injury = bi.bodily_injury

    left join dim_position_in_vehicle piv
    on tp.position_in_vehicle = piv.position_in_vehicle

    left join dim_safety_equipment se
    on tp.safety_equipment = se.safety_equipment

    left join dim_ped_location pl
    on tp.ped_location = pl.ped_location

    left join dim_ped_action pa
    on tp.ped_action = pa.ped_action

    left join dim_complaint c
    on tp.complaint = c.complaint

    left join dim_ped_role pr
    on tp.ped_role = pr.ped_role

    left join dim_person_sex ps
    on tp.person_sex = ps.person_sex

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
order by unique_id
