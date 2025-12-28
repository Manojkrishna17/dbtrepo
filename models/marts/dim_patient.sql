-- models/marts/dim_patient.sql
{{ config(materialized='table') }}

with patients as (
    select *
    from {{ ref('stg_patient') }}
),

latest_patient as (
    select *
    from patients
    qualify row_number() over (
        partition by patient_id 
        order by loaded_at desc
    ) = 1   -- get the most recent version if there are duplicates over time
),

enriched as (
    select
        patient_id,
        full_name,
        age,
        gender,
        contact_number,
        address,

        -- Add simple business-friendly columns
        case 
            when age < 18 then 'Child'
            when age between 18 and 64 then 'Adult'
            when age >= 65 then 'Senior'
            else 'Unknown'
        end as age_group,

        case 
            when gender in ('Male', 'MALE') then 'M'
            when gender in ('Female', 'FEMALE') then 'F'
            else 'O'  -- Other/Unknown
        end as gender_code,

        created_date,
        modified_date,
        loaded_at,

        -- Surrogate key (optional but recommended for star schema)
        md5(patient_id) as patient_sk

    from latest_patient
)

select * from enriched