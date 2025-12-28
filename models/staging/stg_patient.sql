{{ config(materialized='view') }}

with source as (
    select * from {{ source('appolo_raw', 'patient_raw') }}
),

renamed as (
    select
        patient_id::varchar as patient_id,
        fullname::varchar as full_name,  -- Rename for snake_case consistency
        try_to_number(age) as age,  -- Cast to int; handles bad data
        upper(gender)::varchar as gender,  -- Standardize to uppercase
        contactnumber::varchar as contact_number,  -- Rename
        address::varchar as address,
        try_to_date(created_date, 'DD-MM-YYYY HH:MI') as created_date,
        try_to_date(modified_date, 'DD-MM-YYYY HH:MI') as modified_date,
        loaded_at
    from source
    where patient_id != 'PatientID'  -- Skip any lingering headers
)

select * from renamed