{{ config(materialized='view') }}  -- Start as view for quick testing; change to 'table' later

with source as (
    select * from {{ source('appolo_raw', 'appointment_raw') }}
),

renamed as (
    select
        appointment_id::varchar as appointment_id,  -- Cast if needed
        patient_id::varchar as patient_id,
        doctor_id::varchar as doctor_id,
        department_id::varchar as department_id,
        prescription_id::varchar as prescription_id,
        try_to_date(appointment_date, 'DD-MM-YYYY HH:MI') as appointment_date,  -- Parse date; adjust format
        try_to_number(amount_billed) as amount_billed,  -- Cast to number
        try_to_number(discount) as discount,
        try_to_number(final_amount) as final_amount,
        try_to_date(created_date, 'DD-MM-YYYY HH:MI') as created_date,
        try_to_date(modified_date, 'DD-MM-YYYY HH:MI') as modified_date,
        loaded_at
    from source
    where appointment_id != 'AppointmentID'  -- Extra safety if headers sneak in
)

select * from renamed

-- with source as (
--     select * from {{ source('appolo_raw', 'appointment_raw') }}
-- ),

-- valid_patients as (
--     select patient_id
--     from {{ source('appolo_raw', 'patient_raw') }}
-- ),

-- renamed as (
--     select
--         appointment_id::varchar as appointment_id,
--         patient_id::varchar as patient_id,
--         doctor_id::varchar as doctor_id,
--         department_id::varchar as department_id,
--         prescription_id::varchar as prescription_id,
--         try_to_date(appointment_date, 'DD-MM-YYYY HH:MI') as appointment_date,
--         try_to_number(amount_billed) as amount_billed,
--         try_to_number(discount) as discount,
--         try_to_number(final_amount) as final_amount,
--         try_to_date(created_date, 'DD-MM-YYYY HH:MI') as created_date,
--         try_to_date(modified_date, 'DD-MM-YYYY HH:MI') as modified_date,
--         loaded_at
--     from source
--     where appointment_id != 'AppointmentID'
--       and patient_id in (select patient_id from valid_patients)
-- )

-- select * from renamed;
