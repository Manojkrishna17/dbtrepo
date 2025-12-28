-- models/marts/fact_appointment.sql
{{ config(materialized='table') }}

with appointments as (
    select *
    from {{ ref('stg_appointment') }}
),

enriched as (
    select
        a.appointment_id,
        
        -- Foreign keys to dimensions
        a.patient_id,
        p.patient_sk,
        a.department_id,
        d.department_sk,

        -- Original keys (keep for reference)
        a.doctor_id,
        a.prescription_id,

        -- Dates & time intelligence
        a.appointment_date,
        date_part('year', a.appointment_date) as appointment_year,
        date_part('month', a.appointment_date) as appointment_month,
        date_part('quarter', a.appointment_date) as appointment_quarter,
        date_part('dayofweek', a.appointment_date) as appointment_day_of_week,

        -- Amounts & measures
        coalesce(a.amount_billed, 0) as billed_amount,
        coalesce(a.discount, 0) as discount_amount,
        coalesce(a.final_amount, 0) as final_amount,

        -- Simple calculated flag
        case 
            when a.final_amount = 0 then 'Free/Comp'
            when a.discount > 0 then 'Discounted'
            else 'Full Price'
        end as payment_type,

        a.created_date,
        a.modified_date,
        a.loaded_at

    from appointments a

    left join {{ ref('dim_patient') }} p
        on a.patient_id = p.patient_id

    left join {{ ref('dim_department') }} d
        on a.department_id = d.department_id
)

select * from enriched