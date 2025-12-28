-- models/marts/dim_department.sql
{{ config(materialized='table') }}

with departments as (
    select *
    from {{ ref('stg_department') }}
),

latest as (
    select *
    from departments
    qualify row_number() over (
        partition by department_id
        order by loaded_at desc
    ) = 1
)

select
    department_id,
    department_name,

    -- Add any simple enrichments (you can expand later)
    case
        when lower(department_name) like '%cardio%' then 'Cardiology'
        when lower(department_name) like '%neuro%' then 'Neurology'
        else 'Other'
    end as department_category,

    created_date,
    modified_date,
    loaded_at,

    -- Surrogate key (good practice for star schema)
    md5(department_id) as department_sk

from latest