{{ config(materialized='view') }}

with source as (
    select * from {{ source('appolo_raw', 'department_raw') }}
),

renamed as (
    select
        department_id::varchar as department_id,
        department_name::varchar as department_name,
        try_to_date(created_date, 'DD-MM-YYYY HH:MI') as created_date,
        try_to_date(modified_date, 'DD-MM-YYYY HH:MI') as modified_date,
        loaded_at
    from source
)

select * from renamed