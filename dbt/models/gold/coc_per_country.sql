{# Port of monthly_test.py `coc`: Chain-of-Custody certificate count per country.
   From the holder-preference deduped frame, currently-valid & active
   (not Suspended/Terminated), category COC. #}
{{ config(materialized='incremental', incremental_strategy='append') }}

with base as (
    select country
    from {{ ref('int_certificate_dedup') }}
    where try_cast("Date_To__c" as date) >= current_date
      and "Cert_Status__c" not in ('Suspended','Terminated')
      and ctype = 'COC'
      and country is not null
),

final as (
    select
        country,
        count(*)          as certificates,
        {{ snap_date() }} as snapshot_date
    from base
    group by country
)

select * from final
{{ skip_existing_snapshot() }}
