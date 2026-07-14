{# Port of monthly_test.py `FM`: Forest-Management certificate count and total
   certified forest area per country. From the deduped frame, currently-valid &
   active, categories FM/COC, FM, CFM. Reproduces the legacy two-step aggregation:
   first collapse by distinct forest-area value (counting certificates), then sum
   the distinct areas and the counts per country. #}
{{ config(materialized='incremental', incremental_strategy='append') }}

with base as (
    select country, forest_area_total as area
    from {{ ref('int_certificate_dedup') }}
    where try_cast("Date_To__c" as date) >= current_date
      and "Cert_Status__c" not in ('Suspended','Terminated')
      and ctype in ('FM/COC','FM','CFM')
      and country is not null
      and forest_area_total is not null
),

step1 as (
    select country, area, count(*) as cnt
    from base
    group by country, area
),

final as (
    select
        country,
        sum(area)         as forest_area,
        sum(cnt)          as certificates,
        {{ snap_date() }} as snapshot_date
    from step1
    group by country
)

select * from final
{{ skip_existing_snapshot() }}
