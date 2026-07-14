{# Port of monthly_test.py `Baseline`: certificate count and summed forest metrics
   per category / country. From the deduped frame, currently-valid & active. #}
{{ config(materialized='incremental', incremental_strategy='append') }}

with base as (
    select *
    from {{ ref('int_certificate_dedup') }}
    where try_cast("Date_To__c" as date) >= current_date
      and "Cert_Status__c" not in ('Suspended','Terminated')
      and ctype is not null
      and country is not null
),

final as (
    select
        ctype,
        country,
        count(*)                    as certificates,
        sum(boreal_forests)         as boreal_forests,
        sum(community_forestry)     as community_forestry,
        sum(forest_area_total)      as forest_area_total,
        sum(ntfp_services)          as management_for_ntfp_and_services,
        sum(plantations)            as plantations,
        sum(temperate_forests)      as temperate_forests,
        sum(tropical_forests)       as tropical_forests,
        {{ snap_date() }}           as snapshot_date
    from base
    group by ctype, country
)

select * from final
{{ skip_existing_snapshot() }}
