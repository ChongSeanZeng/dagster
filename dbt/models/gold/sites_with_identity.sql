{# Port of monthly_test.py df_site: number of sites with an identity, per
   country / category / certificate type. Built from the PRE-dedup frame
   (int_certificate_account_country), currently-valid & non-terminated. #}
{{ config(materialized='incremental', incremental_strategy='append') }}

with base as (
    select country, ctype, "Certificate_Type__c"
    from {{ ref('int_certificate_account_country') }}
    where "Cert_Status__c" != 'Terminated'
      and try_cast("Date_To__c" as date) >= current_date
      and country is not null
      and ctype is not null
      and "Certificate_Type__c" is not null
),

final as (
    select
        country,
        ctype,
        "Certificate_Type__c",
        count(*)          as "number of sites with identity",
        {{ snap_date() }} as snapshot_date
    from base
    group by country, ctype, "Certificate_Type__c"
)

select * from final
{{ skip_existing_snapshot() }}
