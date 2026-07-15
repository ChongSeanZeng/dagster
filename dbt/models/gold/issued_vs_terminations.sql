{# Port of monthly_test.py `new` + `termi` (IssuedVsTerminations): new issues and
   terminations since 2019, per country / date / category / type. Built from the
   PRE-dedup frame. Unified into one table with a `kind` discriminator; `status`
   holds Cert_Status__c for new issues and Reason_for_Termination__c for
   terminations (matching the legacy column re-use). #}
{{ config(materialized='incremental', incremental_strategy='append') }}

with src as (
    select country, ctype, "Certificate_Type__c", "Cert_Status__c",
           "Reason_for_Termination__c", "Date_From__c", "First_Issue_Date__c"
    from {{ ref('int_certificate_account_country') }}
),

new_issues as (
    select
        'new'                     as kind,
        count(*)                  as certificates,
        country,
        try_cast("First_Issue_Date__c" as date) as event_date,
        ctype,
        "Cert_Status__c"          as status,
        "Certificate_Type__c"
    from src
    where try_cast("First_Issue_Date__c" as date) > date '2019-01-01'
      and "Cert_Status__c" != 'Terminated'
      and country is not null and "First_Issue_Date__c" is not null
      and ctype is not null and "Cert_Status__c" is not null
      and "Certificate_Type__c" is not null
    group by country, try_cast("First_Issue_Date__c" as date), ctype,
             "Cert_Status__c", "Certificate_Type__c"
),

terminations as (
    select
        'terminated'              as kind,
        count(*)                  as certificates,
        country,
        try_cast("Date_From__c" as date) as event_date,
        ctype,
        "Reason_for_Termination__c" as status,
        "Certificate_Type__c"
    from src
    where try_cast("Date_From__c" as date) > date '2019-01-01'
      and "Cert_Status__c" = 'Terminated'
      and country is not null and "Date_From__c" is not null
      and ctype is not null and "Reason_for_Termination__c" is not null
      and "Certificate_Type__c" is not null
    group by country, try_cast("Date_From__c" as date), ctype,
             "Reason_for_Termination__c", "Certificate_Type__c"
),

final as (
    select *, {{ snap_date() }} as snapshot_date
    from (select * from new_issues union all select * from terminations)
)

select * from final
{{ skip_existing_snapshot() }}
