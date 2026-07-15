{# Port of monthly_test.py `members`: FSC member accounts enriched with country
   and region. Member accounts identified by the membership RecordTypeId; country
   resolved via silver.country_data; region via the regions seed (ISO3 join). #}
{{ config(materialized='incremental', incremental_strategy='append') }}

with members as (
    select
        a."Sub_Chamber__c" as "Sub_Chamber",
        a."Member_Type__c" as "Member_Type",
        a."Website"        as "Website",
        a."Name"           as "Member",
        a."Chamber__c"     as "Chamber",
        a."Country__c"     as country_id
    from {{ ref('account') }} a
    where a."RecordTypeId" = '01240000000HuGwAAK'
),

enriched as (
    select
        m."Sub_Chamber",
        m."Member_Type",
        cd."Name"           as "Country",
        m."Website",
        m."Member",
        m."Chamber",
        cd."Country_code__c" as iso3
    from members m
    join {{ ref('country_data') }} cd on m.country_id = cd."Id"
),

final as (
    select
        e."Sub_Chamber",
        e."Member_Type",
        e."Country",
        e."Website",
        e."Member",
        e."Chamber",
        r."Region",
        r."Functional_region" as "region_fun",
        {{ snap_date() }}      as snapshot_date
    from enriched e
    join {{ ref('regions') }} r on e.iso3 = r."ISO3"
)

select * from final
{{ skip_existing_snapshot() }}
