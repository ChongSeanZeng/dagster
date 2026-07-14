{# Reproduces monthly_test.py's `df` BEFORE the holder-preference dedup:
   one row per (account, certificate), certificate joined to its human-readable
   category via RecordType and to its holder account + country.
   Ephemeral — inlined into the gold models that consume it. #}
{{ config(materialized='ephemeral') }}

with acct as (
    select
        a."Certificate_Relation__c" as cert_rel,
        a."Name"                    as company,
        a."Type"                    as acct_type,
        cd."Name"                   as country
    from {{ ref('account') }} a
    join {{ ref('country_data') }} cd on a."Country__c" = cd."Id"
    where a."Type" in ({{ account_types() }})
),

cert as (
    select
        c."Id" as cert_rel,
        rt."Name" as ctype,
        c."Cert_Status__c",
        c."CB__c",
        c."Certificate_Type__c",
        c."Reason_for_Termination__c",
        c."Date_From__c",
        c."Date_To__c",
        c."First_Issue_Date__c",
        try_cast(c."Forest_Area_Total__c" as double)                 as forest_area_total,
        try_cast(c."Boreal_Forests__c" as double)                    as boreal_forests,
        try_cast(c."Community_Forestry__c" as double)                as community_forestry,
        try_cast(c."Plantations__c" as double)                       as plantations,
        try_cast(c."Temperate_Forests__c" as double)                 as temperate_forests,
        try_cast(c."Tropical_Forests__c" as double)                  as tropical_forests,
        try_cast(c."Management_for_NTFP_and_Services__c" as double)  as ntfp_services
    from {{ ref('certificate_c') }} c
    join {{ ref('recordtype') }} rt on c."RecordTypeId" = rt."Id"
    where c."Cert_Status__c" in ('Issued','Reissued','Reinstated','Extended','Suspended','Terminated')
      and c."CB__c" not in ('ASI','FSC')
      and rt."Name" in ('CFM','COC','CW/FM','FM','FM/COC')
)

select
    cert.*,
    acct.company,
    acct.acct_type,
    acct.country
from cert
join acct on cert.cert_rel = acct.cert_rel
