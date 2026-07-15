{# Port of daily.py df2: certificates + companies (Σ sites) per certificate
   category. daily.py hardcoded 5 RecordTypeIds; here we join silver.recordtype
   and filter on the category name instead. Currently-valid certificates only. #}
{{ config(materialized='incremental', incremental_strategy='append') }}

with cert as (
    select rt."Name" as ctype, c."Number_of_Sites__c"
    from {{ ref('certificate_c') }} c
    join {{ ref('recordtype') }} rt on c."RecordTypeId" = rt."Id"
    where c."Cert_Status__c" in ('Issued','Reissued','Reinstated','Extended')
      and c."CB__c" not in ('ASI','FSC')
      and try_cast(c."Date_To__c" as date) >= current_date
      and rt."Name" in ('CFM','COC','CW/FM','FM','FM/COC')
),

final as (
    select
        ctype                                                     as "Certificate Category",
        count(*)                                                  as "Number Of Certificates",
        cast(sum(try_cast("Number_of_Sites__c" as double)) as bigint) as "Number Of Companies",
        {{ snap_date() }}                                         as snapshot_date
    from cert
    group by ctype
)

select * from final
{{ skip_existing_snapshot() }}
