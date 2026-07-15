{# Port of daily.py fm: certificates + certified forest area per forest type,
   with a "Total" row. daily.py's 3 forest RecordTypeIds map to categories
   CFM / FM / FM/COC. Currently-valid certificates only. #}
{{ config(materialized='incremental', incremental_strategy='append') }}

with cert as (
    select c."Forest_Type__c" as forest_type, c."Forest_Area_Total__c"
    from {{ ref('certificate_c') }} c
    join {{ ref('recordtype') }} rt on c."RecordTypeId" = rt."Id"
    where c."Cert_Status__c" in ('Issued','Reissued','Reinstated','Extended')
      and c."CB__c" not in ('ASI','FSC')
      and try_cast(c."Date_To__c" as date) >= current_date
      and rt."Name" in ('CFM','FM','FM/COC')
),

per_type as (
    select
        forest_type                                                 as "Forest Type",
        count(*)                                                    as "Number Of Certificates",
        cast(sum(try_cast("Forest_Area_Total__c" as double)) as bigint) as "Certified Forest Area"
    from cert
    group by forest_type
),

with_total as (
    select * from per_type
    union all
    select 'Total', sum("Number Of Certificates"), sum("Certified Forest Area") from per_type
),

final as (
    select *, {{ snap_date() }} as snapshot_date from with_total
)

select * from final
{{ skip_existing_snapshot() }}
