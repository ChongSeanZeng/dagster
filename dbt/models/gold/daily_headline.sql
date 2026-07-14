{# Port of daily.py headline figures: promotional (non-certificate) licenses with
   an active contract, member accounts, and distinct member countries. One row
   per snapshot. Member accounts identified by the membership RecordTypeId. #}
{{ config(materialized='incremental', incremental_strategy='append') }}

with final as (
    select
        (
            select count(*) from {{ ref('non_certificate_holder') }}
            where try_cast("Contract_end_date__c" as date) > current_date
        ) as "Promotional license",
        (
            select count(*) from {{ ref('account') }}
            where "RecordTypeId" = '01240000000HuGwAAK'
        ) as "Members",
        (
            select count(distinct "Country__c") from {{ ref('account') }}
            where "RecordTypeId" = '01240000000HuGwAAK'
        ) as "Member Countries",
        {{ snap_date() }} as snapshot_date
)

select * from final
{{ skip_existing_snapshot() }}
