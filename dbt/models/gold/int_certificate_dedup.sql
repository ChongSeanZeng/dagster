{# Reproduces monthly_test.py's holder-preference dedup: one row per certificate
   relation, keeping a holder-type account row when one exists (concat(df_ch, df)
   + unique(keep="first")). Ephemeral. #}
{{ config(materialized='ephemeral') }}

select * exclude (_rn)
from (
    select
        *,
        row_number() over (
            partition by cert_rel
            order by case when acct_type in ({{ cert_holder_types() }}) then 0 else 1 end
        ) as _rn
    from {{ ref('int_certificate_account_country') }}
)
where _rn = 1
