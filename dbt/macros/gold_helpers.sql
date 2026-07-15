{# Shared helpers for the gold snapshot models (ports of daily.py / monthly_test.py). #}

{# The snapshot date stamped on every gold row. Passed by Dagster as a dbt var
   (`--vars 'snapshot_date: 2026-07-14'`); defaults to the dbt run date so a
   bare `dbt run` still works. #}
{% macro snap_date() -%}
cast('{{ var("snapshot_date", run_started_at.strftime("%Y-%m-%d")) }}' as date)
{%- endmacro %}

{# Idempotency guard for append-snapshot models: on an incremental run, drop the
   whole batch if this snapshot_date was already loaded (re-run safe). Place at
   the very end of a model whose final projection includes a `snapshot_date`. #}
{% macro skip_existing_snapshot() %}
{% if is_incremental() %}
where snapshot_date not in (select distinct snapshot_date from {{ this }})
{% endif %}
{% endmacro %}

{# Account types that identify a certificate-holding legal entity / site
   (monthly_test.ACCOUNT_TYPES). Note the en-dash in "Group Member – Legal Entity". #}
{% macro account_types() -%}
'COC - Single Certificate - Legal Entity', 'COC - Site - Legal Entity',
'COC - Multisite Manager - Legal Entity', 'COC - Site',
'COC - Group Member – Legal Entity', 'FM - Group Member',
'FM - Single Certificate', 'FM - Group Manager',
'COC - Group Manager - Legal Entity', 'Project Certificate',
'COC - Site Legal Entity'
{%- endmacro %}

{# The "certificate holder" account types preferred on dedup
   (monthly_test.CERT_HOLDER_TYPES). #}
{% macro cert_holder_types() -%}
'COC - Single Certificate - Legal Entity',
'COC - Multisite Manager - Legal Entity',
'COC - Group Manager - Legal Entity',
'FM - Group Manager', 'FM - Single Certificate'
{%- endmacro %}
