{# Generic silver transform shared by every entity:
   - dedupe to the latest row per primary key (by the incremental cursor)
   - optionally drop soft-deleted rows (Salesforce IsDeleted; Dataverse has none)
   - optionally drop hard-deleted rows via an anti-join on a live-ids snapshot
     (<table>__live_ids, refreshed by the manual "id 拉齐" reconcile)
   - project only the specified business columns ("指定列"); when no column list
     is given, keep everything except dlt system columns
   Original source column names are preserved.

   Params:
     source_name     : dbt source (always 'bronze')
     table_name      : bronze table
     primary_key     : pk column (Id / accountid)
     cursor_field    : incremental cursor used to pick the latest row
     has_soft_delete : filter out IsDeleted rows (Salesforce)
     columns         : list of business columns to publish (None -> keep all)
     live_ids_table  : bronze live-ids table for hard-delete anti-join (None -> skip)
#}
{% macro silver_dedupe(source_name, table_name, primary_key, cursor_field,
                       has_soft_delete=true, columns=none, live_ids_table=none) %}
{# Only filter IsDeleted if the source actually has that column. Some Salesforce
   objects (e.g. RecordType) have no IsDeleted field, so has_soft_delete=true must
   not hard-require it. Checked at run time against the real bronze schema. #}
{%- set has_isdeleted = false -%}
{%- if has_soft_delete and execute -%}
    {%- set _cols = adapter.get_columns_in_relation(source(source_name, table_name))
                    | map(attribute='name') | map('lower') | list -%}
    {%- set has_isdeleted = 'isdeleted' in _cols -%}
{%- endif -%}
with ranked as (
    select
        *,
        row_number() over (
            partition by {{ adapter.quote(primary_key) }}
            order by {{ adapter.quote(cursor_field) }} desc
        ) as _rn
    from {{ source(source_name, table_name) }}
    {# IsDeleted may land as a boolean or as a "true"/"false" string depending on
       how the Salesforce Bulk API / dlt typed it — normalize both. #}
    {% if has_isdeleted %}where not coalesce(try_cast({{ adapter.quote('IsDeleted') }} as boolean), false){% endif %}
),
deduped as (
    select *
    from ranked
    where _rn = 1
    {%- set live_rel = none -%}
    {%- if live_ids_table is not none -%}
        {%- set live_src = source(source_name, live_ids_table) -%}
        {# skip the anti-join until the live-ids snapshot has been created by the
           manual "id 拉齐" reconcile — otherwise a fresh setup would fail. #}
        {%- if execute -%}
            {%- set live_rel = adapter.get_relation(
                database=live_src.database, schema=live_src.schema,
                identifier=live_src.identifier) -%}
        {%- endif -%}
    {%- endif -%}
    {% if live_rel is not none %}
    -- hard-delete reconciliation: keep only ids still present in the source
    and {{ adapter.quote(primary_key) }} in (
        select {{ adapter.quote(primary_key) }} from {{ live_src }}
    )
    {% endif %}
)
{% if columns is not none %}
select
    {% for col in columns %}{{ adapter.quote(col) }}{{ "," if not loop.last }}
    {% endfor %}
from deduped
{% else %}
select * exclude (_rn, {{ adapter.quote('_dlt_load_id') }}, {{ adapter.quote('_dlt_id') }})
from deduped
{% endif %}
{% endmacro %}
