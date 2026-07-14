{# Use the model's +schema (silver/gold) verbatim instead of dbt's default
   "<target_schema>_<custom_schema>" prefixing, so tables land in lake.silver /
   lake.gold exactly. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
