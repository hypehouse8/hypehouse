{% macro apply_row_access_policy() %}
{%- set source_relation = adapter.get_relation(
      database = this.database,
      schema = this.schema,
      identifier = this.name
      ) -%}

{% set table_exists=source_relation is not none %}

{% if table_exists %}
    {% if execute and not is_incremental() %}
        alter table {{ this }} add row access policy prd.analytics.rap_analyst_allowed_region ON (country_name) 
    {% endif %}
{% endif %}
{% endmacro %}