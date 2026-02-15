{#
    Schema name generation for Icebreaker adapter.
    
    Uses the profile's schema (e.g. 'dbt_tdoberneck') as the target schema prefix
    combined with the custom schema name from dbt_project.yml.
    
    Result: dbt_tdoberneck + stg_halo -> dbt_tdoberneck_stg_halo
#}
{% macro icebreaker__generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro icebreaker__create_schema(relation) %}
    {%- set schema_name = relation.schema | default(relation.without_identifier().schema, true) -%}
    {%- if schema_name and schema_name | trim -%}
    CREATE SCHEMA IF NOT EXISTS {{ schema_name | trim }}
    {%- endif -%}
{% endmacro %}

{% macro icebreaker__drop_schema(relation) %}
    {%- set schema_name = relation.schema | default(relation.without_identifier().schema, true) -%}
    {%- if schema_name and schema_name | trim -%}
    DROP SCHEMA IF EXISTS {{ schema_name | trim }} CASCADE
    {%- endif -%}
{% endmacro %}

{% macro icebreaker__create_table_as(temporary, relation, sql) %}
    {%- set sql = sql | trim -%}
    
    {# Get routing decision from adapter #}
    {%- set route = config.get('icebreaker_route', 'auto') -%}
    
    {%- if temporary -%}
        CREATE OR REPLACE TEMPORARY TABLE {{ relation }} AS (
            {{ sql }}
        )
    {%- else -%}
        CREATE OR REPLACE TABLE {{ relation }} AS (
            {{ sql }}
        )
    {%- endif -%}
{% endmacro %}

{% macro icebreaker__create_view_as(relation, sql) %}
    CREATE OR REPLACE VIEW {{ relation }} AS (
        {{ sql }}
    )
{% endmacro %}

{% macro icebreaker__rename_relation(from_relation, to_relation) %}
    ALTER TABLE {{ from_relation }} RENAME TO {{ to_relation.identifier }}
{% endmacro %}

{% macro icebreaker__drop_relation(relation) %}
    DROP {{ relation.type }} IF EXISTS {{ relation }}
{% endmacro %}

{% macro icebreaker__truncate_relation(relation) %}
    DELETE FROM {{ relation }}
{% endmacro %}

{% macro icebreaker__list_schemas(database) %}
    SELECT schema_name FROM information_schema.schemata
{% endmacro %}

{% macro icebreaker__list_relations_without_caching(schema_relation) %}
    SELECT
        table_catalog as database,
        table_schema as schema,
        table_name as name,
        CASE table_type
            WHEN 'BASE TABLE' THEN 'table'
            WHEN 'VIEW' THEN 'view'
            ELSE 'table'
        END as type
    FROM information_schema.tables
    WHERE table_schema = '{{ schema_relation.schema }}'
{% endmacro %}

{% macro icebreaker__get_columns_in_relation(relation) %}
    SELECT
        column_name,
        data_type,
        ordinal_position
    FROM information_schema.columns
    WHERE table_schema = '{{ relation.schema }}'
      AND table_name = '{{ relation.identifier }}'
    ORDER BY ordinal_position
{% endmacro %}

{# Cross-database timestamp macros #}
{% macro icebreaker__current_timestamp() %}
    current_timestamp
{% endmacro %}

{% macro icebreaker__current_timestamp_backcompat() %}
    current_timestamp::{{ type_timestamp() }}
{% endmacro %}

{# Utility macro to check current engine #}
{% macro get_engine() %}
    {{ return(adapter.connections._active_engine) }}
{% endmacro %}

{# Cost savings estimation #}
{% macro estimate_savings(results) %}
    {%- set local_count = 0 -%}
    {%- set cloud_count = 0 -%}
    
    {%- for result in results -%}
        {%- if 'Local' in result.message -%}
            {%- set local_count = local_count + 1 -%}
        {%- else -%}
            {%- set cloud_count = cloud_count + 1 -%}
        {%- endif -%}
    {%- endfor -%}
    
    {%- set total = local_count + cloud_count -%}
    {%- set savings_pct = (local_count / total * 100) if total > 0 else 0 -%}
    
    {{ log("Icebreaker Savings: " ~ local_count ~ "/" ~ total ~ " models ran locally (" ~ savings_pct|round(1) ~ "% free)", info=True) }}
{% endmacro %}

{# Override load_csv_rows to use DuckDB-compatible INSERT syntax #}
{% macro icebreaker__load_csv_rows(model, agate_table) %}
    {%- set cols_csv = agate_table.column_names | join(", ") -%}
    {%- set sql -%}
        INSERT INTO {{ this }} ({{ cols_csv }})
        VALUES
        {%- for row in agate_table.rows -%}
            ({%- for value in row -%}
                {%- if value is none -%}
                    NULL
                {%- elif value is number -%}
                    {{ value }}
                {%- else -%}
                    '{{ value | replace("'", "''") }}'
                {%- endif -%}
                {%- if not loop.last -%},{%- endif -%}
            {%- endfor -%})
            {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
    {%- endset -%}
    
    {% do run_query(sql) %}
    {{ return(sql) }}
{% endmacro %}

