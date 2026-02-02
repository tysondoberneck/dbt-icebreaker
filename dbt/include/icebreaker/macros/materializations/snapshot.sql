{#
    Snapshot Materialization for Icebreaker
    
    Implements SCD Type 2 (Slowly Changing Dimension) tracking.
    Adds columns:
    - dbt_valid_from: When this version became valid
    - dbt_valid_to: When this version was superseded (NULL if current)
    - dbt_scd_id: Unique ID for this version
    - dbt_updated_at: When this row was last touched
    
    Strategies:
    - timestamp: Compare updated_at column to detect changes
    - check: Compare specified columns to detect changes
    
    AUTOMATIC ROUTING - no tags required!
#}

{% materialization snapshot, adapter='icebreaker' %}
    {%- set identifier = model['alias'] -%}
    {%- set target_relation = api.Relation.create(
        database=database, schema=schema, identifier=identifier, type='table'
    ) -%}
    {%- set existing_relation = adapter.get_relation(
        database=database, schema=schema, identifier=identifier
    ) -%}
    
    {# Get snapshot config #}
    {%- set strategy = config.get('strategy', 'timestamp') -%}
    {%- set unique_key = config.get('unique_key') -%}
    {%- set updated_at = config.get('updated_at') -%}
    {%- set check_cols = config.get('check_cols', []) -%}
    
    {# Parse unique_key if it's a list #}
    {%- if unique_key is sequence and unique_key is not string -%}
        {%- set unique_key_list = unique_key -%}
    {%- elif unique_key is string -%}
        {%- set unique_key_list = [unique_key] -%}
    {%- else -%}
        {%- set unique_key_list = [] -%}
    {%- endif -%}
    
    {# AUTOMATIC ROUTING #}
    {%- set routing = adapter.get_routing_decision(model, sql) -%}
    {%- set venue = routing.venue | lower -%}
    {%- set reason = routing.reason -%}
    
    {# User override #}
    {%- set explicit_route = config.get('icebreaker_route') -%}
    {%- if explicit_route -%}
        {%- set venue = explicit_route | lower -%}
        {%- set reason = "User override: " ~ explicit_route -%}
    {%- endif -%}
    
    {%- set is_cloud = venue in ['cloud', 'motherduck'] -%}
    
    {{ run_hooks(pre_hooks) }}
    
    {# Ensure schema exists #}
    {% call statement('create_schema') %}
        CREATE SCHEMA IF NOT EXISTS {{ schema }}
    {% endcall %}
    
    {# Switch engine #}
    {% if is_cloud %}
        {% call statement('switch_engine') %}
            -- ICEBREAKER_ENGINE:cloud
            SELECT 1
        {% endcall %}
    {% else %}
        {% call statement('switch_engine') %}
            -- ICEBREAKER_ENGINE:local
            SELECT 1
        {% endcall %}
    {% endif %}
    
    {% if existing_relation is none %}
        {# ============================================================ #}
        {# FIRST RUN: Create snapshot table with SCD columns            #}
        {# ============================================================ #}
        
        {% if is_cloud %}
            {{ log("☁️  📸 " ~ identifier ~ " → SNAPSHOT INIT (" ~ reason ~ ")", info=True) }}
        {% else %}
            {{ log("🏠 📸 " ~ identifier ~ " → SNAPSHOT INIT (" ~ reason ~ ")", info=True) }}
        {% endif %}
        
        {%- set start_time = modules.datetime.datetime.now() -%}
        
        {% call statement('main') %}
            CREATE TABLE {{ schema }}.{{ identifier }} AS (
                SELECT 
                    source.*,
                    {% if strategy == 'timestamp' and updated_at %}
                        source.{{ updated_at }} AS dbt_valid_from,
                    {% else %}
                        CURRENT_TIMESTAMP AS dbt_valid_from,
                    {% endif %}
                    CAST(NULL AS TIMESTAMP) AS dbt_valid_to,
                    MD5(
                        CAST({{ unique_key_list | join(" || '-' || ") }} AS VARCHAR) 
                        || '-' || 
                        {% if strategy == 'timestamp' and updated_at %}
                            CAST(source.{{ updated_at }} AS VARCHAR)
                        {% else %}
                            CAST(CURRENT_TIMESTAMP AS VARCHAR)
                        {% endif %}
                    ) AS dbt_scd_id,
                    CURRENT_TIMESTAMP AS dbt_updated_at
                FROM ({{ sql }}) AS source
            )
        {% endcall %}
        
        {%- set end_time = modules.datetime.datetime.now() -%}
        {%- set duration = (end_time - start_time).total_seconds() -%}
        
    {% else %}
        {# ============================================================ #}
        {# INCREMENTAL SNAPSHOT: Detect changes and update history      #}
        {# ============================================================ #}
        
        {% if is_cloud %}
            {{ log("☁️  📸 " ~ identifier ~ " → SNAPSHOT UPDATE (" ~ reason ~ ")", info=True) }}
        {% else %}
            {{ log("🏠 📸 " ~ identifier ~ " → SNAPSHOT UPDATE (" ~ reason ~ ")", info=True) }}
        {% endif %}
        
        {%- set start_time = modules.datetime.datetime.now() -%}
        
        {# Create staging table with current source data + SCD columns #}
        {%- set staging_table = identifier ~ '__ib_snapshot_staging' -%}
        
        {% call statement('create_staging') %}
            CREATE OR REPLACE TEMPORARY TABLE {{ staging_table }} AS (
                SELECT 
                    source.*,
                    {% if strategy == 'timestamp' and updated_at %}
                        source.{{ updated_at }} AS dbt_valid_from,
                    {% else %}
                        CURRENT_TIMESTAMP AS dbt_valid_from,
                    {% endif %}
                    CAST(NULL AS TIMESTAMP) AS dbt_valid_to,
                    MD5(
                        CAST({{ unique_key_list | join(" || '-' || ") }} AS VARCHAR) 
                        || '-' || 
                        {% if strategy == 'timestamp' and updated_at %}
                            CAST(source.{{ updated_at }} AS VARCHAR)
                        {% else %}
                            CAST(CURRENT_TIMESTAMP AS VARCHAR)
                        {% endif %}
                    ) AS dbt_scd_id
                FROM ({{ sql }}) AS source
            )
        {% endcall %}
        
        {# Build join condition for matching records #}
        {%- set join_conditions = [] -%}
        {%- for key in unique_key_list -%}
            {%- do join_conditions.append("target." ~ key ~ " = staging." ~ key) -%}
        {%- endfor -%}
        {%- set join_condition = join_conditions | join(" AND ") -%}
        
        {# Close out changed records (set dbt_valid_to) #}
        {% call statement('close_changed') %}
            UPDATE {{ schema }}.{{ identifier }} AS target
            SET 
                dbt_valid_to = staging.dbt_valid_from,
                dbt_updated_at = CURRENT_TIMESTAMP
            FROM {{ staging_table }} AS staging
            WHERE {{ join_condition }}
              AND target.dbt_valid_to IS NULL
              AND target.dbt_scd_id != staging.dbt_scd_id
        {% endcall %}
        
        {# Insert new versions of changed records + brand new records #}
        {% call statement('insert_new') %}
            INSERT INTO {{ schema }}.{{ identifier }}
            SELECT 
                staging.*,
                CURRENT_TIMESTAMP AS dbt_updated_at
            FROM {{ staging_table }} AS staging
            LEFT JOIN {{ schema }}.{{ identifier }} AS target
              ON {{ join_condition }}
              AND target.dbt_valid_to IS NULL
            WHERE 
                {# New record #}
                target.{{ unique_key_list[0] }} IS NULL
                OR 
                {# Changed record (different scd_id) #}
                target.dbt_scd_id != staging.dbt_scd_id
        {% endcall %}
        
        {# Handle deleted records (optional) #}
        {%- set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) -%}
        
        {% if invalidate_hard_deletes %}
            {% call statement('close_deleted') %}
                UPDATE {{ schema }}.{{ identifier }} AS target
                SET 
                    dbt_valid_to = CURRENT_TIMESTAMP,
                    dbt_updated_at = CURRENT_TIMESTAMP
                WHERE target.dbt_valid_to IS NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM {{ staging_table }} AS staging
                      WHERE {{ join_condition }}
                  )
            {% endcall %}
        {% endif %}
        
        {# Drop staging table #}
        {% call statement('drop_staging') %}
            DROP TABLE IF EXISTS {{ staging_table }}
        {% endcall %}
        
        {%- set end_time = modules.datetime.datetime.now() -%}
        {%- set duration = (end_time - start_time).total_seconds() -%}
        
    {% endif %}
    
    {# Log execution #}
    {{ icebreaker_log_execution(identifier, venue, duration) }}
    
    {# Sync #}
    {{ log("🔄 Syncing " ~ identifier ~ " to all engines...", info=True) }}
    {% call statement('sync') %}
        -- ICEBREAKER_SYNC:{{ schema }}.{{ identifier }}
        SELECT 1
    {% endcall %}
    
    {{ run_hooks(post_hooks) }}
    
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
