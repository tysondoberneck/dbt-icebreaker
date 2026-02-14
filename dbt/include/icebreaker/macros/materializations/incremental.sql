{#
    Incremental Materialization for Icebreaker
    
    Supports three incremental strategies:
    - merge: Delete matching rows, then insert new (default)
    - append: Insert only, no deletions
    - delete+insert: Delete by partition, then insert
    
    AUTOMATIC ROUTING - no tags required!
    Uses adapter.get_routing_decision() to determine execution venue.
#}

{% materialization incremental, adapter='icebreaker' %}
    {%- set identifier = model['alias'] -%}
    {%- set target_relation = api.Relation.create(
        database=database, schema=schema, identifier=identifier, type='table'
    ) -%}
    {%- set existing_relation = adapter.get_relation(
        database=database, schema=schema, identifier=identifier
    ) -%}
    
    {# Get incremental config #}
    {%- set unique_key = config.get('unique_key') -%}
    {%- set strategy = config.get('incremental_strategy', 'merge') -%}
    {%- set on_schema_change = config.get('on_schema_change', 'ignore') -%}
    
    {# Parse unique_key if it's a list #}
    {%- if unique_key is sequence and unique_key is not string -%}
        {%- set unique_key_list = unique_key -%}
    {%- elif unique_key is string -%}
        {%- set unique_key_list = [unique_key] -%}
    {%- else -%}
        {%- set unique_key_list = [] -%}
    {%- endif -%}
    
    {# AUTOMATIC ROUTING - no tags required! #}
    {%- set routing = adapter.get_routing_decision(model, sql) -%}
    {%- set venue = routing.venue | lower -%}
    {%- set reason = routing.reason -%}
    
    {# User override still supported (rarely needed) #}
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
    
    {# Switch engine if needed #}
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
    
    {# Determine if this is a full refresh or incremental #}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    
    {% if existing_relation is none or full_refresh_mode %}
        {# ============================================================ #}
        {# FULL REFRESH: Create table from scratch                      #}
        {# ============================================================ #}
        
        {% if full_refresh_mode %}
            {{ log(identifier ~ " -> FULL REFRESH (" ~ reason ~ ")", info=True) }}
        {% else %}
            {{ log(identifier ~ " -> FIRST RUN (" ~ reason ~ ")", info=True) }}
        {% endif %}
        
        {%- set start_time = modules.datetime.datetime.now() -%}
        
        {% call statement('main') %}
            CREATE OR REPLACE TABLE {{ schema }}.{{ identifier }} AS (
                {{ sql }}
            )
        {% endcall %}
        
        {%- set end_time = modules.datetime.datetime.now() -%}
        {%- set duration = (end_time - start_time).total_seconds() -%}
        
    {% else %}
        {# ============================================================ #}
        {# INCREMENTAL: Update existing table                           #}
        {# ============================================================ #}
        
        {% if is_cloud %}
            {{ log(identifier ~ " -> INCREMENTAL (" ~ strategy ~ ", " ~ reason ~ ")", info=True) }}
        {% else %}
            {{ log(identifier ~ " -> INCREMENTAL (" ~ strategy ~ ", " ~ reason ~ ")", info=True) }}
        {% endif %}
        
        {%- set start_time = modules.datetime.datetime.now() -%}
        
        {# Create temp table with new/changed rows #}
        {# Use schema-qualified name to work with MotherDuck attachment #}
        {%- set tmp_identifier = identifier ~ '__ib_tmp' -%}
        {%- set tmp_table = schema ~ '.' ~ tmp_identifier -%}
        
        {# dbt requires 'main' statement to be called - use it for the primary work #}
        {% call statement('main') %}
            CREATE OR REPLACE TABLE {{ tmp_table }} AS (
                {{ sql }}
            )
        {% endcall %}
        
        {% if strategy == 'append' %}
            {# -------------------------------------------------------- #}
            {# APPEND STRATEGY: Just insert, no deletes                 #}
            {# -------------------------------------------------------- #}
            
            {% call statement('incremental_append') %}
                INSERT INTO {{ schema }}.{{ identifier }}
                SELECT * FROM {{ tmp_table }}
            {% endcall %}
            
        {% elif strategy == 'delete+insert' %}
            {# -------------------------------------------------------- #}
            {# DELETE+INSERT STRATEGY: Delete by partition, then insert #}
            {# -------------------------------------------------------- #}
            
            {%- set partition_by = config.get('partition_by') -%}
            
            {% if partition_by %}
                {% call statement('delete_partitions') %}
                    DELETE FROM {{ schema }}.{{ identifier }}
                    WHERE {{ partition_by }} IN (
                        SELECT DISTINCT {{ partition_by }} 
                        FROM {{ tmp_table }}
                    )
                {% endcall %}
            {% elif unique_key_list | length > 0 %}
                {# Fallback: delete by unique key #}
                {% call statement('delete_keys') %}
                    DELETE FROM {{ schema }}.{{ identifier }}
                    WHERE ({{ unique_key_list | join(', ') }}) IN (
                        SELECT {{ unique_key_list | join(', ') }}
                        FROM {{ tmp_table }}
                    )
                {% endcall %}
            {% endif %}
            
            {% call statement('insert_new') %}
                INSERT INTO {{ schema }}.{{ identifier }}
                SELECT * FROM {{ tmp_table }}
            {% endcall %}
            
        {% else %}
            {# -------------------------------------------------------- #}
            {# MERGE STRATEGY (default): Delete matching, then insert   #}
            {# -------------------------------------------------------- #}
            
            {% if unique_key_list | length > 0 %}
                {# Build the join condition #}
                {%- set join_conditions = [] -%}
                {%- for key in unique_key_list -%}
                    {%- do join_conditions.append("target." ~ key ~ " = source." ~ key) -%}
                {%- endfor -%}
                {%- set join_condition = join_conditions | join(" AND ") -%}
                
                {# Delete existing rows that will be updated #}
                {% call statement('delete_existing') %}
                    DELETE FROM {{ schema }}.{{ identifier }} AS target
                    WHERE EXISTS (
                        SELECT 1 FROM {{ tmp_table }} AS source
                        WHERE {{ join_condition }}
                    )
                {% endcall %}
            {% endif %}
            
            {# Insert all rows from temp table #}
            {% call statement('insert_new') %}
                INSERT INTO {{ schema }}.{{ identifier }}
                SELECT * FROM {{ tmp_table }}
            {% endcall %}
            
        {% endif %}
        
        {# Drop temp table #}
        {% call statement('drop_tmp') %}
            DROP TABLE IF EXISTS {{ tmp_table }}
        {% endcall %}
        
        {%- set end_time = modules.datetime.datetime.now() -%}
        {%- set duration = (end_time - start_time).total_seconds() -%}
        
    {% endif %}
    
    {# Log execution for savings tracking #}
    {{ icebreaker_log_execution(identifier, venue, duration) }}
    
    {# Sync to all engines #}
    {{ log("Syncing " ~ identifier ~ " to all engines...", info=True) }}
    {% call statement('sync') %}
        -- ICEBREAKER_SYNC:{{ schema }}.{{ identifier }}
        SELECT 1
    {% endcall %}
    
    {{ run_hooks(post_hooks) }}
    
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
