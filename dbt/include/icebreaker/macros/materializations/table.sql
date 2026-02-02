{#
    Table Materialization for Icebreaker
    
    Architecture:
    - If MotherDuck available: cloud primary + local_db attached for sync
    - If local-only: just create table locally (no sync needed)
    
    Routing: AUTOMATIC based on SQL analysis (no tags required!)
    - External data sources → CLOUD
    - Cloud-only functions → CLOUD
    - Large data volumes → CLOUD
    - Everything else → LOCAL (free compute)
#}

{% materialization table, adapter='icebreaker' %}
    {%- set identifier = model['alias'] -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}
    
    {# 
    AUTOMATIC ROUTING - No tags required!
    The adapter analyzes the SQL to determine optimal execution venue.
    User can still override with icebreaker_route config if needed.
    #}
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
    
    {# Log routing decision #}
    {% if is_cloud %}
        {{ log("☁️  " ~ identifier ~ " → CLOUD (" ~ reason ~ ")", info=True) }}
    {% else %}
        {{ log("🏠 " ~ identifier ~ " → LOCAL (" ~ reason ~ ")", info=True) }}
    {% endif %}
    
    {{ run_hooks(pre_hooks) }}
    
    {# Ensure schema exists #}
    {% call statement('create_schema') %}
        CREATE SCHEMA IF NOT EXISTS {{ schema }}
    {% endcall %}
    
    {% if old_relation is not none %}
        {{ adapter.drop_relation(old_relation) }}
    {% endif %}
    
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
    
    {# Create the table and track time #}
    {%- set start_time = modules.datetime.datetime.now() -%}
    
    {% call statement('main') %}
        CREATE OR REPLACE TABLE {{ schema }}.{{ identifier }} AS (
            {{ sql }}
        )
    {% endcall %}
    
    {%- set end_time = modules.datetime.datetime.now() -%}
    {%- set duration = (end_time - start_time).total_seconds() -%}
    
    {# Log execution for savings tracking #}
    {{ icebreaker_log_execution(identifier, venue, duration) }}
    
    {# 
    SYNC to all engines: Use -- ICEBREAKER_SYNC comment to trigger sync.
    The connection manager handles retry and verification.
    #}
    {{ log("🔄 Syncing " ~ identifier ~ " to all engines...", info=True) }}
    {% call statement('sync') %}
        -- ICEBREAKER_SYNC:{{ schema }}.{{ identifier }}
        SELECT 1
    {% endcall %}
    
    {{ run_hooks(post_hooks) }}
    
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
