{#
    View Materialization for Icebreaker
#}

{% materialization view, adapter='icebreaker' %}
    {%- set identifier = model['alias'] -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='view') -%}
    
    {{ run_hooks(pre_hooks) }}
    
    {% if old_relation is not none %}
        {{ adapter.drop_relation(old_relation) }}
    {% endif %}
    
    {% call statement('main') %}
        {{ icebreaker__create_view_as(target_relation, sql) }}
    {% endcall %}
    
    {{ run_hooks(post_hooks) }}
    
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
