{#
    Icebreaker Savings Tracking Macro
    
    Called from materializations to log execution and calculate savings.
    Uses accurate cloud pricing models based on actual provider rates.
#}

{% macro icebreaker_log_execution(model_name, engine, duration_seconds, row_count=0) %}
    {#
    Log an execution for savings tracking.
    
    Args:
        model_name: Name of the model
        engine: "local" or "cloud"  
        duration_seconds: How long the execution took
        row_count: Number of rows processed (for better cost estimation)
    #}
    
    {% if execute %}
        {% set cloud_type = var('icebreaker_cloud_type', 'motherduck') %}
        
        {# Calculate savings based on cloud provider #}
        {% if engine == 'local' %}
            {% if cloud_type == 'snowflake' %}
                {# Snowflake: 60-sec minimum, XS warehouse (1 credit/hour), $2/credit standard #}
                {% set billable_seconds = [60, duration_seconds] | max %}
                {% set hours = billable_seconds / 3600.0 %}
                {% set credits = hours * 1 %}  {# X-Small = 1 credit/hour #}
                {% set savings = credits * 2.0 %}  {# $2 per credit (standard edition) #}
            {% elif cloud_type == 'motherduck' %}
                {# MotherDuck Pulse: 1-sec minimum, $0.40/CU-hour #}
                {% set billable_seconds = [1, duration_seconds] | max %}
                {% set hours = billable_seconds / 3600.0 %}
                {% set savings = hours * 0.40 %}  {# $0.40 per CU-hour #}
            {% elif cloud_type == 'bigquery' %}
                {# BigQuery: estimate bytes from row count (500 bytes/row avg), $5/TB #}
                {% set bytes_estimate = row_count * 500 if row_count > 0 else duration_seconds * 50 * 1024 * 1024 %}
                {% set tb_scanned = bytes_estimate / (1024 * 1024 * 1024 * 1024) %}
                {% set savings = tb_scanned * 5.0 %}
            {% else %}
                {# Default to MotherDuck Pulse pricing #}
                {% set billable_seconds = [1, duration_seconds] | max %}
                {% set hours = billable_seconds / 3600.0 %}
                {% set savings = hours * 0.40 %}
            {% endif %}
            
            {# Display savings with appropriate precision #}
            {% if savings >= 0.01 %}
                {{ log("Saved ~$" ~ (savings | round(2)) ~ " by running locally", info=True) }}
            {% elif savings >= 0.001 %}
                {{ log("Saved ~$" ~ (savings | round(3)) ~ " by running locally", info=True) }}
            {% elif savings > 0 %}
                {{ log("Saved ~$" ~ (savings | round(4)) ~ " by running locally", info=True) }}
            {% endif %}
        {% else %}
            {% set savings = 0 %}
        {% endif %}
        
        {# Log to savings database via SQL comment that connection manager intercepts #}
        {% call statement('log_savings', fetch_result=False) %}
            -- ICEBREAKER_LOG_SAVINGS:{{ model_name }}:{{ engine }}:{{ duration_seconds }}:{{ savings }}:{{ cloud_type }}:{{ row_count }}
            SELECT 1
        {% endcall %}
    {% endif %}
{% endmacro %}

