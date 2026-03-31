{% macro log_model_start(dbt_invocation_id) %}
  {% if execute %}
    {% set model_name = model.name %}
    {% set data_start = var('data_interval_start', none) %}
    {% set data_end = var('data_interval_end', none) %}

    {# If data_start or data_end are None, get the default values from get_date_intervals() #}
    {% if data_start is none or data_end is none %}
      {% set default_intervals = get_date_intervals() %}
      {% set default_start = default_intervals[0] %}
      {% set default_end = default_intervals[1] %}

      {% if data_start is none %}
        {% set data_start = default_start %}
      {% endif %}
      {% if data_end is none %}
        {% set data_end = default_end %}
      {% endif %}
    {% endif %}

    {% set variables_json = {'data_interval_start': data_start, 'data_interval_end': data_end} | tojson %}

    {% set insert_sql %}
      INSERT INTO admin.dbt_log (
        dbt_invocation_id,
        model_name,
        status,
        start_time,
        variables
      ) VALUES (
        '{{ dbt_invocation_id }}',
        '{{ model_name }}',
        'running',
        statement_timestamp() at time zone 'Asia/Seoul',
        
        '{{ variables_json }}'
      );
    {% endset %}
    
    {% do run_query(insert_sql) %}
  {% endif %}
{% endmacro %}
