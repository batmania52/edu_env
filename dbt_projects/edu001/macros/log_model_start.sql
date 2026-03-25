{% macro log_model_start(dbt_invocation_id) %}
  {% if execute %}
    {% set model_name = model.name %}
    {% set intervals = get_date_intervals() %}
    {% set data_start = intervals[0] %}
    {% set data_end = intervals[1] %}

    {% set variables_json = [
        {'key':'run_mode', 'value': var('run_mode', 'schedule')},
        {'key':'data_interval_start', 'value': data_start}, 
        {'key':'data_interval_end', 'value': data_end}        
    ] | tojson %}

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
