{% macro get_date_intervals() %}
  {% set start_date_var = var('data_interval_start', none) %}
  {% set end_date_var = var('data_interval_end', none) %}

  {% set current_run_date = modules.datetime.datetime.strptime(run_started_at.strftime('%Y-%m-%d'), '%Y-%m-%d') %}

  {% set start_datetime = none %}
  {% set end_datetime = none %}

  {# Parse start_date_var #}
  {% if start_date_var is none %}
    {% set start_datetime = (current_run_date - modules.datetime.timedelta(days=4)) %}
  {% else %}
    {# auditor.py always passes YYYY-MM-DD HH:MM:SS format #}
    {% set start_datetime = modules.datetime.datetime.strptime(start_date_var, '%Y-%m-%d %H:%M:%S') %}
  {% endif %}

  {# Parse end_date_var #}
  {% if end_date_var is none %}
    {# Default end_date is the start of the current day (run_started_at's day) #}
    {% set end_datetime = current_run_date %}
  {% else %}
    {# auditor.py always passes YYYY-MM-DD HH:MM:SS format, and it's already the exclusive end #}
    {% set end_datetime = modules.datetime.datetime.strptime(end_date_var, '%Y-%m-%d %H:%M:%S') %}
  {% endif %}

  {{ return([start_datetime.strftime('%Y-%m-%d %H:%M:%S'), end_datetime.strftime('%Y-%m-%d %H:%M:%S')]) }}
{% endmacro %}