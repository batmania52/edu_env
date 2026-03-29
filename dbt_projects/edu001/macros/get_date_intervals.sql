{% macro get_date_intervals() %}
  {# 1. 파라미터 가져오기 (기본값 설정) #}
  {% set run_mode = var('run_mode', 'schedule') %}
  {% set start_date_var = var('data_interval_start', none) %}
  {% set end_date_var = var('data_interval_end', none) %}

  {# 디버깅 로그: dbt 실행 로그에서 확인 가능 #}
  {% do log("DEBUG: Received run_mode = " ~ run_mode, info=True) %}
  {% do log("DEBUG: Received start_date_var = " ~ start_date_var, info=True) %}

  {# 현재 시각 기준 (fallback용) #}
  {% set current_run_date = modules.datetime.datetime.strptime(run_started_at.strftime('%Y-%m-%d'), '%Y-%m-%d') %}

  {% set start_dt = none %}
  {% set end_dt = none %}

  {# 문자열 -> datetime 객체 변환 #}
  {% if start_date_var and start_date_var != "" %}
    {% set start_dt = modules.datetime.datetime.strptime(start_date_var, '%Y-%m-%d %H:%M:%S') %}
  {% else %}
    {% set start_dt = current_run_date %}
  {% endif %}

  {% if end_date_var and end_date_var != "" %}
    {% set end_dt = modules.datetime.datetime.strptime(end_date_var, '%Y-%m-%d %H:%M:%S') %}
  {% else %}
    {% set end_dt = current_run_date %}
  {% endif %}

  {# 2. 날짜 가공 로직 (문자열 비교 시 공백 제거) #}
  {% if run_mode|trim == 'schedule' %}
    {% do log("DEBUG: Applying schedule logic (-3d, -1d)", info=True) %}
    {% set start_dt = (start_dt - modules.datetime.timedelta(days=3)) %}
    {% set end_dt = (end_dt - modules.datetime.timedelta(days=1)) %}
  {% else %}
    {% do log("DEBUG: Applying manual logic (no change)", info=True) %}
  {% endif %}

  {% set final_start = start_dt.strftime('%Y-%m-%d %H:%M:%S') %}
  {% set final_end = end_dt.strftime('%Y-%m-%d %H:%M:%S') %}

  {% do log("DEBUG: Final Intervals -> " ~ final_start ~ " to " ~ final_end, info=True) %}

  {{ return([final_start, final_end]) }}
{% endmacro %}