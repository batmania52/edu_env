{% macro log_all_model_results() %}
  {% if execute %}
    {% for result in results %}
      {% if result.node.resource_type == 'model' %}
        {% set model_name = result.node.name %}
        {% set dbt_invocation_id = invocation_id %} {# dbt_invocation_id는 전역 변수 #}
        {% set status = result.status %}
                {# completed_at을 KST로 변환 후 문자열로 포맷 #}
        {% set kst_timezone = modules.pytz.timezone('Asia/Seoul') %}
        

        {% set rows_affected = result.adapter_response.rows_affected if result.adapter_response.rows_affected is defined else -1 %}

        {# 데이터베이스에서 start_time을 조회하여 execution_time_seconds 계산 #}
        

        {% set update_sql %}
          UPDATE admin.dbt_log
          SET
            status = '{{ status }}',
            end_time = statement_timestamp() at time zone 'Asia/Seoul',
            execution_time_seconds = EXTRACT(EPOCH FROM (statement_timestamp() at time zone 'Asia/Seoul' - start_time))::numeric,
            rows_affected = {{ rows_affected }}
          WHERE dbt_invocation_id = '{{ dbt_invocation_id }}' AND model_name = '{{ model_name }}';
        {% endset %}

        {% do run_query(update_sql) %}
        {% do log("Updated final status for model: " ~ model_name ~ " with status: " ~ status ~ " for dbt_invocation_id: " ~ dbt_invocation_id, info=True) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}