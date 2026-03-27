{#-
  [Model Information]
  - Name: stg_purchase_orders
  - Developer: Gemini Agent
  - Created At: 2026-03-23
  - Description: 발주 데이터의 스테이징 모델
  
  [Update History]
  - 2026-03-23: 최초 생성 (Gemini Agent)
  - 2026-03-27: 증분 조건을 between으로 변경 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select purchase_order_id
     , customer_id
     , order_date
     , total_amount
     , status
     , current_timestamp::timestamp as dbt_dtm
  from {{ source('edu', 'purchase_orders') }}
 where order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
