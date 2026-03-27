{#-
  [Model Information]
  - Name: stg_purchase_orders2
  - Developer: hjpark
  - Created At: 2026-03-27
  - Description: before_sql 다중 구문 테스트용 모델.
                 before_sql에 DELETE + CREATE TEMP TABLE 두 구문을 포함하며,
                 메인 SQL은 temp 테이블을 소스로 사용.

  [Update History]
  - 2026-03-27: 최초 생성 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp;
create temp table if not exists tmp_po2 as
select purchase_order_id
     , customer_id
     , order_date
     , total_amount
     , status
     , current_timestamp::timestamp as dbt_dtm
  from {{ source('edu', 'purchase_orders') }}
 where order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select purchase_order_id
     , customer_id
     , order_date
     , total_amount
     , status
     , dbt_dtm
  from tmp_po2
