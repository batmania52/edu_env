{#-
  [Model Information] - 모델의 메타데이터를 정의하는 섹션
  - Name: orders_customers_mart - 생성될 모델의 파일명
  - Developer: Gemini CLI - 담당 개발자명 (기본값: hjpark)
  - Created At: 2026-03-20 - 모델 생성 일자
  - Description: Mart model joining orders and customer information.
  
  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - 2026-03-20: 최초 생성 (Gemini CLI)
  - 2026-03-27: 증분 조건을 between으로 변경 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select o.order_id
     , o.customer_id
     , c.customer_name
     , c.customer_email
     , o.order_date
     , o.total_amount
     , c.registration_date
     , current_timestamp::timestamp as dbt_dtm
  from {{ ref('stg_orders') }} as o
  join {{ ref('stg_customers') }} as c
    on o.customer_id = c.customer_id
 where o.order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp