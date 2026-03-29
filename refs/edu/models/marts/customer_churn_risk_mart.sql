{#-
  [Model Information] - 모델의 메타데이터를 정의하는 섹션
  - Name: customer_churn_risk_mart - 생성될 모델의 파일명
  - Developer: Gemini CLI - 담당 개발자명 (기본값: hjpark)
  - Created At: 2026-03-20 - 모델 생성 일자
  - Description: Mart model analyzing customer churn risk based on order history.
  
  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - 2026-03-20: 최초 생성 (Gemini CLI)
  - 2026-03-28: dbt_dtm 컬럼 추가 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where analysis_date >= '{{ start }}'::date and analysis_date < '{{ end }}'::date
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

with customer_orders as (
    select customer_id
         , max(order_date) as last_order_date
         , count(order_id) as total_orders
         , avg(total_amount) as avg_order_value
      from {{ ref('stg_orders') }}
     where order_date >= '{{ start }}'::timestamp -- filter orders based on date range
       and order_date < '{{ end }}'::timestamp
     group by customer_id
)
select c.customer_id
     , c.customer_name
     , c.customer_email
     , c.registration_date
     , co.last_order_date
     , (date '{{ end }}' - co.last_order_date::date) as days_since_last_order
     , co.total_orders
     , co.avg_order_value::numeric(10,2) as avg_order_value
     , case
         when (date '{{ end }}' - co.last_order_date::date) > 90 then '3'
         when (date '{{ end }}' - co.last_order_date::date) > 30 then '2'
         else '1'
       end::numeric(10,2) as churn_risk_score
     , case
         when (date '{{ end }}' - co.last_order_date::date) > 90 then 'High'
         when (date '{{ end }}' - co.last_order_date::date) > 30 then 'Medium'
         else 'Low'
       end::varchar(255) as churn_risk_segment
     , date '{{ end }}'::date as analysis_date
     , current_timestamp::timestamp as dbt_dtm
  from {{ ref('stg_customers') }} as c
  left join customer_orders as co
    on c.customer_id = co.customer_id
 where c.registration_date >= '{{ start }}'::date -- filter customers based on registration date (proxy for analysis_date)
   and c.registration_date < '{{ end }}'::date
