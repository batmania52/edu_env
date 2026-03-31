{#-
  [Model Information]
  - Name: orders_churn_mart
  - Developer: hjpark
  - Created At: 2026-03-31
  - Description: 주문별 고객 이탈 위험도 마트. orders_customers_mart와 customer_churn_risk_mart를 조인하여 주문 단위로 이탈 위험 정보를 제공.

  [Update History]
  - 2026-03-31: 최초 생성 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select ocm.order_id
     , ocm.customer_id
     , ocm.customer_name
     , ocm.order_date
     , ocm.total_amount
     , ccr.churn_risk_score
     , ccr.churn_risk_segment
     , ccr.days_since_last_order
     , ccr.total_orders
     , ccr.avg_order_value
     , current_timestamp::timestamp as dbt_dtm
  from {{ ref('orders_customers_mart') }} as ocm
  left join {{ ref('customer_churn_risk_mart') }} as ccr
    on ocm.customer_id = ccr.customer_id
 where ocm.order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
