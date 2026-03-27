{#-
  [Model Information]
  - Name: purchase_order_summary
  - Developer: Gemini Agent
  - Created At: 2026-03-23
  - Description: 고객별 발주 요약 정보
  
  [Update History]
  - 2026-03-23: 최초 생성 (Gemini Agent)
  - 2026-03-27: 증분 조건을 between으로 변경 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where customer_id in (select distinct customer_id from {{ ref('stg_purchase_orders') }} where order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp)
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

with purchase_orders_summary as (
select po.customer_id
     , count(po.purchase_order_id) as total_purchase_orders
     , sum(po.total_amount)::numeric(10,2) as total_purchase_amount -- 캐스팅 적용
     , sum(case when po.status = 'pending' then 1 else 0 end) as pending_orders
     , sum(case when po.status = 'completed' then 1 else 0 end) as completed_orders
     , sum(case when po.status = 'canceled' then 1 else 0 end) as canceled_orders
     , min(po.order_date) as first_purchase_order_date
     , max(po.order_date) as last_purchase_order_date
  from {{ ref('stg_purchase_orders') }} as po
 where po.order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
 group by po.customer_id
)

select cust.customer_id
     , cust.customer_name
     , pos.total_purchase_orders
     , pos.total_purchase_amount
     , pos.pending_orders
     , pos.completed_orders
     , pos.canceled_orders
     , pos.first_purchase_order_date
     , pos.last_purchase_order_date
     , current_timestamp::timestamp as dbt_dtm
  from {{ ref('stg_customers') }} as cust
  join purchase_orders_summary as pos
    on cust.customer_id = pos.customer_id
