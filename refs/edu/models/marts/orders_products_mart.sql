{#-
  [Model Information] - 모델의 메타데이터를 정의하는 섹션
  - Name: orders_products_mart - 생성될 모델의 파일명
  - Developer: Gemini CLI - 담당 개발자명 (기본값: hjpark)
  - Created At: 2026-03-20 - 모델 생성 일자
  - Description: Mart model joining orders, order items, and product details.
  
  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - 2026-03-20: 최초 생성 (Gemini CLI)
  - 2026-03-28: dbt_dtm 컬럼 추가 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where order_date >= '{{ start }}'::timestamp and order_date < '{{ end }}'::timestamp
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select o.order_id
     , o.customer_id
     , o.order_date
     , o.total_amount
     , oi.product_id
     , p.product_name
     , p.product_category
     , oi.price as item_price -- price of the item in the order
     , p.price as product_price -- base price from product master
     , oi.quantity
     , p.created_date as product_created_date
     , current_timestamp::timestamp as dbt_dtm
  from {{ ref('stg_orders') }} as o
  join {{ ref('stg_order_items') }} as oi
    on o.order_id = oi.order_id
  join {{ ref('stg_products') }} as p
    on oi.product_id = p.product_id
 where o.order_date >= '{{ start }}'::timestamp
   and o.order_date < '{{ end }}'::timestamp
