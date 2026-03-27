{#-
  [Model Information] - 모델의 메타데이터를 정의하는 섹션
  - Name: receipt_analysis_mart - 생성될 모델의 파일명
  - Developer: Antigravity - 담당 개발자명
  - Created At: 2026-03-26 - 모델 생성 일자
  - Description: 영수증 데이터를 기반으로 한 상세 판매 분석 마트 모델
  
  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - 2026-03-26: 최초 생성 (Antigravity)
  - 2026-03-27: 증분 조건을 between으로 변경 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select r.receipt_id::integer -- 첫 번째 컬럼은 select 키워드와 동일한 라인에 위치
     , r.order_id::integer
     , r.customer_id::integer
     , c.customer_name
     , c.customer_email
     , r.order_date
     , o.total_amount::numeric(10, 2) as total_order_amount
     , r.product_id::integer
     , p.product_name
     , p.product_category
     , r.item_quantity::integer
     , r.item_price::numeric(10, 2)
     , r.item_total::numeric(10, 2)
     , current_timestamp::timestamp as dbt_dtm
  from {{ ref('stg_receipts') }} as r
  left join {{ ref('stg_customers') }} as c
    on r.customer_id = c.customer_id
  left join {{ ref('stg_orders') }} as o
    on r.order_id = o.order_id
  left join {{ ref('stg_products') }} as p
    on r.product_id = p.product_id
 where r.order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
