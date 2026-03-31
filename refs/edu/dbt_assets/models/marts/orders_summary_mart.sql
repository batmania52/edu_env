{#-
  [Model Information] - 모델의 메타데이터를 정의하는 섹션
  - Name: orders_summary_mart - 생성될 모델의 파일명
  - Developer: hjpark - 담당 개발자명 (기본값: hjpark)
  - Created At: 2026-03-28 - 모델 생성 일자
  - Description: 주문-고객-상품 통합 요약 마트. orders_mart와 orders_customers_mart를 참조하여 리니지 depth 테스트용으로 생성.

  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - 2026-03-28: 최초 생성 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select om.order_id
     , om.customer_id
     , ocm.customer_name
     , ocm.customer_email
     , ocm.registration_date
     , om.order_date
     , om.total_amount
     , om.product_id
     , om.quantity
     , om.price
     , om.item_total
     , current_timestamp::timestamp as dbt_dtm
  from {{ ref('orders_mart') }} as om
  join {{ ref('orders_customers_mart') }} as ocm
    on om.order_id = ocm.order_id
 where om.order_date between '{{ start }}'::timestamp and '{{ end }}'::timestamp
