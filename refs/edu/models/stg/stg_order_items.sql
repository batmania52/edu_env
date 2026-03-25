{#-
  [Model Information] - 모델의 메타데이터를 정의하는 섹션
  - Name: stg_order_items - 생성될 모델의 파일명
  - Developer: Gemini CLI - 담당 개발자명 (기본값: hjpark)
  - Created At: 2026-03-20 - 모델 생성 일자
  - Description: Staging model for raw order items data. No date column for incremental loading, so a full truncate is performed.
  
  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - 2026-03-20: 최초 생성 (Gemini CLI)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
truncate table {{ this }}
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select id as order_item_id
     , order_id
     , product_id
     , quantity
     , price
  from {{ source('edu', 'order_items') }}