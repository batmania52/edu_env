{#-
  [Model Information] - 모델의 메타데이터를 정의하는 섹션
  - Name: stg_products - 생성될 모델의 파일명
  - Developer: Gemini CLI - 담당 개발자명 (기본값: hjpark)
  - Created At: 2026-03-20 - 모델 생성 일자
  - Description: Staging model for raw product data
  
  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - 2026-03-20: 최초 생성 (Gemini CLI)
  - 2026-03-28: dbt_dtm 컬럼 추가 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where created_date >= '{{ start }}'::date and created_date < '{{ end }}'::date
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select product_id
     , product_name
     , product_category
     , price
     , created_date
     , current_timestamp::timestamp as dbt_dtm
  from {{ source('edu', 'raw_products') }}
 where created_date >= '{{ start }}'::date
   and created_date < '{{ end }}'::date
