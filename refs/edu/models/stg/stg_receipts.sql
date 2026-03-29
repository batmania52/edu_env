{#-
  [AI_GENERATION_RULES]
  1. STICKY RIGHT: Align 'from', 'where', 'and', 'join', 'on' keywords to the right.
  2. INDENT=5: Use 5 spaces for indentation based on comma position.
  3. SELECT_FIRST_LINE: Place the first column on the same line as 'select'.
  4. NO BLANK LINES IN SQL: Do not include empty lines within the SQL query block.
  5. LOWERCASE: Use lowercase for all SQL keywords and identifiers.
  6. UPDATE_HISTORY: Always include the creation and modification history.
-#}

{#-
  [Model Information] - 모델의 메타데이터를 정의하는 섹션
  - Name: stg_receipts - 생성될 모델의 파일명
  - Developer: Gemini CLI - 담당 개발자명 (기본값: hjpark)
  - Created At: 2026-03-23 - 모델 생성 일자 (오늘 날짜)
  - Description: 영수증 데이터의 스테이징 모델
  
  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - 2026-03-23: 최초 생성 (Gemini CLI)
  - 2026-03-28: dbt_dtm 컬럼 추가 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where order_date >= '{{ start }}'::timestamp and order_date < '{{ end }}'::timestamp
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select receipt_id -- 첫 번째 컬럼은 select 키워드와 동일한 라인에 위치
     , order_id
     , customer_id
     , customer_name
     , customer_email
     , order_date
     , total_order_amount
     , product_id
     , product_name
     , product_category
     , item_quantity
     , item_price
     , item_total
     , current_timestamp::timestamp as dbt_dtm
  from {{ source('edu', 'receipts') }}
 where order_date >= '{{ start }}'::timestamp
   and order_date < '{{ end }}'::timestamp
