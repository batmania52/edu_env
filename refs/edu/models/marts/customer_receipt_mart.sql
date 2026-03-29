{#-
  [AI_GENERATION_RULES]
  1. STICKY RIGHT: Align 'from', 'where', 'and', 'join', 'on', 'group by' keywords to the right.
  2. INDENT=5: Use 5 spaces for indentation based on comma position.
  3. SELECT_FIRST_LINE: Place the first column on the same line as 'select'.
  4. JOIN/ON NEWLINE: Always place 'on' on a new line after 'join'.
  5. NO BLANK LINES IN SQL: Do not include empty lines within the SQL query block.
  6. LOWERCASE: Use lowercase for all SQL keywords and identifiers.
  7. UPDATE_HISTORY: Always include the creation and modification history.
-#}

{#-
  [Model Information] - 모델의 메타데이터를 정의하는 섹션
  - Name: customer_receipt_mart - 생성될 모델의 파일명
  - Developer: Gemini CLI - 담당 개발자명
  - Created At: 2026-03-23 - 모델 생성 일자 (오늘 날짜)
  - Description: 고객별 영수증 요약 정보 마트 모델
  
  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - 2026-03-23: 최초 생성 (Gemini CLI)
  - 2026-03-28: dbt_dtm 컬럼 추가 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where customer_id in (select customer_id from {{ ref('stg_receipts') }} where order_date >= '{{ start }}'::timestamp and order_date < '{{ end }}'::timestamp)
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select customer_id -- 첫 번째 컬럼은 select 키워드와 동일한 라인에 위치
     , customer_name
     , customer_email
     , min(order_date)::date as first_order_date
     , max(order_date)::date as last_order_date
     , count(distinct order_id) as total_receipt_count
     , sum(item_quantity) as total_items_purchased
     , sum(item_total)::numeric(18,2) as total_spend_amount
     , avg(item_price)::numeric(10,2) as average_item_price
     , current_timestamp::timestamp as dbt_dtm
  from {{ ref('stg_receipts') }}
 where order_date >= '{{ start }}'::timestamp
   and order_date < '{{ end }}'::timestamp
 group by 1, 2, 3
