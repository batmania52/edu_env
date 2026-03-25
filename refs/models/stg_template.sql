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
  - Name: <MODEL_NAME> - 생성될 모델의 파일명
  - Developer: hjpark - 담당 개발자명 (기본값: hjpark)
  - Created At: <YYYY-MM-DD> - 모델 생성 일자
  - Description: <DESCRIPTION> - 모델의 목적 및 비즈니스 로직 요약
  
  [Update History] - 모델의 변경 이력을 관리하는 섹션
  - <YYYY-MM-DD>: 최초 생성 (hjpark)
-#}

{%- set start, end = get_date_intervals() -%}

{%- set before_sql -%}
delete from {{ this }} where <DATE_COLUMN> >= '{{ start }}'::<DATE_TYPE> and <DATE_COLUMN> < '{{ end }}'::<DATE_TYPE>
{%- endset -%}

{%- do run_query(before_sql) if execute -%}

select <FIRST_COLUMN> -- 첫 번째 컬럼은 select 키워드와 동일한 라인에 위치
     , <ADDITIONAL_COLUMNS>
     , <DATE_COLUMN>
  from {{ source('<SOURCE_NAME>', '<SOURCE_TABLE>') }}
 where <DATE_COLUMN> >= '{{ start }}'::<DATE_TYPE>
   and <DATE_COLUMN> < '{{ end }}'::<DATE_TYPE>
