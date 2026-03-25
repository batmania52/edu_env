## 1. 일반 규칙

*   **dbt docs 사용 금지**: 테스트 시 `dbt docs`는 절대 사용하지 마십시오.
*   **반복 오류 처리**: 테스트 시 같은 에러가 5번 이상 반복되면 시도를 중단하고 에러 내용을 보고하십시오.
*   **dbt_project.yml 수정**: 별도의 요청이 없으면 `dbt_project.yml` 파일 수정을 금지합니다. 필요하다고 판단되면 사용자에게 확인을 받십시오.
*   **dbt_project.yml 수정2**: `dbt_project.yml` 파일은 `on-run-end` 훅과 `models` 부분에 모든 모델 항목을 아래와 같이 정의해야 합니다.
```example
on-run-end:
  - "{{ log_all_model_results() }}"

models:
  <your_project_name>: # 예: edu001
    +pre-hook: # <your_project_name> 아래 모든 모델에 적용
      - sql: "{{ log_model_start(invocation_id) }}"
        transaction: False
    stg:
      materialized: incremental
      incremental_strategy: append
      +schema: stg
      +on_schema_change: fail
      +contract:
        enforced: true
      +tags: ["stg"]
    marts:
      materialized: incremental
      incremental_strategy: append
      +schema: marts
      +on_schema_change: fail
      +contract:
        enforced: true
      +tags: ["marts"]
```

*   **before_sql (날짜 컬럼 없음)**: 모델에 날짜 컬럼이 없어 증분 로딩이 불가능한 경우, `before_sql` 블록에서 `truncate table {{ this }}`를 사용하여 테이블을 완전히 새로 고칩니다.
*   **md 파일 재확인**: `md` 파일을 다시 읽으라는 명령이 있으면 명령 직후 해당 파일을 다시 읽으십시오.
*   **DB 접속 정보**: DB 접속이 필요할 경우 `dbconf.json` 파일의 정보를 통해 접근하십시오.

## 2. 모델 개발 규칙

### 2.1. 모델 개발 표준 프로세스 (SOP)

신규 모델 생성 시 아래 순서를 엄격히 준수하여 개발 효율성을 높이고 실행 오류를 방지합니다.

1.  **소스 조사 (Research)**: 소스 테이블 구조 확인 및 (필요 시) 사용자에게 기준 날짜 컬럼 확인.
2.  **대상 테이블 생성 (DDL)**: `before_sql` 실행 오류 방지를 위해 데이터베이스에 대상 테이블을 미리 생성.
    *   **대상 테이블 생성 스크립트 활용**: 대상 테이블 생성 시 `refs/edu/tools/execute_ddl.py` 스크립트를 활용하여 데이터베이스에 테이블을 미리 생성합니다.
        *   **사용법**: `python refs/edu/tools/execute_ddl.py [dbconf.json 경로] [DDL SQL 파일 경로]`
        *   **예시**: `python refs/edu/tools/execute_ddl.py airflow/dbconf.json refs/edu/ddls/marts_my_new_table.sql`
3.  **메타데이터 설정 (Config)**: 생성된 물리 테이블의 컬럼 정보를 조회하여 `sources.yml` 및 `schema.yml` 정의를 완료.
    ### 2.1.1. Schema.yml 업데이트 스크립트 활용 (`update_schema_yml.py`)

    모델 생성 후 `schema.yml` 파일에 해당 모델의 정의(컬럼 정보, 주석, Primary Key 등)를 자동으로 추가하거나 업데이트하는 스크립트입니다. DB에서 직접 메타데이터를 조회하여 정확하고 일관된 `schema.yml`을 생성하는 데 도움을 줍니다.

    *   **사용법**: `python refs/edu/tools/update_schema_yml.py --schema [DB 스키마 이름] --tables [테이블 이름1,테이블 이름2,...] --schema_file_path [schema.yml 파일 경로] --dbconf_path [dbconf.json 경로] [--update_existing]`
    *   **주요 인자**:
        *   `--schema (필수)`: 메타데이터를 조회할 DB 스키마 이름 (예: `stg`, `marts`).
        *   `--tables (필수)`: `schema.yml`에 정의를 추가/업데이트할 하나 이상의 테이블 이름 (콤마로 구분).
        *   `--schema_file_path (필수)`: 업데이트할 `schema.yml` 파일의 전체 경로.
        *   `--dbconf_path (필수)`: DB 접속 정보가 담긴 `dbconf.json` 파일의 경로.
        *   `--update_existing (선택)`: 기존 모델이 `schema.yml`에 존재할 경우 덮어쓸지 여부. 이 플래그가 없으면 이미 존재하는 모델은 건너뛰고 오류를 발생시킵니다.
    *   **기능**:
        *   DB에 해당 스키마 및 테이블이 존재하는지 검증합니다.
        *   DB에서 테이블 및 컬럼 주석, Primary Key 정보를 조회하여 `schema.yml`에 반영합니다.
        *   여러 테이블을 `--tables` 인자로 한 번에 처리할 수 있습니다.
        *   `--update_existing` 플래그가 `False`일 때, 이미 존재하는 모델은 건너뛰고 다음 모델 처리를 계속합니다.
    *   **예시**:
        *   `stg` 스키마의 `stg_customers` 모델 정의를 `stg/schema.yml`에 추가:
            `python refs/edu/tools/update_schema_yml.py --schema stg --tables stg_customers --schema_file_path dbt_projects/edu001/models/stg/schema.yml --dbconf_path airflow/dbconf.json`
        *   `marts` 스키마의 여러 모델 정의를 `marts/schema.yml`에 추가/업데이트:
            `python refs/edu/tools/update_schema_yml.py --schema marts --tables purchase_order_summary,customer_churn_risk_mart --schema_file_path dbt_projects/edu001/models/marts/schema.yml --dbconf_path airflow/dbconf.json --update_existing`
    *   **출력 요약**: 스크립트 실행 완료 후, `처리완료 모델`, `존재하는 모델`, `DB에 없는 테이블` 등의 요약 정보를 제공합니다.

### 2.2. 모델 헤더 규칙
 (상세 내용은 refs/docs/model_header_rules.md 참조)

### 2.3. SQL 작성 규칙

*   **Config Block**: 모델 생성 시 `config` block을 모델에 넣지 않습니다. (있으면 삭제)
*   **Source/Ref 사용**: `stg` 모델의 source는 `source()`를 사용하고, 그 외의 모델에서는 `ref()`를 사용하십시오.
*   **콤마 위치**: SQL 작성 시 콤마(`,`)는 반드시 맨 앞으로.
*   **SELECT 첫 줄**: SELECT 리스트의 첫 번째 항목은 `select` 키워드와 같은 줄에 위치합니다.
*   **콤마 뒤 공백**: 콤마(`,`) 뒤는 반드시 한 칸을 띄웁니다.
*   **들여쓰기**: `indent=5`를 준수합니다.
*   **Sticky Right 정렬**: `from`, `where`, `and`, `join`, `on` 등 주요 키워드는 우측으로 밀착시켜 정렬(Sticky Right)합니다. 즉, 키워드의 끝이 컬럼이나 조건의 시작점과 일치하도록 배치합니다.
*   **JOIN/ON 개행**: `join` 문 사용 시 `on` 절은 반드시 다음 줄로 개행하여 작성하며, Sticky Right 정렬을 유지합니다.
*   **쿼리 내 빈 줄 금지**: `select` 문 시작부터 끝까지 SQL 절 사이에는 불필요한 빈 줄을 삽입하지 않습니다. (Jinja 블록 사이의 빈 줄은 가독성을 위해 허용)
*   **별칭 수직 정렬**: `select` 절에서 여러 컬럼을 나열할 때, 가독성을 위해 `as` 키워드와 별칭(Alias)을 수직으로 일렬 정렬합니다.
*   **키워드 정렬**: `from`, `where`, `and` 등은 `select` 절의 시작 위치와 수직을 맞추는 대신, 우측 정렬을 통해 쿼리 구조를 명확히 합니다.
*   **개행 지침**: `case` 구문이나 복합 함수는 가독성을 위해 가급적 한 줄로 작성함을 원칙으로 합니다. 단, 라인 전체 길이가 너무 길어져(약 100자 이상) 가독성이 현저히 떨어지는 경우에만 예외적으로 적절한 위치에서 개행하여 작성합니다. (상세 예시는 `refs/models/line_break_example.sql` 참조)
*   **윈도우 함수 정렬**: `row_number()`, `sum() over()` 등 윈도우 함수 사용 시 `over` 절 내부가 길어질 경우, 가독성을 위해 `partition by`와 `order by`를 개행하여 정렬합니다. (상세 예시는 `refs/models/line_break_example.sql` 참조)
*   **소문자 변환**: SQL은 따옴표 안의 값과, 주석을 비롯한 데이터에 영향을 끼치는 모든 것을 제외한 나머지는 소문자로 변환합니다.
*   **선택적 명시적 캐스팅**: 기존 쿼리(DB2 등)와의 가독성 비교를 위해 모든 컬럼에 캐스팅을 적용하지 않고, 아래와 같이 반드시 필요한 경우에만 명시적 캐스팅을 수행합니다. 캐스팅 시에는 가독성을 위해 **`::type` (PostgreSQL 단축 표기)** 형식을 우선 사용합니다. (예: `col::numeric(13)`)
    *   `sum()`, `avg()` 등 집계 함수를 사용하여 Redshift에서 결과 타입이 변할 가능성이 있는 경우.
    *   산술 연산(`+`, `-`, `*`, `/`)이나 `round()` 등 복합 가공 로직이 포함된 경우.
    *   데이터 타입 불일치로 인해 dbt 빌드 시 오류가 발생하는 경우.
*   **기존 쿼리 캐스팅 계승**: 원본 SQL(DB2 등)에 명시적인 캐스팅 로직이 이미 포함되어 있는 경우, 이를 임의로 제거하지 않고 최대한 유지합니다. 단, 캐스팅되는 데이터 타입은 대상 데이터베이스(Redshift 등)의 표준 타입에 맞춰 적절히 수정하여 적용합니다. (예: DB2의 `char(10)` 캐스팅은 `varchar(10)` 또는 `text` 등으로 변경하여 구조 유지)
*   **날짜 기준 컬럼 확인**: `where` 절에 날짜 조건을 적용할 때 소스 테이블에 기준이 될 수 있는 날짜 컬럼이 여러 개 존재할 경우, 반드시 사용자에게 기준이 될 컬럼을 확인한 후 적용합니다.
*   **컴파일 공백 제거**: dbt 컴파일 시 불필요한 공백을 제거하기 위해 Jinja 블록(`{#- ... -#}`, `{%- ... -%}`)에 하이픈(`-`)을 사용하여 공백을 제어합니다. (특히 파일 상단 주석 및 변수 설정 블록 필수 적용)

### 2.4. 표준 템플릿 사용 지침

*   **템플릿 참조**: 모든 새로운 dbt 모델 생성 시, 반드시 `refs/models/` 디렉토리에 있는 표준 템플릿 파일을 참조하여 작성합니다.
    *   **Staging 모델**: `refs/models/stg_template.sql` 참조.
    *   **Marts 모델**: `refs/models/mart_template.sql` 참조.
    *   **개행 예시**: `refs/models/line_break_example.sql` 참조.
*   **지침 준수**: 템플릿에 포함된 `[AI_GENERATION_RULES]`를 엄격히 준수하여 Sticky Right, Indent=5 등 프로젝트 스타일을 일관되게 유지합니다.

### 2.5. SQL 스타일 검증 (SQLFluff)

*   **Lint 검사 필수**: 모든 모델 생성 및 수정 후에는 반드시 `sqlfluff lint` 명령을 통해 프로젝트의 SQL 스타일 지침 준수 여부를 검사해야 합니다.
*   **자동 교정**: 스타일 위반 사항이 발견될 경우 `sqlfluff fix`를 사용하여 자동으로 교정할 수 있습니다.
*   **설정 준수**: 루트 디렉토리의 `.sqlfluff` 설정 파일에 정의된 Sticky Right, Indent=5, Leading Comma 규칙을 엄격히 따릅니다.
