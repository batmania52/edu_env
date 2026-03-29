# Claude 행동 지침서 (edu_project)

> 이 문서는 edu_project에서 Claude가 작업 수행 시 반드시 준수해야 할 행동 규칙을 정의합니다.
> 원본 지침 출처: `GEMINI.md`, `refs/docs/` 하위 문서들

---

## 0. 전역 규칙 (모든 작업에 공통 적용)

- **출력 언어**: 모든 수행 과정과 결과는 **한국어**로 출력한다.
- **파일 우선 원칙**: 파일을 수정하거나 참조할 경우, 기억에 의존하지 말고 **반드시 파일을 먼저 읽고** 확인한다.
- **DB 접속**: DB 접속이 필요한 경우 `refs/edu/tools/dbconf.json` 파일의 정보를 사용한다.
- **반복 오류 처리**: 동일한 에러가 **5회 이상 반복**되면 시도를 중단하고 에러 내용을 사용자에게 보고한다.

---

## 1. dbt 모델 개발 규칙

### 1.1. 신규 모델 생성 표준 절차 (SOP)
반드시 아래 순서를 준수한다.

1. **소스 조사**: 소스 테이블 구조 확인. 날짜 기준 컬럼이 여러 개면 사용자에게 확인 요청.
2. **DDL 실행**: `execute_ddl.py`로 대상 테이블을 DB에 먼저 생성.
   ```bash
   python refs/edu/tools/execute_ddl.py refs/edu/tools/dbconf.json refs/edu/ddls/<DDL파일명>.sql
   ```
3. **schema.yml 업데이트**: `update_schema_yml.py`로 DB에서 메타데이터를 가져와 자동 반영.
   ```bash
   python refs/edu/tools/update_schema_yml.py \
     --schema <스키마명> \
     --tables <테이블명> \
     --schema_file_path dbt_projects/edu001/models/<스키마>/schema.yml \
     --dbconf_path refs/edu/tools/dbconf.json
   ```

### 1.2. 모델 헤더 규칙
- 모든 SQL 파일 **최상단**에 Jinja 주석 형식으로 헤더를 작성한다.
- 주석 형식: `{#- ... -#}` (하이픈 필수 — 공백 제거 목적)
- 작성자명 기본값: **`hjpark`** (별도 요청이 없으면 이 값을 사용)
- 모델 **변경 시** Update History에 반드시 이력을 추가한다.

```sql
{#-
  [Model Information]
  - Name: <파일명>
  - Developer: hjpark
  - Created At: YYYY-MM-DD
  - Description: <모델 목적 및 핵심 비즈니스 로직>

  [Update History]
  - YYYY-MM-DD: 최초 생성 (hjpark)
  - YYYY-MM-DD: <변경 사항 요약> (hjpark)
-#}
```

### 1.3. SQL 작성 스타일 규칙

| 규칙 | 내용 |
|------|------|
| **Config Block** | 모델 SQL에 `config` 블록 포함 금지 (있으면 삭제하고 schema.yml로 이동) |
| **source/ref** | stg 모델: `source()` 사용 / 나머지: `ref()` 사용 |
| **콤마 위치** | 콤마(`,`)는 반드시 **맨 앞**에 위치 |
| **SELECT 첫 줄** | 첫 번째 컬럼은 `select` 키워드와 **같은 줄**에 위치 |
| **들여쓰기** | `indent=5` 준수 (콤마 위치 기준 5칸) |
| **Sticky Right** | `from`, `where`, `and`, `join`, `on` 등 주요 키워드는 **우측 밀착 정렬** |
| **JOIN/ON** | `join` 다음 `on`은 반드시 **다음 줄**에 개행, Sticky Right 유지 |
| **빈 줄 금지** | `select`부터 끝까지 SQL 절 사이 불필요한 빈 줄 삽입 금지 |
| **소문자** | 따옴표 안 값·주석·데이터를 제외한 모든 SQL 키워드/식별자는 소문자 |
| **캐스팅** | 반드시 필요한 경우에만 `::type` 형식 사용 (예: `::numeric(13)`) |
| **공백 제거** | Jinja 블록에 하이픈 사용: `{%- ... -%}`, `{#- ... -#}` |
| **개행 기준** | CASE/복합 함수는 기본 한 줄 작성. 100자 초과 시에만 예외적으로 개행 |

**SQL 스타일 예시:**
```sql
select o.order_id
     , o.customer_id
     , c.customer_name
     , o.order_date
  from {{ ref('stg_orders') }} as o
  join {{ ref('stg_customers') }} as c
    on o.customer_id = c.customer_id
 where o.order_date >= '{{ start }}'::date
   and o.order_date < '{{ end }}'::date
```

### 1.4. 모델 템플릿 참조
신규 모델 생성 시 반드시 아래 템플릿을 참조한다.
- **Staging 모델**: `refs/models/stg_template.sql`
- **Marts 모델**: `refs/models/mart_template.sql`
- **개행 예시**: `refs/models/line_break_example.sql`

### 1.5. SQLFluff 검사
모든 모델 생성·수정 후 반드시 lint 검사를 실행한다.
```bash
# 검사
sqlfluff lint <파일경로>

# 자동 교정
sqlfluff fix <파일경로>
```

---

## 2. dbt 모델 실행 규칙

- **날짜 변수 형식**: `data_interval_start`, `data_interval_end`는 **Timestamp** 형태로 전달.
- **run_mode 필수**: dbt 모델 실행 시 항상 `run_mode=manual`로 설정.
- **개별 모델 실행**: `-s` 또는 `--select <모델명>` 옵션 사용.
- **`--full-refresh` 절대 사용 금지**: 어떠한 경우에도 사용하지 않는다.
- **`dbt docs` 사용 금지**: 테스트 시 절대 사용하지 않는다.
- **`dbt_project.yml` 수정 금지**: 별도 요청이 없으면 수정하지 않는다. 필요하다고 판단되면 **반드시 사용자 확인 후** 진행.

**올바른 실행 예시:**
```bash
dbt run -s stg_customers \
  --vars '{"run_mode": "manual", "data_interval_start": "2026-01-01 00:00:00", "data_interval_end": "2026-01-02 00:00:00"}'
```

---

## 3. schema.yml 파일 설정 규칙

- **파일 위치**: `models/<스키마명>/schema.yml`
- **`tests` 생략**: 모델 정보에 `tests` 블록은 작성하지 않는다.
- **Config 관리**: config 블록은 `schema.yml`에서만 관리. 모델 파일에 존재 시 schema.yml로 옮기고 모델에서 삭제.
- **Materialization**: `materialized='incremental'`, `incremental_strategy='append'`만 사용.
- **컬럼 정보 필수**: 컬럼명, `description`, `data_type`(길이 포함)을 반드시 포함. 정보 미상 시 DB에서 직접 조회.
- **`contract` 설정 금지**: `contract` 관련 설정은 `schema.yml`에 직접 넣지 않는다.

**schema.yml 컬럼 예시:**
```yaml
- name: customer_id
  description: 고객 고유 식별자
  data_type: varchar(18)
```

---

## 4. 데이터 검증 규칙

### 4.1. 검증 절차
1. 타겟 모델의 주 키 컬럼을 식별한다.
2. 소스 테이블에서 샘플 레코드 **5건**의 키 값을 추출한다.
3. 해당 키 값으로 소스 로직을 재현한 쿼리(`source_data`)를 작성한다.
4. 타겟 모델에서 동일 키로 데이터(`target_data`)를 추출한다.
5. `source_data`와 `target_data`를 컬럼 단위로 비교 검증한다.

### 4.2. 검증 스크립트 사용법
```bash
/Users/macbook/projects/edu_project/venv_dbt/bin/python refs/edu/tools/verify_dbt_model.py \
  --project_dir /Users/macbook/projects/edu_project/dbt_projects/edu001 \
  --model_name <스키마>.<모델명> \
  --date_column <날짜컬럼> \
  --start_date YYYY-MM-DD \
  --end_date YYYY-MM-DD \
  --target dev
```
> ⚠️ 실행 전 해당 모델이 `run_mode: manual`로 실행되었는지 반드시 확인한다.

---

## 5. Airflow DAG 개발 규칙

### 5.1. DAG 필수 설정 3가지
모든 DAG에 아래 설정을 반드시 포함한다.

```python
# DAG 생성 시 필수 포함
is_paused_upon_creation = True   # 배포 즉시 자동 실행 방지
catchup = False                   # 과거 데이터 백필 방지
depends_on_past = False           # 이전 실행 의존성 제거
```

### 5.2. dbt 연동 방식
직접 Operator 코딩 대신 `plugins` 디렉토리의 공통 유틸리티를 사용한다.
- **Cosmos 방식**: `dbt_cosmos_utils.get_dbt_tag_task_group()` — 모델 단위 시각화 필요 시
- **Bash 방식**: `dbt_bash_utils.get_dbt_bash_operator()` — 복합 명령 제어 필요 시

### 5.3. 컨텍스트 변수 전달 필수
dbt 실행 시 반드시 아래 Airflow 컨텍스트 변수를 `--vars`로 전달한다.
- `data_interval_start`
- `data_interval_end`
- `run_id`

**표준 DAG 코드 예시:**
```python
from airflow import DAG
from datetime import datetime
from dbt_bash_utils import get_dbt_bash_operator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'standard_dbt_workflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=True,
    tags=['standard', 'dbt'],
) as dag:

    dbt_run = get_dbt_bash_operator(
        task_id='dbt_run_marts',
        command='run',
        select='marts'
    )
```

---

## 6. Python 코딩 규칙

- **원본 직접 수정 금지**: 기존 파일은 직접 수정하지 않고, **새 파일로 먼저 작성**한다.
- **멀티라인 문자열**: 여러 줄 문자열은 삼중 따옴표(`"""`)를 사용한다.
- **수정 적용 절차**:
  1. 새 파일 작성 후 **테스트 진행**
  2. 사용자 **확인** 수령
  3. 기존 파일을 해당 디렉토리의 `bak/` 서브디렉토리로 이동
  4. 새 파일을 **원본 파일명으로 rename**

---

## 7. 절대 금지 사항 요약

| 금지 항목 | 이유 |
|-----------|------|
| `dbt run --full-refresh` | 데이터 덮어쓰기 위험 |
| `dbt docs` 사용 | 테스트 환경 규칙 |
| `config` 블록을 모델 SQL에 포함 | schema.yml에서 중앙 관리 |
| `contract` 설정을 schema.yml에 직접 기재 | 프로젝트 설정 충돌 방지 |
| `dbt_project.yml` 무단 수정 | 전역 설정 영향도 고려 필요 |
| Python 원본 파일 직접 수정 | 롤백 불가 방지 |
| `auditor.py` 무단 수정 | 검증 도구 무결성 유지 |

---

## 8. 주요 디렉토리 구조

```
edu_project/                              ← 프로젝트 루트
├── CLAUDE.md                             ← Claude 행동 지침서 (이 파일)
├── GEMINI.md                             ← 원본 AI 지침 (전역 설정 + 문서 목차)
├── CURRICULUM.md                         ← 교육 커리큘럼 문서
├── .sqlfluff                             ← SQLFluff 전역 설정 (Sticky Right, indent=5 등)
│
├── airflow/                              ← Airflow Docker 환경
│   ├── dbconf.json                       ← DB 접속 정보 (참조용 — 작업 시 refs/edu/tools/dbconf.json 사용)
│   ├── Dockerfile                        ← Airflow 이미지 정의
│   ├── docker-compose.yaml               ← Airflow 컨테이너 구성
│   ├── entrypoint.sh                     ← 컨테이너 시작 스크립트
│   ├── dags/
│   │   └── dbt_daily_flow.py             ← 실습용 Airflow DAG (dbt 일별 실행)
│   └── plugins/
│       └── dbt_cosmos_utils.py           ← Cosmos 방식 dbt 연동 공통 유틸
│
├── dbt_projects/
│   └── edu001/                           ← 실습 dbt 프로젝트 (project name: edu001)
│       ├── dbt_project.yml               ← dbt 프로젝트 설정 (수정 금지 — 변경 시 사용자 확인)
│       ├── models/
│       │   ├── sources.yml               ← 소스 테이블 정의 (schema: edu)
│       │   ├── stg/                      ← Staging 레이어
│       │   │   ├── schema.yml            ← stg 모델 config/컬럼 정의
│       │   │   ├── stg_customers.sql     ← 고객 스테이징
│       │   │   ├── stg_orders.sql        ← 주문 스테이징
│       │   │   ├── stg_order_items.sql   ← 주문 상품 스테이징
│       │   │   ├── stg_products.sql      ← 상품 스테이징
│       │   │   ├── stg_purchase_orders.sql ← 발주 스테이징
│       │   │   └── stg_receipts.sql      ← 영수증 스테이징
│       │   └── marts/                    ← Marts 레이어
│       │       ├── schema.yml            ← marts 모델 config/컬럼 정의
│       │       ├── orders_mart.sql           ← 주문 마트
│       │       ├── orders_customers_mart.sql ← 주문-고객 통합 마트
│       │       ├── orders_products_mart.sql  ← 주문-상품 통합 마트
│       │       ├── customer_receipt_mart.sql ← 고객 영수증 마트
│       │       ├── customer_churn_risk_mart.sql ← 고객 이탈 위험 마트
│       │       └── purchase_order_summary.sql   ← 발주 요약 마트
│       └── macros/                       ← dbt 커스텀 매크로
│           ├── get_date_intervals.sql    ← run_mode에 따른 날짜 범위 반환
│           ├── generate_schema_name.sql  ← 스키마명 생성 규칙
│           ├── log_model_start.sql       ← 모델 시작 로그 기록 (pre-hook)
│           └── log_all_model_results.sql ← 전체 모델 결과 로그 (on-run-end)
│
├── mycodes/                              ← 사용자 작성 유틸리티 스크립트
│   ├── dbt_runner.py                     ← dbt 실행 CLI 도구
│   ├── dbt_runner_st.py                  ← dbt 실행 Streamlit 앱
│   ├── dbt_unified_app.py                ← 통합 dbt 관리 앱
│   ├── dbt_utils.py                      ← dbt 공통 유틸 함수
│   ├── generate_model_yml_cli.py         ← 모델 schema.yml 생성 CLI
│   └── generate_schema_yml_st.py         ← schema.yml 생성 Streamlit 앱
│
└── refs/                                 ← 참조 영역 (원본/템플릿/도구)
    ├── docs/                             ← 원본 지침 문서
    │   ├── dbt_project_rules.md          ← dbt 일반·SQL 작성 규칙
    │   ├── model_header_rules.md         ← 모델 헤더 작성 규칙
    │   ├── model_execution_rules.md      ← 모델 실행 규칙
    │   ├── schema_file_config.md         ← schema.yml 설정 지침
    │   ├── verification_guidelines.md    ← 데이터 검증 절차
    │   ├── auditor_guidelines.md         ← Auditor 실행 지침
    │   ├── airflow_project_rules.md      ← Airflow DAG 개발 규칙
    │   ├── python_guidelines.md          ← Python 코딩 지침
    │   └── dbt_training_curriculum.md    ← 교육 커리큘럼
    │
    ├── models/                           ← SQL 표준 템플릿
    │   ├── stg_template.sql              ← Staging 모델 템플릿
    │   ├── mart_template.sql             ← Marts 모델 템플릿
    │   └── line_break_example.sql        ← 개행 규칙 예시 (CASE/윈도우 함수)
    │
    └── edu/                              ← 교육용 원본 파일
        ├── datas/                        ← 소스 원본 CSV 데이터
        │   ├── edu_order.csv             ← 주문 데이터
        │   ├── edu_order_items.csv       ← 주문 상품 데이터
        │   ├── edu_purchase_orders.csv   ← 발주 데이터
        │   ├── edu_raw_customers.csv     ← 고객 원본 데이터
        │   └── edu_raw_products.csv      ← 상품 원본 데이터
        ├── ddls/                         ← 테이블 DDL 스크립트
        │   ├── edu_*.sql                 ← 소스(raw) 테이블 DDL (6개)
        │   ├── stg_stg_*.sql             ← stg 레이어 테이블 DDL (6개)
        │   ├── marts_*.sql               ← marts 레이어 테이블 DDL (6개)
        │   └── admin_*.sql               ← 검증용 관리 테이블 DDL (4개)
        ├── models/                       ← 완성 모델 참조본 (stg + marts)
        ├── macros/                       ← 완성 매크로 참조본
        ├── airflow_assets/               ← Airflow DAG/플러그인 참조본
        └── tools/                        ← Python 자동화 스크립트
            ├── dbconf.json               ← DB 접속 정보 (tools 전용)
            ├── execute_ddl.py            ← DDL 단일 실행
            ├── execute_all_ddls.py       ← DDL 전체 일괄 실행
            ├── update_schema_yml.py      ← schema.yml 자동 업데이트
            ├── load_data_from_csv.py     ← CSV → DB 데이터 로드
            ├── verify_dbt_model.py       ← dbt 모델 데이터 검증
            ├── generate_schema_yml.py    ← schema.yml 신규 생성
            ├── create_table.py           ← 테이블 생성
            ├── manage_schemas_for_test.py ← 테스트용 스키마 관리
            ├── initialize_log_infrastructure.py ← 로그 인프라 초기화
            ├── remove_model_from_schema_yml.py  ← schema.yml에서 모델 제거
            ├── remove_source_from_sources_yml.py ← sources.yml에서 소스 제거
            └── bak/                      ← 교체된 구버전 원본 파일 보관
```

---

*최초 작성: 2026-03-26 | 출처: GEMINI.md + refs/docs/ 지침 문서 통합*
*디렉토리 구조 업데이트: 2026-03-26*
