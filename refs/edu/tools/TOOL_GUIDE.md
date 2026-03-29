# dbt 프로젝트 관리 도구 가이드 (Python Tools)

이 문서는 `refs/edu/tools/` 디렉토리에 위치한 파이썬 스크립트들의 기능과 사용법을 설명합니다. 이 도구들은 데이터베이스 초기화, DDL 실행, 데이터 로드, 그리고 dbt 메타데이터 관리 자동화를 위해 설계되었습니다.

---

## 1. 데이터베이스 인프라 및 스키마 관리

### 🛠️ initialize_log_infrastructure.py
dbt 실행 로그 및 검증 이력을 저장하기 위한 인프라를 초기화합니다.
- **기능**: `admin` 스키마 생성 및 `admin.dbt_log` 테이블 생성.
- **실행**: `python refs/edu/tools/initialize_log_infrastructure.py`

### 🏗️ manage_schemas_for_test.py
테스트 환경을 위해 주요 스키마를 초기화하거나 정리합니다.
- **기능**: `edu`, `stg`, `marts` 스키마가 존재할 경우 백업(`_bak`) 후 삭제하고, 깨끗한 상태의 스키마를 새롭게 생성합니다.
- **실행**: `python refs/edu/tools/manage_schemas_for_test.py`

---

## 2. DDL 및 테이블 관리

### 📜 execute_all_ddls.py
정의된 모든 테이블 구조를 데이터베이스에 한꺼번에 반영합니다.
- **기능**: `refs/edu/ddls/` 디렉토리 내의 모든 `.sql` 파일을 알파벳 순서로 읽어 실행합니다.
- **실행**: `python refs/edu/tools/execute_all_ddls.py`

### 📄 execute_ddl.py / create_table.py
특정 SQL 파일을 지정하여 실행하거나 테이블을 생성합니다.
- **execute_ddl.py**: 인자값을 통해 DB 설정과 SQL 경로를 받아 실행하는 CLI 도구입니다.
  - `python refs/edu/tools/execute_ddl.py [dbconf_path] [sql_path] --db_key [key]`
- **create_table.py**: 스크립트 내에 지정된 특정 DDL 파일을 실행합니다.

---

## 3. 데이터 로드

### 📥 load_data_from_csv.py
CSV 파일의 데이터를 데이터베이스 테이블로 로드합니다.
- **기능**: `refs/edu/datas/` 내의 `edu_*.csv` 파일들을 읽어 `edu` 스키마의 해당 테이블에 적재합니다. (실행 전 해당 테이블을 `TRUNCATE`하여 정합성을 유지합니다.)
- **실행**: `python refs/edu/tools/load_data_from_csv.py`

---

## 4. dbt 메타데이터 (YAML) 관리

### 📝 generate_schema_yml.py
DB의 실제 테이블 구조를 분석하여 dbt `schema.yml` 파일을 자동 생성합니다.
- **기능**: `stg`, `marts` 스키마의 테이블, 컬럼, 데이터 타입, PK 정보를 추출하여 YAML 파일을 생성합니다.
- **실행**: `python refs/edu/tools/generate_schema_yml.py`

### 🔄 update_schema_yml.py
기존 `schema.yml`에 새로운 테이블 정의를 추가하거나 업데이트합니다.
- **주요 인자**: `--schema`, `--tables`, `--schema_file_path`, `--update_existing`
- **실행 예시**: 
  ```bash
  python refs/edu/tools/update_schema_yml.py --schema stg --tables stg_customers --schema_file_path models/stg/schema.yml --dbconf_path refs/edu/dbconf.json --update_existing
  ```

### 🗑️ remove_model_from_schema_yml.py / remove_source_from_sources_yml.py
YAML 파일에서 특정 모델이나 소스 정의를 안전하게 삭제합니다.
- **모델 삭제**: `python refs/edu/tools/remove_model_from_schema_yml.py --schema_file_path [path] --model_name [name]`
- **소스 삭제**: `python refs/edu/tools/remove_source_from_sources_yml.py --sources_file_path [path] --source_name [source] --table_name [table]`

---

## 5. 통합 대화형 도구

### 🔍 verify_dbt_model.py
터미널에서 대화형(Interactive)으로 DB 구조를 분석하고 dbt 설정을 관리하는 통합 도구입니다.
- **기능**: 
  - DB 스키마/테이블 브라우징
  - 신규 모델 감지 및 YAML 정의 생성
  - 여러 YAML 파일에 흩어진 모델 정의를 하나로 통합 및 중복 제거
  - 수정 전 원본 파일 자동 백업 생성
- **실행**: `python refs/edu/tools/verify_dbt_model.py` (InquirerPy 기반 GUI 제공)

---

## 공통 설정 (dbconf.json)
모든 도구는 `refs/edu/tools/dbconf.json`에 정의된 접속 정보를 참조합니다.
```json
{
  "postgres_default": {
    "host": "localhost",
    "port": 5433,
    "user": "airflow",
    "password": "airflow",
    "database": "airflow"
  }
}
```
