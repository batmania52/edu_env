# dbt-Airflow 통합 교육 환경 설정 커리큘럼

**개요**: 이 커리큘럼은 Docker 기반의 dbt 및 Airflow 통합 실습 환경을 0단계부터 구축하기 위한 상세 가이드를 요약합니다. 데이터 정합성이 확보된 최신 데이터 세트와 자동화 스크립트를 활용합니다.

---

**Phase 1: 인프라 구축 (Docker)**
*   **목표**: 깨끗한 Docker 환경에서 Airflow 및 PostgreSQL 서비스를 초기화하고 실행합니다.
*   **세부 내용**:
    *   기존 작업 디렉토리 (`airflow`, `dbt_projects`, `venv_dbt`)를 삭제하여 환경을 초기화합니다.
    *   `refs/edu` 디렉토리가 존재하는지 확인합니다.
    *   `airflow` 작업 디렉토리를 생성하고, `docker-compose.yaml` 및 `Dockerfile`을 복사합니다.
    *   Docker Compose를 사용하여 모든 서비스를 중지하고 볼륨을 제거한 후, 다시 빌드하고 백그라운드에서 실행합니다. (`airflow` 디렉토리 내에서 실행)
    *   `airflow-init` 서비스가 완료될 때까지 대기하고, DB(5433), Airflow(8081) 포트 접속 가능 여부를 확인합니다.
*   **실행된 명령 예시**:
    ```bash
    rm -rf airflow dbt_projects venv_dbt
    mkdir -p refs/edu airflow
    cp refs/edu/docker_setup/docker-compose.yaml airflow/
    cp refs/edu/docker_setup/Dockerfile airflow/
    # cd airflow && docker-compose down -v && docker-compose up -d --build (port conflict 해결 후 재실행)
    ```
*   **특이사항**: Docker 포트 충돌(예: 5433) 발생 시, `docker ps -aq | xargs docker stop | xargs docker rm` 및 `docker system prune -f --volumes`를 통해 Docker 환경을 완전히 정리한 후 재시도합니다.

---

**Phase 2: 환경 설정 (Configuration)**
*   **목표**: Python 도구 및 Airflow 자산을 위한 연결 정보와 가상 환경을 설정합니다.
*   **세부 내용**:
    *   `dbconf.json.template`을 복사하여 Python 스크립트에서 사용할 데이터베이스 연결 정보를 설정합니다.
    *   Airflow DAGs 및 플러그인 파일을 Airflow 작업 디렉토리로 복사합니다.
    *   `venv_dbt`라는 Python 가상 환경을 생성하고, `refs/edu/requirements.txt`에 명시된 필수 패키지들을 설치합니다.
*   **실행된 명령 예시**:
    ```bash
    cp refs/edu/dbconf.json.template airflow/dbconf.json
    mkdir -p airflow/dags airflow/plugins
    cp refs/edu/airflow_assets/dags/*.py airflow/dags/
    cp refs/edu/airflow_assets/plugins/*.py airflow/plugins/
    python3.11 -m venv venv_dbt
    source venv_dbt/bin/activate && pip install -r refs/edu/requirements.txt
    ```

---

**Phase 3: 데이터 및 인프라 자동 구축**
*   **목표**: dbt 프로젝트에서 사용할 데이터베이스 스키마를 초기화하고, 원본 데이터를 로드하며, dbt 로그 인프라를 구축합니다.
*   **세부 내용**:
    *   `edu`, `stg`, `marts` 스키마를 생성합니다. (`refs/edu/tools/manage_schemas_for_test.py` 사용)
    *   원본 테이블 DDL을 실행하고, CSV 데이터를 각 스키마에 적재합니다. (`refs/edu/tools/execute_all_ddls.py`, `refs/edu/tools/load_data_from_csv.py` 사용)
    *   dbt 실행 이력을 기록할 `admin.dbt_log` 테이블을 포함한 로그 인프라를 구축합니다. (`refs/edu/tools/initialize_log_infrastructure.py` 사용)
*   **실행된 명령 예시**:
    ```bash
    source venv_dbt/bin/activate && python refs/edu/tools/manage_schemas_for_test.py
    source venv_dbt/bin/activate && python refs/edu/tools/execute_all_ddls.py
    source venv_dbt/bin/activate && python refs/edu/tools/load_data_from_csv.py
    source venv_dbt/bin/activate && python refs/edu/tools/initialize_log_infrastructure.py
    ```
*   **특이사항**: `stg_my_first_dbt_model.sql` 파일은 필요 없어 삭제되었고, `execute_all_ddls.py`는 디렉토리의 모든 `.sql` 파일을 참조하므로 별도의 수정은 필요 없었습니다.

---

**Phase 4: dbt 프로젝트 정비**
*   **목표**: 새로운 dbt 프로젝트를 초기화하고, `dbt_project.yml`을 올바르게 구성하며, 메타데이터 및 매크로를 설정하고 모델 파일을 배치합니다.
*   **세부 내용**:
    *   기존 `dbt_projects/edu001` 디렉토리를 삭제하고 `dbt init` 명령으로 새로운 `edu001` 프로젝트를 생성합니다.
    *   `dbt init`으로 생성된 `example` 모델 디렉토리를 삭제합니다.
    *   `profiles.yml.template`을 복사하여 dbt 프로필을 설정합니다.
    *   **dbt_project.yml 설정**: `stg` 및 `marts` 모델에 대한 `materialized`, `incremental_strategy`, `schema`, `on_schema_change`, `contract`, `tags`, 그리고 프로젝트 레벨 `pre-hook` 및 `on-run-end` 훅을 `refs/docs/dbt_project_rules.md`에 따라 정확하게 설정합니다. (이 단계가 모델 개발/실행보다 먼저 이루어져야 합니다.)
    *   `generate_schema_yml.py` 스크립트를 사용하여 `schema.yml` 파일을 자동 생성합니다.
    *   핵심 매크로(`log_model_start.sql`, `log_all_model_results.sql` 등) 및 `sources.yml` 파일을 dbt 프로젝트로 복사합니다.
    *   `refs/edu/models`에 있는 사전 정의된 `stg` 및 `marts` 모델 파일들을 `dbt_projects/edu001/models`로 복사합니다.
*   **실행된 명령 예시**:
    ```bash
    rm -rf dbt_projects/edu001
    cd dbt_projects && source ../venv_dbt/bin/activate && dbt init edu001 --skip-profile-setup
    rm -rf dbt_projects/edu001/models/example
    mkdir -p dbt_projects/edu001/.dbt
    cp refs/edu/profiles.yml.template dbt_projects/edu001/.dbt/profiles.yml
    # dbt_projects/edu001/dbt_project.yml 파일 수정 (pre-hook, on-run-end, stg/marts 설정)
    source venv_dbt/bin/activate && python refs/edu/tools/generate_schema_yml.py
    cp refs/edu/macros/*.sql dbt_projects/edu001/macros/
    cp refs/edu/models/sources.yml dbt_projects/edu001/models/sources.yml
    cp refs/edu/models/marts/*.sql dbt_projects/edu001/models/marts/
    cp refs/edu/models/marts/schema.yml dbt_projects/edu001/models/marts/
    cp refs/edu/models/stg/*.sql dbt_projects/edu001/models/stg/
    cp refs/edu/models/stg/schema.yml dbt_projects/edu001/models/stg/
    ```
*   **특이사항**: `dbt_project.yml`의 `models` 섹션에 `+pre-hook`, `on-run-end`, `+tags`와 `materialized: incremental` 등을 올바르게 설정하는 것이 중요합니다. `stg_order_items.sql` 모델의 `price` 컬럼 타입 불일치 문제 해결을 위해, `stg.stg_order_items` 테이블을 `NUMERIC(10, 2)` 타입으로 수동 재설정했습니다.

---

**Phase 5: 실행 및 검증 (End-to-End)**
*   **목표**: dbt 모델을 실행하고, Airflow와 dbt 간의 통합을 확인합니다.
*   **세부 내용**:
    *   `dbt run` 명령을 실행하여 모든 dbt 모델을 빌드합니다. (이 단계에서 `admin.dbt_log`에 로깅됩니다.)
    *   `admin.dbt_log` 테이블을 조회하여 dbt 작업의 성공 여부 및 상세 로그를 확인합니다.
    *   Airflow 웹 UI에 접속하여 `postgres_default` 연결이 올바르게 설정되었는지 확인합니다.
*   **실행된 명령 예시**:
    ```bash
    source venv_dbt/bin/activate && cd dbt_projects/edu001 && dbt run --profiles-dir .dbt -t dev
    # mcp_docker_query "SELECT model_name, status, start_time, end_time, execution_time_seconds, rows_affected FROM admin.dbt_log ORDER BY start_time DESC LIMIT 10;"
    docker exec airflow-airflow-webserver-1 airflow connections add postgres_default --conn-type 'postgres' --conn-host 'postgres' --conn-port '5432' --conn-login 'airflow' --conn-password 'airflow_password' --conn-schema 'airflow'
    ```
*   **수동 확인 사항**:
    *   호스트에서 DB 접속(5433), Airflow 접속(8081)이 가능한지 확인하십시오.
    *   `stg`, `marts` 테이블들의 데이터 건수가 0이 아닌 유의미한 수치를 기록하는지 확인하십시오.
    *   Airflow 웹 UI (`http://localhost:8081`, ID/PW: `airflow` / `airflow`)에 접속하여 통합을 확인하십시오.
