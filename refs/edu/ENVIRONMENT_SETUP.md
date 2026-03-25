# dbt-Airflow 통합 교육 환경 설정 가이드 (v4)

이 문서는 Docker 기반의 dbt와 Airflow 통합 실습 환경을 0단계부터 구축하기 위한 상세 가이드입니다. 이 가이드는 데이터 정합성이 확보된 최신 데이터 세트와 자동화 스크립트를 활용합니다.

## 1. 인프라 구축 (Docker)

### 1.1 서비스 초기화 및 실행
기존 상태를 완전히 삭제하고 깨끗한 환경에서 시작합니다. 이 과정은 현재 프로젝트의 Docker 서비스에만 영향을 미치며, 다른 실행 중인 Docker 컨테이너는 건드리지 않습니다.
```bash
# 기존 Airflow, dbt 프로젝트 및 가상 환경 디렉토리 삭제
rm -rf airflow
rm -rf dbt_projects
rm -rf venv_dbt

# 필요한 디렉토리 생성
mkdir -p refs/edu

# Airflow 작업 디렉토리 생성 및 Docker 설정 파일 복사
mkdir -p airflow
cp refs/edu/docker_setup/docker-compose.yaml airflow/
cp refs/edu/docker_setup/Dockerfile airflow/


# Docker 서비스 재시작: 기존 서비스 중지 및 컨테이너/볼륨 삭제 후 빌드 및 백그라운드 실행
cd airflow && docker-compose down -v && docker-compose up -d --build
```
*   **주의**: `airflow-init` 서비스가 완료될 때까지 약 1분 정도 대기하십시오.
*   **포트 확인**: 호스트에서 DB 접속(5433), Airflow 접속(8081)이 가능한지 확인합니다.

## 2. 환경 설정 (Configuration)

### 2.1 연결 정보 설정
`refs/edu`에 있는 템플릿을 복사하여 실제 설정 파일을 생성합니다.
```bash
# 1. Python 도구용 설정
cp refs/edu/dbconf.json.template airflow/dbconf.json
```

### 2.2 Airflow 자산 배치
Airflow DAGs 및 플러그인 파일을 Airflow 작업 디렉토리로 복사합니다.
```bash
# Airflow DAGs 복사
mkdir -p airflow/dags
cp refs/edu/airflow_assets/dags/*.py airflow/dags/

# Airflow 플러그인 복사
mkdir -p airflow/plugins
cp refs/edu/airflow_assets/plugins/*.py airflow/plugins/
```

### 2.3 Python 가상 환경 설정

이 프로젝트의 Python 의존성을 관리하고 `psycopg2`와 같은 필수 라이브러리를 설치합니다.

```bash
# 가상 환경 생성 (최초 1회)
python3.11 -m venv venv_dbt

# 가상 환경 활성화 및 필요한 패키지 설치
source venv_dbt/bin/activate && pip install -r refs/edu/requirements.txt
```

## 3. 데이터 및 인프라 자동 구축

다음 순서대로 Python 스크립트를 실행하여 데이터베이스 환경을 완성합니다. (가상 환경의 Python 권장)

### 3.1 스키마 초기화
실습용 스키마(`edu`, `stg`, `marts`)를 생성합니다.
```bash
source venv_dbt/bin/activate && python refs/edu/tools/manage_schemas_for_test.py
```

### 3.2 원본 데이터 로드
원본 테이블 DDL을 실행하고, 정합성이 완료된 최신 CSV 데이터를 적재합니다.
```bash
source venv_dbt/bin/activate && python refs/edu/tools/execute_all_ddls.py
source venv_dbt/bin/activate && python refs/edu/tools/load_data_from_csv.py
```

### 3.3 로그 인프라 구축
dbt 실행 이력을 기록할 `admin.dbt_log` 인프라를 구축합니다.
```bash
source venv_dbt/bin/activate && python refs/edu/tools/initialize_log_infrastructure.py
```

## 4. dbt 프로젝트 정비

### 4.1 dbt 프로젝트 초기화
새로운 dbt 프로젝트를 생성하고 기본 구조를 설정합니다.

```bash
# 기존 dbt 프로젝트 디렉토리 (edu001) 삭제 (선택 사항이나 권장)
rm -rf dbt_projects/edu001

# dbt 프로젝트 초기화
cd dbt_projects&& source ../venv_dbt/bin/activate && dbt init edu001 --skip-profile-setup

# 기본 example 모델 삭제 (프로젝트 필요에 따라 제거)
rm -rf dbt_projects/edu001/models/example

# dbt 프로필 설정 파일 복사 (기본 profiles.yml을 덮어씀)
mkdir -p dbt_projects/edu001/.dbt
cp refs/edu/profiles.yml.template dbt_projects/edu001/.dbt/profiles.yml

```
### 4.2 메타데이터 및 매크로 설정
현재 DB 구조와 dbt 모델을 동기화하고 운영 매크로를 배치합니다.
```bash
# 1. schema.yml 자동 생성
source venv_dbt/bin/activate && python refs/edu/tools/generate_schema_yml.py

# 2. 핵심 매크로 복사
cp refs/edu/macros/*.sql dbt_projects/edu001/macros/
```

## 4.3 프로젝트 구성
생성한 프로젝트 설정을 합니다.

**⚠️ 중요: 다음 단계를 순서대로 진행해야 합니다.**

### 1. dbt_project.yml 설정 (가장 먼저 수행)
** 각 스키마에 맞게 model항목에 추가해야 합니다.**
** 상세한 `dbt_project.yml` model 설정은 `refs/docs/dbt_project_rules.md`를 반드시 참고하십시오.**

### 2. model 개발
** 각 모델의 생성규칙은 `refs/docs/dbt_project_rules.md`를 참고하십시오.**
** stg 모델은 edu 스키마에서 1:1로 개발해야 합니다.**
** marts 모델은 stg 모델을 참조해서 join sql을 자의적으로 판단해서 개발해야 합니다.**
** 모델 작성 템플릿은 `@refs/models` 이하의 파일들을 참조해야 합니다.**

## 5. 실행 및 검증 (End-to-End)

### 5.1 dbt 전체 실행
```bash
cd dbt_projects/edu001
dbt run --profiles-dir .dbt -t dev
```
*   **검증**: `stg`, `marts` 테이블들의 데이터 건수가 0이 아닌 유의미한 수치를 기록하는지 확인하십시오.

### 5.2 Airflow 통합 확인
*   **URL**: http://localhost:8081 (ID/PW: `airflow` / `airflow`)
*   **Admin -> Connections**: `postgres_default`가 없으므로 connection 정보를 넣을것(gemini가 수행)
```bash
docker exec airflow-airflow-webserver-1 airflow connections add postgres_default --conn-type 'postgres' --conn-host 'postgres' --conn-port '5432' --conn-login 'airflow' --conn-password 'airflow_password' --conn-schema 'airflow'
```
---
**💡 팁**: 모든 과정이 자동화되어 있으므로, 환경이 꼬였을 경우 `Phase 1.1`부터 다시 시작하면 언제든 깨끗한 상태로 복구됩니다.
