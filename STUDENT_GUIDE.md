# 🎓 dbt-Airflow 통합 환경 구축 실습 가이드 (교육생용)

이 가이드는 **Docker** 기반의 **dbt**와 **Airflow** 통합 환경을 처음부터 구축하며 데이터 엔지니어링의 기초를 다지는 실습서입니다. 각 단계를 차근차근 따라하며 환경을 완성해 보세요.

---

## 🚀 1단계: 인프라 환경 구축 (Docker)

실습에 필요한 데이터베이스(Postgres)와 워크플로우 도구(Airflow)를 컨테이너로 띄웁니다.

### 1.1 기존 환경 정리 및 설정 파일 준비
이미 실행 중이거나 설정된 파일들이 있다면 충돌을 방지하기 위해 초기화합니다.

```bash
# 1. 이전 설정 파일 및 가상 환경 삭제 (주의: 기존 데이터가 삭제됩니다)
rm -rf airflow/config airflow/dags airflow/logs airflow/plugins
rm -rf dbt_projects/edu001
rm -rf venv_dbt

# 2. 실습용 Docker 설정 파일 복사
cp refs/edu/docker_setup/docker-compose.yaml airflow/
cp refs/edu/docker_setup/Dockerfile airflow/
cp refs/edu/docker_setup/entrypoint.sh airflow/

# 3. Airflow 초기화 스크립트 실행
. airflow_init.sh
```

### 1.2 Docker 컨테이너 실행
프로젝트 루트에서 다음 명령어를 실행하여 서비스를 백그라운드(`-d`)로 실행합니다.

```bash
cd airflow && docker-compose down -v && docker-compose up -d --build
```
*   **💡 팁**: 실행 후 웹 브라우저에서 `http://localhost:8081` (ID/PW: `airflow`/`airflow`) 접속이 되는지 확인하세요!

---

## 🐍 2단계: Python 가상 환경 설정 및 데이터베이스 초기화

아래 스크립트 하나로 가상 환경 설치, 패키지 설치, 데이터베이스 초기화가 한 번에 완료됩니다.
**이후 모든 실습 과정은 가상 환경이 활성화된 상태에서 진행해야 합니다.**

```bash
# 가상환경 설치 + 활성화 + DB 초기화를 한 번에 실행합니다.
# (가상환경 자동 생성 -> 패키지 설치 -> 스키마 생성 -> DDL 실행 -> CSV 로드 -> 로그 테이블 생성)
. set_environments.sh
```

*   **실행 내용**:
    *   `venv_dbt` 가상 환경 자동 생성 및 패키지 설치 (이미 존재하면 건너뜀)
    *   `edu`, `stg`, `marts` 스키마 생성
    *   원본 테이블 구조(DDL) 생성 및 CSV 데이터 업로드
    *   dbt 실행 이력을 남길 `admin.dbt_log` 인프라 구축

> **💡 참고**: Python 3.11이 설치되어 있어야 합니다. 가상환경 생성 실패 시 `python3.11 --version`으로 확인해 주세요.

---

## 🛠️ 3단계: dbt 프로젝트 설정

본격적인 데이터 모델링을 위해 dbt 프로젝트를 초기화하고 환경을 맞춥니다.

### 4.1 프로젝트 초기화
```bash
# 1. dbt 프로젝트 생성 (edu001)
cd dbt_projects
dbt init edu001 --skip-profile-setup
cd ..

# 2. 불필요한 예제 모델 삭제
rm -rf dbt_projects/edu001/models/example

# 3. DB 접속 정보(Profile) 설정
mkdir -p dbt_projects/edu001/.dbt
cp refs/edu/profiles.yml.template dbt_projects/edu001/.dbt/profiles.yml
```

### 4.2 메타데이터 및 매크로 배치
```bash
# 1. DB의 테이블 정보를 바탕으로 schema.yml 자동 생성
##STG
python refs/edu/tools/update_schema_yml.py --schema stg --tables stg_customers,stg_order_items,stg_orders,stg_products,stg_purchase_orders,stg_purchase_orders2,stg_receipts --schema_file_path ~/projects/edu_env/dbt_projects/edu001/models/stg/schema.yml --dbconf_path refs/edu/tools/dbconf.json --db_key postgres_default --update_existing

##MARTS
python refs/edu/tools/update_schema_yml.py --schema marts --tables customer_churn_risk_mart,customer_receipt_mart,orders_customers_mart,orders_mart,orders_products_mart,orders_summary_mart,purchase_order_summary,receipt_analysis_mart --schema_file_path ~/projects/edu_env/dbt_projects/edu001/models/marts/schema.yml --dbconf_path refs/edu/tools/dbconf.json --db_key postgres_default --update_existing

# 2. 공통으로 사용할 매크로(*.sql) 복사
cp refs/edu/macros/*.sql dbt_projects/edu001/macros/
```

---

## ✍️ 4단계: 모델 개발 (핵심 실습)

이 단계부터는 직접 SQL을 작성하여 데이터를 가공합니다.

1.  **설정 변경**: `dbt_projects/edu001/dbt_project.yml` 파일에서 `models` 섹션을 프로젝트 규칙에 맞게 수정하세요. (`refs/docs/dbt_project_rules.md` 참고)
2.  **Staging 모델 개발**: `edu` 스키마의 원본 데이터를 1:1로 가져오는 모델을 만듭니다.
3.  **Mart 모델 개발**: 비즈니스 로직에 맞게 데이터를 조인하고 가공합니다.
4.  **템플릿 활용**: `refs/models` 폴더의 템플릿 파일들을 참고하여 표준 형식을 유지하세요.

---

## 🧪 5단계: 실행 및 Airflow 연동

### 6.1 dbt 실행 및 검증
작성한 모델이 정상적으로 빌드되는지 확인합니다.

```bash
cd dbt_projects/edu001
dbt run --profiles-dir .dbt -t dev \
  --vars '{"run_mode": "manual", "data_interval_start": "2026-01-01 00:00:00", "data_interval_end": "2026-01-02 00:00:00"}'
```

---

## 🏁 마무리
모든 단계가 완료되었습니다! 이제 `http://localhost:8081`에서 Airflow DAG이 정상적으로 동작하는지 확인하고, 데이터가 `marts` 스키마에 잘 쌓였는지 조회해 보세요.

**💡 문제 발생 시**: 처음부터 다시 시작하고 싶다면 `1단계`부터 다시 실행하면 깨끗한 상태로 복구됩니다.
