# 💡 Project Instructions

# DBT 프로젝트 개발 가이드라인

이 문서는 DBT 프로젝트 개발 시 준수해야 할 규칙과 지침을 설명합니다.

*   [1. 일반 규칙 및 SQL 작성 규칙](refs/docs/dbt_project_rules.md)
*   [2. 표준 프로젝트 디렉토리 구조](#2-표준-프로젝트-디렉토리-구조)
*   [3. 모델 개발 규칙](#3-모델-개발-규칙)
    *   [3.1. 모델 헤더 규칙](refs/docs/model_header_rules.md)
    *   [3.2. SQL 작성 규칙](refs/docs/dbt_project_rules.md)
*   [4. 모델 실행 규칙](refs/docs/model_execution_rules.md)
*   [5. Schema 파일 설정 지침](refs/docs/schema_file_config.md)
*   [6. 🛠️ Data Integrity Auditor 실행 지침](refs/docs/auditor_guidelines.md)
*   [7. 데이터 모델 상세 검증 지침](refs/docs/verification_guidelines.md)
*   **9. Python 코딩 지침**
    *   [Python 코딩 지침](refs/docs/python_guidelines.md)
*   **8. Gemini CLI 모델 기능**
    *   Gemini 2.5 Flash 모델은 멀티모달 기능을 활용하여 이미지 파일을 직접 분석하고 텍스트를 추출할 수 있습니다.


# Global Settings
- 이 파일은 특정 프로젝트 외에 공통으로 사용하는 기본 설정입니다.
- AI가 수행하는 모든 과정과 결과는 한국어로 출력한다.
## 1. 기본 규칙
*  1. 파일을 수정하거나 참조해야 할 경우 기억에 우선하지 말고 파일을 먼저 확인 할것
## 2. 표준 프로젝트 디렉토리 구조

```text
. # 프로젝트 루트 디렉토리
└── refs/ # 참조 영역 (DDL, 데이터, 문서, Python 스크립트 등)
    ├── docs/ # 교육 커리큘럼 및 문서
    ├── edu/ # 원본 dbt 프로젝트 (템플릿)
    │   ├── airflow_assets/ # Airflow 관련 에셋 (DAGs, plugins)
    │   │   ├── dags/ # Airflow DAG 파일
    │   │   └── plugins/ # Airflow 플러그인
    │   ├── datas/ # 추출된 원본 CSV 데이터 파일
    │   ├── ddls/ # 테이블 DDL 스크립트
    │   ├── docker_setup/ # Docker 환경 설정 파일
    │   │   ├── config/ # Airflow 구성 파일
    │   │   ├── dags/ # Airflow DAG 파일
    │   │   └── logs/ # 로그 파일
    │   │       ├── dag_processor_manager/
    │   │       └── scheduler/
    │   │           ├── 2026-03-20/
    │   │           └── latest/
    │   ├── infra/ # 인프라 관련 파일
    │   ├── macros/ # 사용자 정의 dbt 매크로
    │   ├── models/ # dbt 모델
    │   │   ├── marts/
    │   │   └── stg/
    │   └── tools/ # 프로젝트 관리 및 자동화를 위한 Python 스크립트 (데이터 로드/추출/검증 등)
    └── models/
``````