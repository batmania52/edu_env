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
## 3. 실행 결과 보고 원칙 (엄격 준수)
*   **실행 전 결과 보고 금지**: 모든 도구(run_command, replace_file_content 등)의 실행 결과는 도구 호출이 완료된 후, 출력된 텍스트 그대로를 사용하여 보고한다. 
*   **가상 데이터 생성 금지**: 조회 결과가 나오기 전까지 AI가 임의로 "성공"이나 수치(건수 등)를 조작하여 보고하는 것을 엄격히 금지한다.
*   **증명 기반 소통**: 도구 실행 완료 시점에 실제 터미널 출력(Output) 또는 조회 결과를 가감 없이 보여줌으로써 실제 수행 여부를 사용자에게 증명한다.

## ⚠️ 실행 정합성 및 보고 원칙 (최우선 준수)
*   **추측 금지 및 가짜 로그 생성 금지**: 절대로 터미널 실행 결과를 추측하여 답변하거나 가짜 로그를 생성(Hallucination)하지 마십시오. 모든 명령어는 반드시 터미널 도구를 통해 실제로 실행한 후 그 출력값(stdout)만 보고하십시오.
*   **실측 결과만 보고**: 모든 명령어는 반드시 터미널 도구(run_command, command_status)를 통해 실제로 실행한 후, 그로부터 얻은 **공식 출력값(stdout)**만을 가공 없이 보고하십시오.
*   **가상 데이터 수치 언급 금지**: 실제 DB 조회나 dbt 실행 결과가 나오기 전까지는 "성공", "150건" 등의 예측 수치를 답변에 포함하지 마십시오.

## 4. 즉각적인 도구 호출 원칙 (Run-First)
*   **서론 배제**: run_command나 replace 등 사용자 승인이 필요한 도구를 호출할 때는, 대화창에서 "호출하겠습니다" 등의 서론을 생략한다.
*   **즉시 호출**: 명령어가 결정되면 답변 본문보다 도구(Tool Call)를 먼저 호출하여 사용자 화면에 [Run] 버튼이 즉시 나타나게 한다.
*   **승인 대기**: 답변은 도구 호출과 동시에 간결하게 작성하며, 사용자의 승인 클릭을 최우선으로 기다린다.

## ⚠️ 무한 반복 루프 방지 및 즉각 호출 (엄격 준수)
*   **서술어형 인자 금지**: toolAction 등 도구 인자에 서술형 문구 사용을 절대 금지한다.
*   **명사형/영문 인자 사용**: dbt run, Checking logs 등 짧은 명사 또는 영문만 사용하여 시스템 루프를 차단한다.
*   **Run-First (버튼 우선)**: 긴 설명보다 도구 호출(Tool Call)을 먼저 수행하여 사용자가 [Run] 버튼을 즉시 누를 수 있게 한다.
