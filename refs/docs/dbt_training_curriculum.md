# 2일 dbt 교육 커리큘럼

이 커리큘럼은 기존 `edu` 환경을 활용하여 환경 설정부터 고급 dbt 개념 및 프로젝트 모범 사례에 이르기까지 새로운 dbt 사용자를 위해 설계되었습니다.

## 1일차: dbt 소개 및 환경 설정

### 모듈 1: dbt란 무엇인가?
*   ELT vs ETL 소개
*   왜 dbt인가? (장점, 사용 사례)
*   핵심 개념 (모델, 소스, 테스트, 시드, 매크로, 스냅샷)

### 모듈 2: dbt 환경 설정
*   **사전 준비:** Python, Git, Docker, IDE (예: VS Code)
*   **로컬 DB 환경 구축 (Docker):**
    *   **PostgreSQL 컨테이너 실행:** `docker compose up -d postgres` 명령을 사용하여 실습용 데이터베이스를 백그라운드로 실행.
    *   **컨테이너 상태 확인:** `docker ps` 명령을 통해 Postgres 컨테이너가 정상적으로 실행 중인지 확인.
*   **`edu` 환경 설정:**
    *   **코드 배포:** GitHub에서 `edu` dbt 저장소 클론. `dbconf.json`과 같은 민감한 파일에 대한 `.gitignore`의 중요성 강조.
    *   **데이터베이스 연결:** `dbconf.json`을 사용하여 `profiles.yml`을 데이터베이스 연결용으로 구성.
    *   **스키마 및 테이블 생성:** `tools/execute_all_ddls.py`를 사용하여 `refs/ddls`의 DDL 파일로부터 필요한 스키마(예: `edu`, `stg`, `marts`) 및 원본 테이블 생성.
    *   **초기 데이터 로딩:** `tools/load_data_from_csv.py`를 활용하여 `refs/datas`의 CSV 파일로부터 `edu` 스키마 테이블로 초기 원본 데이터 로드.
    *   **dbt 프로젝트 개요:** `dbt_project.yml` 이해 (기본 구성 및 프로젝트 구조).
    *   **기본 dbt 명령어 실행:** (`dbt debug`, `dbt compile`).

### 모듈 3: 소스 및 스테이징 모델 작업
*   **소스 정의:** `sources.yml`을 사용하여 원본 데이터 소스를 정의하는 방법.
*   **스테이징 모델 리팩토링:**
    *   `stg` 모델 생성 모범 사례 (예: `stg_customer.sql`, `stg_order.sql`).
    *   **내부 생성에서 소스로:** `stg_product.sql` 및 `stg_customer.sql`이 내부적으로 데이터를 생성하는 대신 각각 `source('edu', 'raw_products')` 및 `source('edu', 'raw_customers')`에서 선택하도록 리팩토링된 방법 이해.
    *   **`refs/datas` 및 `refs/ddls`의 역할:** 이 디렉토리들이 원본 데이터 및 소스 테이블의 DDL 관리를 어떻게 용이하게 하는지 설명.
    *   `source()` 함수 활용.
    *   `GEMINI.md`의 SQL 작성 규칙 준수 (콤마 위치, 들여쓰기, 소문자 키워드 등).
    *   모델 헤더 규칙 적용.

### 모듈 4: 데이터 테스트 및 문서화 [이론 및 설명 중심 - 실습 제외]
*   **dbt 테스트 소개:**
    *   내장 테스트 개요 (unique, not_null, accepted_values, relationships).
    *   `schema.yml`에 테스트를 정의하는 방법 및 원리 설명.
*   **문서 생성:**
    *   `dbt docs generate` 및 `dbt docs serve` 기능 및 활용 사례 설명.
    *   **중요 사항:** 자동화된 테스트 또는 특정 컨텍스트에서 `dbt docs`를 실행하지 말라는 `GEMINI.md` 프로젝트 지침 강조.

## 2일차: 고급 dbt 및 마트 개발

### 모듈 5: 마트 모델 구축
*   **마트 모델 생성:**
    *   분석 마트 모델 개발 (예: `orders_customers_mart.sql`).
    *   모델 연결을 위한 `ref()` 함수 활용.
    *   `GEMINI.md`에 따라 모델 구체화 (`incremental`, `append`) 이해 및 적용.

### 모듈 6: 매크로 및 재사용성
*   **Jinja 소개:** dbt SQL 파일에서 Jinja 템플릿을 사용하는 방법.
*   **사용자 정의 매크로 생성:**
    *   매크로를 사용하여 재사용 가능한 SQL 로직 개발 (예: `get_date_intervals.sql`).
    *   `macros` 디렉토리에 매크로 저장.

### 모듈 7: 데이터 무결성 및 검증
*   **데이터 무결성 감사 도구 (`auditor.py`) - [교육과정 제외]:**
    *   데이터 무결성 감사 스크립트의 목적 및 기능.
    *   **위치 업데이트:** `auditor.py`가 이제 `tools/auditor.py`에 위치함을 명시.
    *   `GEMINI.md`의 `auditor.py` 실행 가이드라인 참조.
    *   다양한 매개변수 (모델명 자동 완성, 날짜 컬럼 추론, 전체 검사)로 `auditor.py` 실행.
    *   출력 해석: `FINAL INTEGRITY SUMMARY`, `🚨 DIFF`, `⚠️ WARNING`.
    *   데이터 불일치 디버깅 전략.
*   **새로운 데이터 모델 상세 검증 방법론:**
    *   **단순 검증의 한계:** 강력한 데이터 모델 검증에 왜 단순 `SELECT LIMIT` 쿼리가 불충분한지 논의.
    *   **키 기반 샘플링 및 비교:** 새로운 방법론(`GEMINI.md` 섹션 7.1에 자세히 설명됨) 소개. 여기에는 다음이 포함됨:
        1.  소스에서 키 기반 샘플 레코드 추출.
        2.  샘플링된 키에 대한 모델 로직을 복제하여 소스 데이터 추출.
        3.  샘플링된 키에 대한 타겟 데이터 추출.
        4.  시각적 및 논리적 검증을 위한 나란히 비교.
    *   **검증 스크립트 활용:** 이 방법론을 구현하기 위한 실용적인 도구로 `tools/verify_mart_*.py` 및 `tools/sample_data_verifier.py` 소개.
*   **기본 행 수 검증:** `tools/verify_loaded_data.py`를 사용하여 데이터베이스 테이블과 CSV 파일 간의 행 수 비교.

### 모듈 8: 프로젝트 모범 사례 및 다음 단계
*   **프로젝트 구성:** dbt 프로젝트 구조 및 명명 규칙 모범 사례.
*   **버전 관리:** Git을 이용한 dbt 프로젝트 통합.
*   **dbt를 이용한 CI/CD (개요):** CI/CD 파이프라인에서 dbt 실행 및 테스트 자동화에 대한 간략한 논의.
*   **깨끗한 환경 관리:** 테스트/개발 환경 재설정을 위한 `tools/manage_schemas_for_test.py` 사용 강조.
*   **문제 해결:** 일반적인 dbt 문제 및 해결 방법.
*   **Q&A 및 자유 토론:** 특정 질문에 답변하고 협업 학습 장려.