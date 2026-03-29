# dbt Unified Dashboard — 사용 가이드

> **파일**: `mycodes/dbt_unified_app.py`
> **실행**: `streamlit run mycodes/dbt_unified_app.py`

---

## 개요

dbt 모델 개발 라이프사이클 전체를 하나의 화면에서 처리하는 통합 대시보드.

| 탭 | 역할 |
|----|------|
| 🚀 dbt Runner | 모델 실행, Run 범위 설정, Lineage 시각화 및 상세 정보 조회 |
| 📝 YAML Generator | DB 테이블 → schema.yml 생성 및 기존 설정과의 차이(Diff) 분석 |
| 🔍 데이터 검증 | 소스/타겟 COUNT·SUM·샘플 비교 |
| 📋 이력 | 검증 이력 + dbt 실행 로그 조회 |

---

## 사이드바

앱 전체에 공통 적용되는 설정.

| 항목 | 설명 |
|------|------|
| Project Directory | dbt 프로젝트 경로 |
| Profiles Directory | `.dbt` 폴더 경로 (기본값: `~/.dbt`) |
| 설정 저장 | `.dbt_unified_cache.json`에 경로 캐싱 |
| 🎯 Target 선택 | `profiles.yml` outputs 목록 — DB 접속 정보 결정 |
| 🔄 초기화 | 전체 세션 초기화 |

---

## TAB 1 — dbt Runner

### 흐름

```
모델 선택 → (Run 범위 설정) → (Lineage 분석) → 상세 정보 확인 → 날짜 선택 → Compile → Run → 검증 실행
```

### 모델/모드 선택

- **그룹 → 모델** 순으로 계층 선택 (models 디렉토리 구조 기반)
- **모드**: `manual` / `schedule` 선택
- **🔄 초기화** 버튼: 모델/날짜/Lineage/Run 설정 전체 초기화

### Run 범위 설정

Lineage 시각화와 독립적으로 dbt run의 `--select` / `--exclude` 범위를 설정.

| 컨트롤 | 설명 |
|--------|------|
| ⬆️ Upstream 체크박스 | 체크 시 선택 모델의 upstream을 run 범위에 포함 (+depth) |
| ⬇️ Downstream 체크박스 | 체크 시 선택 모델의 downstream을 run 범위에 포함 (depth+) |
| 🚫 Exclude 모델 | run 범위에서 제외할 모델 멀티셀렉트 |

### 🧬 Lineage 시각화 및 모델 상세 정보

단순 관계 표시를 넘어 모델의 내부 정보를 즉시 확인할 수 있는 기능을 제공합니다.

1.  **Lineage 분석**: `target/manifest.json`을 분석하여 전후방 의존 관계를 그래프로 렌더링.
2.  **모델 클릭 (상세 팝업)**: 시각화 패널의 모델/소스 버튼 클릭 시 팝업 실행.
    - **📄 SQL**: 현재 모델의 원본 SQL 소스 코드.
    - **📝 YAML Metadata**: `schema.yml`에 정의된 설명, 테스트, 메타데이터.
    - **📦 Dependencies**: 해당 노드가 의존하는 부모 노드 목록.
    - **🔍 Focus**: 해당 모델을 중심으로 Lineage 재분석.
3.  **소스(Source) 구분**: 🔌 아이콘과 함께 표시되며, DB 원본 테이블 정보 확인 가능.

---

## TAB 2 — YAML Generator (Enhanced)

### 분석 및 Diff 기능

단순 생성을 넘어 기존 설정과의 정합성을 체크합니다.

1.  **🔍 분석 시작**: DB의 최신 스키마 정보와 현재 `schema.yml` 설정을 비교.
2.  **상태 구분**:
    - 🔵 **NEW**: YAML에 정의되지 않은 신규 테이블.
    - 🟠 **CHANGED**: 기존 정의가 있으나 컬럼 주석, 데이터 타입, PK 정보 등이 DB와 다른 경우.
    - 🟢 **SAME**: DB 정보와 YAML 정의가 완전히 일치함.
3.  **🔍 Diff 보기**: CHANGED 상태인 모델의 'Diff' 버튼 클릭 시, **기존 설정 vs 신규 분석 결과**를 나란히 비교하여 변경된 컬럼만 하이라이트.

### 적용 및 백업

- **저장 경로**: 기존 파일 유지 또는 신규 경로 지정 가능.
- **🚀 일괄 적용**: 
  - 실행 시 기존 파일은 `models/bak/`에 자동 백업.
  - 여러 파일에 중복 정의된 모델을 자동으로 감지하여 한 곳으로 통합 정리.

---

## TAB 3 — 데이터 검증

### 날짜 필터 및 before_sql 제어

1.  **타겟 날짜 조건**: `before_sql`의 `DELETE` 구문에서 날짜 컬럼을 자동 추출하여 권장 조건으로 제시.
2.  **Compiled SQL 검토**: 컴파일된 소스 쿼리를 직접 확인하고, 검증 시 `before_sql` 실행 여부를 구문별(DELETE, CREATE TEMP 등)로 제어.
3.  **샘플 데이터 비교**: 소스와 타겟의 데이터를 셀 단위로 비교하여 불일치 항목을 주황색으로 강조.

---

## TAB 4 — 이력 및 연동

- **검증 이력**: 과거 검증 결과의 요약 및 상세 쿼리/데이터 재조회.
- **실행 로그**: `admin.dbt_log` 연동. 특정 실행 행 클릭 시 해당 시점의 날짜 조건으로 **'즉시 검증'** 팝업 연동.

---

## admin 스키마 테이블 (이력 저장용)

| 테이블 | 용도 |
|--------|------|
| `admin.verification_summary` | 검증 실행 메타 (모델명/날짜/PASS·FAIL) |
| `admin.verification_count` | COUNT 검증 상세 결과 |
| `admin.verification_sum` | 숫자 컬럼 SUM 비교 상세 |
| `admin.verification_sample` | 샘플 데이터 비교 상세 (JSONB 저장) |
| `admin.dbt_log` | dbt pre-hook을 통한 모델 실행 로그 |

---

*최초 작성: 2026-03-27 | 최종 업데이트: 2026-03-29*
*주요 변경: Lineage 모델 상세 정보 팝업, YAML Meta Diff 분석 기능 추가*
