# dbt Unified Dashboard — 사용 가이드

> **파일**: `mycodes/dbt_unified_app.py`
> **실행**: `streamlit run mycodes/dbt_unified_app.py`

---

## 개요

dbt 모델 개발 라이프사이클 전체를 하나의 화면에서 처리하는 통합 대시보드.

| 탭 | 역할 |
|----|------|
| 🚀 dbt Runner | 모델 실행 및 Lineage 분석 |
| 📝 YAML Generator | DB 테이블 → schema.yml 자동 생성 |
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

> 이력 탭 레이블에 `(미설정)` 이 붙으면 `admin` 스키마 테이블이 없는 상태.
> 해당 DDL을 먼저 실행해야 이력 저장이 활성화됨.

---

## TAB 1 — dbt Runner

### 흐름

```
모델 선택 → (Lineage 분석) → 날짜 선택 → Compile → Command Review → Run → 검증 실행
```

### 모델/모드 선택

- **그룹 → 모델** 순으로 계층 선택 (models 디렉토리 구조 기반)
- **모드**: `manual` / `schedule` 선택
- 선택된 모델의 schema.yml 정의가 없으면 오류 표시 + YAML Generator 안내

### Lineage 분석

- **업/다운스트림 Depth** ➕➖ 버튼으로 조정
- **🧬 Lineage 분석** 클릭 → `target/manifest.json` 파싱
  - 업스트림: 의존 모델/소스 계층 표시
  - 다운스트림: 현재 모델을 참조하는 모델 계층 표시
  - 각 모델 버튼 클릭 → 해당 모델로 자동 이동
  - 소스(Source)는 클릭 불가

### 날짜 선택

- **시작**: 기본값 D-4 / **종료**: 기본값 D-1
- dbt compile 시 `data_interval_start / data_interval_end` vars로 전달
- 종료 < 시작이면 실행 버튼 비활성화

### Compile

- **DBT Compile** 클릭 → `dbt compile` 실행
  - 성공 시: compiled SQL 및 before_sql 추출/표시
  - before_sql 구문 유형별 표시 (🗑️ DELETE/TRUNCATE, 📄 기타)

### Run

1. **🔍 Command Review** — 실행될 dbt run 명령어 미리 확인
2. **▶️ Run Execution** — Command Review 후에만 활성화
   - 성공 시 실행 결과 테이블 표시 (모델명, 상태, 적재 건수, 실행 시간)
   - 결과는 rerun 후에도 유지됨

### 검증 연동

- Run 성공 후 **🔍 검증 실행** 버튼 표시
- 클릭 시 검증 탭 팝업이 해당 모델/날짜 정보로 자동 채워짐

---

## TAB 2 — YAML Generator

### 흐름

```
스키마 선택 → 테이블 선택 → 분석 시작 → 결과 검토 → 일괄 적용
```

### 스키마/테이블 선택

- DB에서 사용자 스키마 목록 자동 로드
- 테이블 multiselect로 여러 테이블 동시 분석 가능

### Runner 이관

- Runner에서 schema.yml 없는 모델이 감지되면 배너 표시
- **📥 이력 가져오기** — DB에서 모델명으로 스키마 자동 탐색 후 선택 상태로 이동

### 분석 및 결과

- **🔍 분석 시작** — 선택 테이블마다 컬럼 정보·PK·테이블 주석 수집
  - 🔵 신규 / 🟠 존재함 (기존 파일 경로 표시) / 🟢 반영됨 구분
  - "내용 동일" 표시 시 적용 체크박스 기본값 OFF

### 적용

- 각 모델마다 저장 경로 선택 가능 (기존 파일 or 신규 경로 직접 입력)
- **📄 YAML 미리보기** — 적용 전 내용 확인
- **🚀 일괄 적용**
  - 기존 파일 자동 백업 (`models/bak/` 경로, 타임스탬프 포함)
  - 다른 파일에 중복 정의된 모델 자동 제거
  - 30일 이상 된 백업 파일 자동 정리

**생성되는 YAML 포맷:**

```yaml
name: table_name
description: 테이블 주석
config:
  materialized: incremental
  incremental_strategy: append
  unique_key: pk_column
columns:
  - name: col_name
    description: 컬럼 주석
    data_type: varchar(50)
```

---

## TAB 3 — 데이터 검증

### 흐름

```
모델/스키마 선택 → 날짜 범위 → 검증 항목 → 날짜 필터 → 샘플 제외 컬럼
→ ⚙️ Compile → (compiled SQL 확인 + before_sql 체크박스) → ▶️ 검증 실행
```

### 1. 모델 및 타겟 스키마 선택

- 모델 선택 시 DB에서 동일명 테이블의 스키마를 자동 탐색
  - 후보 1개: 자동 선택
  - 후보 2개 이상: 직접 선택 경고
  - 후보 없음: 테이블 미존재 오류

### 2. 날짜 범위

- dbt compile vars에 전달될 날짜 설정 (기본값 D-4 ~ D-1)

### 3. 검증 항목

| 항목 | 설명 |
|------|------|
| 행 수 (COUNT) | 소스 쿼리 결과 건수 vs 타겟 테이블 건수 비교 |
| 숫자 컬럼 SUM | 숫자 타입 컬럼 전체 합계 비교 |
| 샘플 데이터 비교 | 소스 샘플 N건을 타겟에서 조회하여 셀 단위 비교 |

- 샘플 건수: 5~100행 슬라이더 설정 (기본값 5)

### 4. 타겟 날짜 조건

COUNT/SUM 검증 시 타겟 테이블에 적용할 WHERE 절 설정.

| 옵션 | 동작 |
|------|------|
| (조건 없음) | 전체 데이터 기준 |
| ⚡ `컬럼명` | before_sql DELETE 문에서 감지된 날짜 컬럼 (권장) |
| 날짜/타임스탬프 컬럼 | `BETWEEN '시작 00:00:00'::timestamp AND '종료 23:59:59'::timestamp` 자동 생성 |
| 비날짜 컬럼 | 사용자 정의 조건 체크박스 활성화 → 시작/종료 값 직접 입력 |
| ✏️ 직접 입력 | WHERE 절 전체를 자유 입력 (서브쿼리 패턴 등 복잡한 조건 시 사용) |

- **✅ 쿼리에 반영** 클릭 시 조건이 확정되어 검증 실행 버튼이 활성화됨
- 직접 입력 시 macOS 스마트 따옴표(` ' ' `)를 직선 따옴표(`'`)로 자동 정규화

> `{{start}}/{{end}}`가 있는 모델은 **반드시 날짜 조건을 반영한 후** 검증 실행 가능.

### 5. 샘플 비교 제외 컬럼

- 샘플 비교에서 제외할 컬럼 선택 (기본값: `dbt_dtm`)
- dbt 적재 시각처럼 소스/타겟이 항상 다른 컬럼 제외 시 활용

### Compile

- **⚙️ Compile** — `dbt compile`로 날짜 vars 반영된 SQL 생성
  - 완료 후 compiled SQL expander에서 확인 가능

### before_sql 체크박스

compiled SQL 보기 expander 내에 표시.

| 구문 유형 | 기본값 | 설명 |
|-----------|--------|------|
| DELETE / TRUNCATE | ☐ 미체크 | 데이터 삭제 위험 — 기본 비활성 |
| 기타 SQL (CREATE TEMP TABLE 등) | ☑ 체크 | 검증 전 실행 — temp table 생성 등 |

- 체크된 구문은 검증 실행 직전 **단일 DB 커넥션**에서 먼저 실행
- 이후 COUNT/SUM/샘플 쿼리는 동일 커넥션 사용 → **temp table 공유 가능**

### 검증 실행 결과

**상단 요약:**
- 검증 시각 / 모델명 / 타겟 스키마 / 기간
- 상태 배지: `전체` `COUNT` `SUM` `SAMPLE` PASS/FAIL 색상 구분

**COUNT 결과:**
- 소스 건수 / 타겟 건수 delta 메트릭
- 🔍 실행 쿼리 보기 (실행된 before_sql + 소스/타겟 SQL)

**SUM 결과:**
- 컬럼별 소스/타겟 합계 + 차이 테이블
- CSV 다운로드

**샘플 비교:**
- 소스 DataFrame / 타겟 DataFrame 나란히 표시
- 불일치 셀 주황색 하이라이트
- 불일치 상세 테이블
- 소스+타겟 합본 CSV 다운로드

**💾 검증결과 저장:**
- `admin.verification_summary` + `count/sum/sample` 상세 테이블에 저장
- 저장 완료 시 UUID 표시

---

## TAB 4 — 이력

### 공통 필터

모델명(빈칸=전체) / 시작일 / 종료일 / 조회 버튼

### 🔍 검증 이력 탭

- `admin.verification_summary` 조회 (최대 200건)
- 표시: 실행 시각 / 모델명 / 기간 / COUNT·SUM·SAMPLE 상태 (✅/❌)

**행 클릭 시 상세 조회:**

| 섹션 | 내용 |
|------|------|
| COUNT 상세 | 소스/타겟 건수 메트릭 + 쿼리 보기 |
| SUM 상세 | 컬럼별 소스/타겟 SUM 테이블 + 쿼리 보기 |
| SAMPLE 상세 | 소스/타겟 샘플 DataFrame + 쿼리 보기 |

### 📊 실행 로그 탭

- `admin.dbt_log` 조회 (최대 200건)
- 표시: invocation_id / 모델명 / 상태 / 실행 시각 / 실행 시간 / 적재 건수

**행 클릭 시:**
- Variables JSON 표시 (data_interval_start/end 포함)
- **🔍 검증 탭으로** 버튼 — 해당 로그의 모델명과 날짜로 검증 탭 팝업 자동 실행

---

## 탭 간 연동

```
Runner ──[schema 없음]──→ YAML Generator  (runner_to_gen)
Runner ──[run 성공]─────→ 검증 탭 팝업   (runner_to_val)
실행 로그 ──[행 선택]───→ 검증 탭 팝업   (runner_to_val)
```

- 검증 탭 팝업은 `st.dialog()`로 렌더링되며, 위젯 key 버전을 `+1000` 으로 분리하여 검증 탭 자체와 충돌 방지
- 팝업 닫힘 감지 시 검증 결과 자동 초기화

---

## admin 스키마 테이블 (이력 저장용)

| 테이블 | 용도 |
|--------|------|
| `admin.verification_summary` | 검증 실행 메타 (모델명/날짜/PASS·FAIL) |
| `admin.verification_count` | COUNT 검증 상세 |
| `admin.verification_sum` | SUM 검증 상세 |
| `admin.verification_sample` | SAMPLE 검증 상세 |
| `admin.dbt_log` | dbt 실행 로그 (pre-hook 기록) |

> DDL 파일: `refs/edu/ddls/admin_*.sql`
