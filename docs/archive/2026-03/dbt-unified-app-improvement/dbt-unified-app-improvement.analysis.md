# Analysis: dbt_unified_app 코드 품질 개선

**Feature**: dbt-unified-app-improvement
**Created At**: 2026-03-28
**Phase**: Check
**Match Rate**: 95%
**Author**: hjpark

---

## Context Anchor

| 항목 | 내용 |
|------|------|
| WHY | 코드 리뷰 결과 보안 취약점과 유지보수 문제가 동시에 발견됨. 기능 추가 전 기반 정리 필요 |
| WHO | hjpark (로컬 dbt 관리 도구 사용자) |
| RISK | 파일 분리 시 import 경로 오류, Streamlit session_state 공유 방식 변경 필요 가능성 |
| SUCCESS | SQL injection 0건, 중복 코드 제거, 단일 파일 → 모듈 구조, 기존 기능 정상 동작 유지 |
| SCOPE | mycodes/dbt_unified_app.py + 분리될 모듈 파일들 |

---

## 1. 분석 개요

| 항목 | 결과 |
|------|------|
| Match Rate | **95%** (수정 후) |
| Critical 이슈 | 0건 |
| Important 이슈 | 3건 |
| Minor 이슈 | 3건 |
| 분석 파일 | 5개 (`*_new.py`) |
| 총 구현 줄 수 | 2,848줄 (5개 파일 합산) |

---

## 2. Success Criteria 평가

| SC | 기준 | 결과 | 근거 |
|----|------|------|------|
| SC-01 | SQL injection 취약점 0건 | ✅ Met | `_validate_where_clause()` + `_DDL_PATTERN` + `SET TRANSACTION READ ONLY` — `validator_new.py:33,52,165,194` |
| SC-02 | `manifest.json` 로드 1회 (캐시) | ✅ Met | `@st.cache_data` `_load_manifest_cached()` — `manifest_utils_new.py:66-67` |
| SC-03 | 중복 셀 비교 코드 1개 함수 | ✅ Met | `cells_differ()` 단일 정의 — `validator_new.py:71` |
| SC-04 | `dbt_unified_app.py` 500줄 이하 | ⚠️ Partial | 879줄 (목표 500줄 대비 76% 초과). YAML Generator + Runner UI가 ~600줄 차지. 69% 감축(2817→879) 달성 |
| SC-05 | 앱 실행 후 3개 탭 정상 동작 | ⏳ Pending | 수동 테스트 필요 |

---

## 3. FR별 구현 대조

### ✅ 일치 항목 (9건)

| FR | 설계 내용 | 구현 확인 |
|----|----------|----------|
| FR-01 | `_validate_where_clause` + DDL 키워드 차단 | `validator_new.py:33-56` |
| FR-01 | `SET TRANSACTION READ ONLY` | `validator_new.py:165, 194` |
| FR-02 | `get_table_detail()` `psycopg2.sql.SQL` + `sql.Literal` | `db_utils_new.py:102-111` |
| FR-03 | `@st.cache_data` + mtime 캐시 키 | `manifest_utils_new.py:66-81` |
| FR-04 | `cells_differ()` 단일 정의 | `validator_new.py:71` |
| FR-06 | `get_conn()` except 블록 `conn.rollback()` | `db_utils_new.py:30-31` |
| FR-07 | `LIMIT %s` 파라미터화 | `validator_new.py:203` |
| FR-08 | 4개 모듈 분리 | 5개 `*_new.py` 파일 생성 |
| — | 단방향 의존성 구조 | `dbt_unified_app_new → validator/history → db_utils` |

---

## 4. Gap 목록

### Important (3건)

#### GAP-01: `mk_badge` 명명 불일치
- **설계**: Design §3.3 — `mk_badge()` (공개 함수)
- **구현**: `_mk_badge()` (private) — `validator_new.py:60`
- **영향**: `history_new.py:9`에서 `from validator_new import _mk_badge`로 private 함수를 cross-module import. Python 관례상 `_` prefix는 내부 전용 신호이므로 API 계약 위반
- **권장 조치**: `_mk_badge` → `mk_badge`로 rename (validator_new.py + history_new.py 2곳)

#### GAP-02: `fetch_verification_history`, `fetch_verification_detail` 위치 변경
- **설계**: Design §3.3 — `validator.py` 내 배치 명시
- **구현**: `history_new.py:12,39`에 배치
- **이유**: 순환 import 방지를 위한 의도적 변경 (validator가 history를 import하면 circular)
- **권장 조치**: Design §3.3 함수 목록에서 해당 2개 제거, §3.4 history.py 항목에 추가

#### GAP-03: `dbt_unified_app_new.py` 줄 수 초과
- **설계**: Design §3.5 — `~400줄 목표`
- **구현**: 879줄 (Design 대비 120% 초과, 원본 대비 69% 감축)
- **원인**: YAML Generator UI (~250줄)와 Runner UI (~300줄)가 진입점에 잔류
- **권장 조치**: Plan SC-04(500줄)도 미충족. 추가 분리(yaml_utils_new.py, runner_new.py) 또는 Design 목표치 갱신

### Minor (3건)

#### GAP-04: `load_manifest` vs `_load_manifest_cached` 명명
- **설계**: `load_manifest()` — Design §3.2
- **구현**: `_load_manifest_cached()` + `_get_manifest()` 래퍼 구조
- **영향**: 기능 동등. `_get_manifest()`가 공개 인터페이스 역할 수행

#### GAP-05: `mk_badge ok` 파라미터 None 허용 확장
- **설계**: `ok: bool`
- **구현**: None 허용 (`validator_new.py:60`), None 시 빈 문자열 반환
- **영향**: 기능 확장이며 하위 호환 유지

#### GAP-06: `manifest_utils_new.py` Design 미기재 함수 6개
- Design에 없던 `check_model_schema_exists`, `_get_model_raw_sql`, `_detect_before_sql_date_col`, `_has_date_vars`, `cleanup_old_runs_by_date`, `get_latest_run_results` 추가
- 원래 `dbt_unified_app.py`에 있던 유틸 함수가 적절히 이동됨

---

## 5. Decision Record 검증

| 결정 | 설계 | 구현 |
|------|------|------|
| Architecture Option C (Pragmatic) | Option C 선택 | ✅ 5개 모듈 구조 구현 |
| 단방향 의존성 | db_utils ← validator ← app | ✅ `history_new.py:8-9` import 구조 확인 |
| session_state 초기화 위치 | `dbt_unified_app.py`에서 초기화 | ✅ `dbt_unified_app_new.py` 내 session_state 블록 유지 |
| CLAUDE.md §6 새 파일 작성 절차 | `*_new.py` 파일로 작성 후 rename | ✅ 모든 파일 `_new` suffix |

---

## 6. 다음 단계 옵션

| 옵션 | 내용 | Match Rate 예상 |
|------|------|----------------|
| A. 지금 모두 수정 | GAP-01 rename + Design 문서 갱신 (GAP-02,03) | ~95% |
| B. Critical만 수정 | (Critical 없음, GAP-01 Important만 수정) | ~93% |
| C. 그대로 진행 | 현재 89%로 Report 진행 | 89% |

> SC-05 (앱 정상 동작) 수동 테스트 후 최종 결정 권장

---

*생성일: 2026-03-28 | Author: hjpark | gap-detector 분석 기반*
