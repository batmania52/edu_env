# Plan: dbt_unified_app 코드 품질 개선

**Feature**: dbt-unified-app-improvement
**Created At**: 2026-03-28
**Phase**: Plan
**Author**: hjpark

---

## Executive Summary

| 항목 | 내용 |
|------|------|
| Feature | dbt_unified_app 코드 품질 개선 |
| 시작일 | 2026-03-28 |
| 예상 완료 | 2026-03-29 |
| 기간 | 1-2일 |

### 1.3 Value Delivered (4-Perspective)

| 관점 | 내용 |
|------|------|
| Problem | 코드 리뷰(Score 58/100)에서 SQL injection 2건·Major 5건·2817줄 단일 파일 확인 |
| Solution | 보안 패치 + 중복 코드 제거 + 파일 모듈화로 유지보수성 확보 |
| Function UX Effect | SQL injection 차단으로 DB 안전성 향상, 모듈화로 기능 추가 속도 개선 |
| Core Value | 로컬 개발 도구의 안전성·확장성 기반 확립 |

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

## 1. 배경 및 목적

`mycodes/dbt_unified_app.py`는 dbt 모델 실행·검증·이력 관리를 하나의 Streamlit 앱으로 제공하는 핵심 도구다.
2026-03-27 기능 추가 이후 코드 리뷰(Score 58/100) 결과 아래 문제가 확인되었다:

- **보안**: SQL injection 취약점 2건 (직접 SQL 문자열 보간)
- **성능**: `manifest.json` 4회 중복 로드
- **유지보수**: 동일 로직 4-5곳 중복, 2817줄 단일 파일

---

## 2. 요구사항

### 2.1 기능 요구사항

| ID | 요구사항 | 우선순위 |
|----|----------|---------|
| FR-01 | `where_clause` 직접 보간 SQL에 read-only 트랜잭션 적용 및 DDL 키워드 차단 | Critical |
| FR-02 | `get_table_detail` 내 f-string 보간을 `psycopg2.sql` 모듈로 교체 | Critical |
| FR-03 | `manifest.json` 로드를 1회로 통합 (`@st.cache_data` 활용) | Major |
| FR-04 | 셀 비교 로직(`cells_differ`) 공통 함수로 추출 (4곳 → 1곳) | Major |
| FR-05 | `_mk_badge`/`_det_badge` 중복 함수 모듈 레벨로 통합 | Major |
| FR-06 | `get_conn` 컨텍스트 매니저에 예외 시 `conn.rollback()` 추가 | Major |
| FR-07 | `LIMIT {limit}` f-string을 파라미터화 쿼리로 변경 | Major |
| FR-08 | `dbt_unified_app.py`를 기능별 모듈로 분리 | Major (구조) |

### 2.2 비기능 요구사항

| ID | 요구사항 |
|----|----------|
| NFR-01 | 기존 기능 모두 정상 동작 유지 (검증탭, 실행탭, 이력탭) |
| NFR-02 | Streamlit session_state 공유 방식 유지 |
| NFR-03 | 분리 후 앱 시작 시간 현재 대비 유지 또는 개선 |

---

## 3. 범위

### 포함 (In-Scope)

- FR-01 ~ FR-08 전체
- 파일 모듈화: `db_utils.py`, `manifest_utils.py`, `validator.py`, `history.py`
- `dbt_unified_app.py` → 진입점(entry point)으로 슬림화

### 제외 (Out-of-Scope)

- Minor 이슈 (regex 개선, `ThreadedConnectionPool` 전환, 사이클 감지)
- UI/UX 변경
- 신규 기능 추가

---

## 4. 성공 기준 (Success Criteria)

| SC | 기준 | 측정 방법 |
|----|------|----------|
| SC-01 | SQL injection 취약점 0건 | 코드 리뷰 재검증 |
| SC-02 | `manifest.json` 로드 호출 1회 (캐시 적용) | 코드 grep 확인 |
| SC-03 | 중복 셀 비교 코드 1개 함수로 통합 | 코드 grep 확인 |
| SC-04 | `dbt_unified_app.py` 500줄 이하 (진입점) | wc -l |
| SC-05 | 앱 실행 후 기존 3개 탭 정상 동작 | 수동 확인 |

---

## 5. 모듈 분리 설계 (초안)

```
mycodes/
├── dbt_unified_app.py        ← 진입점 (~300줄 목표)
├── db_utils.py               ← DB 연결풀, get_conn, get_table_detail
├── manifest_utils.py         ← manifest.json 캐시 로드, lineage
├── validator.py              ← val_* 함수, cells_differ, render_validation_ui
└── history.py                ← render_history_ui, fetch_* 함수
```

---

## 6. 리스크

| 리스크 | 가능성 | 영향 | 대응 |
|--------|--------|------|------|
| 모듈 분리 시 순환 import | 중 | 앱 시작 불가 | 의존성 방향을 단방향으로 설계 |
| session_state 접근 실패 | 중 | 런타임 오류 | 공유 상태는 dbt_unified_app.py에서 초기화 후 전달 |
| Python 규칙 위반 (원본 직접 수정) | 낮 | 롤백 불가 | CLAUDE.md §6 준수 — 새 파일 작성 후 테스트, 확인 후 rename |

---

## 7. 구현 순서

1. **FR-01, FR-02** — SQL injection 패치 (보안 우선)
2. **FR-06, FR-07** — 커넥션/쿼리 안전성 패치
3. **FR-03** — manifest.json 캐시 통합
4. **FR-04, FR-05** — 중복 코드 제거
5. **FR-08** — 파일 모듈화 (위 패치 완료 후 진행)

---

*생성일: 2026-03-28 | Author: hjpark*
