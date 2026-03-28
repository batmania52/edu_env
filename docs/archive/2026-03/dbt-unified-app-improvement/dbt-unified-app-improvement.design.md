# Design: dbt_unified_app 코드 품질 개선

**Feature**: dbt-unified-app-improvement
**Created At**: 2026-03-28
**Phase**: Design
**Architecture**: Option C — Pragmatic
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

## 1. 개요

### 1.1 선택된 아키텍처

**Option C — Pragmatic**: 4개 모듈 분리 + 보안 패치 통합

### 1.2 아키텍처 이유

- FR-08 (모듈화)와 FR-01~07 (패치)를 동시에 진행하여 작업 횟수 최소화
- 단방향 의존성(`db_utils ← validator ← app`)으로 순환 import 방지
- `session_state`는 `dbt_unified_app.py`에서 초기화, 하위 모듈은 인자로 수령
- `cells_differ`, `_mk_badge` 공통 함수는 `validator.py` 모듈 레벨에 배치 (별도 utils.py 불필요)

---

## 2. 모듈 구조

```
mycodes/
├── dbt_unified_app.py     ← 진입점: 캐시, YAML 유틸, dbt 러너, 탭 라우팅 (~400줄)
├── db_utils.py            ← DB 연결풀, 커넥션 관리, 스키마/테이블 조회
├── manifest_utils.py      ← manifest.json 캐시 로드, SQL 치환, lineage
├── validator.py           ← 검증 로직 전체 + render_validation_ui
└── history.py             ← 이력 조회 + render_history_ui
```

### 의존성 방향 (단방향)

```
dbt_unified_app.py
    ↓ import
    ├── db_utils.py        (의존성 없음 — 최하위)
    ├── manifest_utils.py  (의존성 없음 — 최하위)
    ├── validator.py       → imports db_utils
    └── history.py         → imports db_utils
```

---

## 3. 각 모듈 상세 설계

### 3.1 db_utils.py

**책임**: DB 연결 풀 관리, 커넥션 대여/반납, 스키마·테이블·컬럼 조회

**포함 함수**:
| 함수 | 원본 위치 | 변경사항 |
|------|----------|---------|
| `_make_pool()` | L46 | 동일 |
| `get_conn()` | L54 | **FR-06**: `finally`에 `conn.rollback()` 추가 |
| `get_db_config()` | L66 | 동일 |
| `check_history_tables_exist()` | L86 | 동일 |
| `get_schemas()` | L102 | 동일 |
| `get_db_tables()` | L115 | 동일 |
| `get_table_detail()` | L126 | **FR-02**: `psycopg2.sql.SQL` + `sql.Identifier` 로 교체 |

**FR-06 설계** — `get_conn()` 수정:
```python
@contextmanager
def get_conn(db_config):
    p = _make_pool(...)
    conn = p.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()   # ← 추가: dirty connection 방지
        raise
    finally:
        p.putconn(conn)
```

**FR-02 설계** — `get_table_detail()` 수정:
```python
from psycopg2 import sql

# Before (f-string 보간)
cur.execute(f"... WHERE a.attrelid = '{schema_name}.{table_name}'::regclass ...")

# After (psycopg2.sql 사용)
cur.execute(
    sql.SQL("... WHERE a.attrelid = {}::regclass ...")
    .format(sql.Literal(f"{schema_name}.{table_name}"))
)
```

---

### 3.2 manifest_utils.py

**책임**: `manifest.json` 1회 로드(캐시), SQL 변수 치환, lineage 탐색

**포함 함수**:
| 함수 | 원본 위치 | 변경사항 |
|------|----------|---------|
| `load_manifest()` | 신규 | **FR-03**: `@st.cache_data` + mtime 캐시 키 |
| `_get_this_from_manifest()` | L302 | `load_manifest()` 호출로 교체 |
| `_get_ref_from_manifest()` | L317 | `load_manifest()` 호출로 교체 |
| `_get_source_from_manifest()` | L332 | `load_manifest()` 호출로 교체 |
| `get_before_sql_from_model()` | L348 | 동일 (내부 호출만 교체) |
| `get_compiled_sql()` | L293 | 동일 |
| `get_dbt_model_hierarchy()` | L277 | 동일 |
| `get_lineage_from_manifest()` | L484 | `load_manifest()` 호출로 교체 |

**FR-03 설계** — `load_manifest()` 신규:
```python
@st.cache_data
def load_manifest(manifest_path: str, _mtime: float) -> dict:
    """manifest.json을 1회만 로드하고 mtime이 변경될 때만 재로드"""
    with open(manifest_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# 호출 방법
def _get_manifest(project_dir):
    path = os.path.join(project_dir, 'target', 'manifest.json')
    if not os.path.exists(path):
        return {}
    mtime = os.path.getmtime(path)
    return load_manifest(path, mtime)
```

---

### 3.3 validator.py

**책임**: 검증 로직 전체, 공통 유틸(`cells_differ`, `mk_badge`), `render_validation_ui`

**포함 함수**:
| 함수 | 원본 위치 | 변경사항 |
|------|----------|---------|
| `cells_differ()` | 신규 | **FR-04**: 4곳 중복 로직 추출 |
| `mk_badge()` | 신규 | **FR-05**: `_mk_badge`/`_det_badge` 통합 |
| `val_compile_model()` | L1211 | 동일 |
| `val_tgt_count()` | L1300 | **FR-01**: read-only 트랜잭션 + DDL 차단 |
| `val_tgt_sum()` | L1321 | **FR-01**: read-only 트랜잭션 + DDL 차단 |
| `val_src_sample()` | L1334 | **FR-07**: `LIMIT %s` 파라미터화 |
| `val_tgt_sample_matched()` | L1340 | 동일 |
| `val_tgt_sample_batch()` | L1366 | 동일 |
| `val_compare_results()` | L1409 | `cells_differ()` 사용으로 교체 |
| `_style_sample_df()` | L1644 | `cells_differ()` 사용으로 교체 |
| `insert_verification_to_db()` | L1520 | `cells_differ()` 사용으로 교체 |
| `render_validation_ui()` | L1664 | `cells_differ()`, `mk_badge()` 사용 |

**FR-01 설계** — `val_tgt_count()`, `val_tgt_sum()`:
```python
# DDL 키워드 차단 함수 (모듈 레벨)
_DDL_PATTERN = re.compile(
    r'\b(DROP|ALTER|CREATE|TRUNCATE|INSERT|UPDATE|DELETE|GRANT|REVOKE)\b',
    re.IGNORECASE
)

def _validate_where_clause(where_clause: str) -> None:
    if _DDL_PATTERN.search(where_clause):
        raise ValueError(f"WHERE절에 허용되지 않는 키워드가 포함되어 있습니다: {where_clause[:100]}")

def val_tgt_count(db_config, schema, table, date_filter=None, conn=None):
    if date_filter:
        _validate_where_clause(date_filter["where_clause"])  # DDL 차단
    where = f'\nWHERE {date_filter["where_clause"]}' if date_filter else ""
    sql = f'SELECT COUNT(*)\nFROM "{schema}"."{table}"{where}'
    ctx = nullcontext(conn) if conn else get_conn(db_config)
    with ctx as _conn:
        with _conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")  # read-only 트랜잭션
            cur.execute(sql)
            return cur.fetchone()[0]
```

**FR-04 설계** — `cells_differ()`:
```python
def cells_differ(sv, tv) -> bool:
    """두 셀 값을 비교. NaN은 동일로 처리, 숫자는 float 비교."""
    if pd.isna(sv) and pd.isna(tv):
        return False
    try:
        return float(sv) != float(tv)
    except (ValueError, TypeError):
        return str(sv) != str(tv)
```

**FR-05 설계** — `mk_badge()`:
```python
def mk_badge(label: str, ok: bool, fail_color: str) -> str:
    """HTML 배지 생성. 성공 시 초록, 실패 시 fail_color."""
    color = '#27ae60' if ok else fail_color
    return f'<span style="background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:0.85em">{label}</span>'
```

**FR-07 설계** — `val_src_sample()`:
```python
# Before
sql = f"SELECT * FROM (\n{compiled_sql}\n) _src LIMIT {limit}"
cur.execute(sql)

# After
sql = f"SELECT * FROM (\n{compiled_sql}\n) _src LIMIT %s"
cur.execute(sql, (limit,))
```

---

### 3.4 history.py

**책임**: dbt 실행 로그 및 검증 이력 UI 렌더링

**포함 함수**:
| 함수 | 원본 위치 | 변경사항 |
|------|----------|---------|
| `fetch_verification_history()` | L1447 | validator.py에서 이동 (순환 import 방지) |
| `fetch_verification_detail()` | L1474 | validator.py에서 이동 (순환 import 방지) |
| `fetch_dbt_log()` | L1493 | 동일 |
| `render_history_ui()` | L2471 | `mk_badge()` → `validator.mk_badge` import |

---

### 3.5 dbt_unified_app.py (진입점)

**책임**: 앱 설정, YAML 유틸, dbt 러너, session_state 초기화, 탭 라우팅

**유지 함수** (이동 안 함):
- `load_cache()`, `save_cache()`, `convert_to_dbt_ts()` (L21-40)
- YAML 관련: `is_pure_model_yml()`, `get_all_yml_files()`, `build_model_entry()` 등
- dbt 러너 관련: `run_dbt_command()`, `render_runner_ui()` 등
- 탭 라우팅: `main()`, `open_validation_dialog()` 등

**예상 줄 수**: ~900줄 (현재 2817줄의 32%) — YAML Generator UI(~250줄) + Runner UI(~300줄)가 진입점 잔류. 원본 대비 69% 감축 달성.

---

## 4. 구현 파일 목록

### 신규 생성 파일 (CLAUDE.md §6 준수: 새 파일로 먼저 작성)

| 파일 | 목적 |
|------|------|
| `mycodes/db_utils_new.py` | db_utils 초안 → 테스트 후 `db_utils.py`로 rename |
| `mycodes/manifest_utils_new.py` | manifest_utils 초안 → 테스트 후 rename |
| `mycodes/validator_new.py` | validator 초안 → 테스트 후 rename |
| `mycodes/history_new.py` | history 초안 → 테스트 후 rename |
| `mycodes/dbt_unified_app_new.py` | 진입점 초안 → 테스트 후 rename |

### 백업 (rename 전 bak/ 이동)

| 원본 | 백업 경로 |
|------|----------|
| `mycodes/dbt_unified_app.py` | `mycodes/bak/dbt_unified_app.py` |

---

## 5. 구현 순서 (Implementation Guide)

### Module 1 — 보안·안정성 패치 (FR-01, FR-02, FR-06, FR-07)
1. `db_utils_new.py` 작성 (FR-02, FR-06 포함)
2. `validator_new.py` 내 `_validate_where_clause`, `val_tgt_count`, `val_tgt_sum` 수정 (FR-01)
3. `val_src_sample` LIMIT 파라미터화 (FR-07)
4. 앱 기동 확인

### Module 2 — 성능·중복 제거 (FR-03, FR-04, FR-05)
1. `manifest_utils_new.py` 작성 (`load_manifest` 캐시 포함, FR-03)
2. `cells_differ()` 추출 및 4곳 교체 (FR-04)
3. `mk_badge()` 통합 및 2곳 교체 (FR-05)

### Module 3 — 파일 모듈화 완성 (FR-08)
1. `validator_new.py` 전체 완성
2. `history_new.py` 작성
3. `dbt_unified_app_new.py` 진입점으로 슬림화
4. 전체 기동 테스트 (3개 탭 확인)
5. 사용자 확인 후 bak/ 이동 + rename

---

### 11.3 Session Guide

| 세션 | 모듈 | 예상 작업량 | SC 연관 |
|------|------|------------|---------|
| Session 1 | Module 1 | ~100줄 수정 | SC-01 |
| Session 2 | Module 2 | ~80줄 수정 | SC-02, SC-03 |
| Session 3 | Module 3 | ~신규 400줄 + 리팩토링 | SC-04, SC-05 |

---

## 6. 검증 방법

| SC | 검증 명령 |
|----|----------|
| SC-01 | `grep -n "where_clause\|f-string\|f'" mycodes/validator.py` — 결과 없어야 함 |
| SC-02 | `grep -rn "manifest.json" mycodes/ \| grep "open\("` — 1건만 나와야 함 |
| SC-03 | `grep -n "cells_differ\|_style_sample\|sample_status" mycodes/validator.py` — cells_differ 정의 1건 |
| SC-04 | `wc -l mycodes/dbt_unified_app.py` — 500 이하 |
| SC-05 | Streamlit 앱 실행 후 YAML생성·검증·이력 탭 수동 확인 |

---

*생성일: 2026-03-28 | Author: hjpark | Architecture: Option C Pragmatic*
