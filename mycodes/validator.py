# Design Ref: §3.3 — 검증 로직 전체, 공통 유틸(cells_differ, mk_badge), render_validation_ui
# 의존성: db_utils (get_conn, get_db_config, get_schemas), manifest_utils (get_compiled_sql 등)
import os
import re
import json
import subprocess
import uuid
import psycopg2
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from contextlib import nullcontext

from db_utils import get_conn, get_db_config, get_schemas
from manifest_utils import (
    get_compiled_sql,
    get_dbt_model_hierarchy,
    get_before_sql_from_model,
    convert_to_dbt_ts,
    _get_model_raw_sql,
    _detect_before_sql_date_col,
    _has_date_vars,
)

# psycopg2 숫자 타입 OID (pg_type.oid)
_NUMERIC_OIDS = {20, 21, 23, 700, 701, 1700, 790}  # int8,int2,int4,float4,float8,numeric,money
# psycopg2 날짜/타임스탬프 타입 OID
_DATE_OIDS    = {1082, 1114, 1184, 1083, 1266}      # date, timestamp, timestamptz, time, timetz

DATE_FORMATS = ["yyyymmdd", "yyyy-mm-dd", "yyyymm", "yyyy-mm"]

# Design Ref: §3.3 FR-01 — WHERE절 DDL 키워드 차단
_DDL_PATTERN = re.compile(
    r'\b(DROP|ALTER|CREATE|TRUNCATE|INSERT|UPDATE|DELETE|GRANT|REVOKE)\b',
    re.IGNORECASE
)


def _fmt_date(date_obj, fmt):
    """date 객체를 선택한 포맷 문자열로 변환"""
    if fmt == "yyyymmdd":
        return date_obj.strftime("%Y%m%d")
    elif fmt == "yyyy-mm-dd":
        return date_obj.strftime("%Y-%m-%d")
    elif fmt == "yyyymm":
        return date_obj.strftime("%Y%m")
    elif fmt == "yyyy-mm":
        return date_obj.strftime("%Y-%m")
    return date_obj.strftime("%Y-%m-%d")


def _validate_where_clause(where_clause: str) -> None:
    """WHERE절에 DDL 키워드 포함 여부 검사. 포함 시 ValueError 발생."""
    # Plan SC: SC-01 — SQL injection 취약점 0건
    if _DDL_PATTERN.search(where_clause):
        raise ValueError(f"WHERE절에 허용되지 않는 키워드가 포함되어 있습니다: {where_clause[:120]}")


# Design Ref: §3.3 FR-05 — badge HTML 생성 공통 함수 (2곳 중복 → 1곳)
def mk_badge(label: str, ok, fail_color: str) -> str:
    """검증 결과 배지 HTML 생성. ok=None이면 빈 문자열, 성공 시 초록, 실패 시 fail_color."""
    if ok is None:
        return ''
    bg = '#27ae60' if ok else fail_color
    return (f'<span style="background:{bg};color:white;padding:3px 10px;'
            f'border-radius:4px;font-size:13px;font-weight:500;">'
            f'{label} {"PASS" if ok else "FAIL"}</span>')


# Design Ref: §3.3 FR-04 — 셀 비교 공통 함수 (4곳 중복 → 1곳)
def cells_differ(sv, tv) -> bool:
    """두 셀 값이 다른지 비교. NaN 쌍은 동일로, 숫자는 float 비교."""
    # Plan SC: SC-03 — 중복 셀 비교 코드 1개 함수로 통합
    if pd.isna(sv) and pd.isna(tv):
        return False
    try:
        return float(sv) != float(tv)
    except (TypeError, ValueError):
        return str(sv) != str(tv)


def val_compile_model(project_dir, profile_dir, target_val, model_name, sdt, edt):
    """
    dbt compile 실행 후 compiled SQL 문자열 반환.
    vars: data_interval_start / data_interval_end / run_mode=manual
    """
    v_j = json.dumps({
        "data_interval_start": convert_to_dbt_ts(sdt),
        "data_interval_end":   convert_to_dbt_ts(edt, is_end=True),
        "run_mode": "manual",
    })
    args = [
        "dbt", "-q", "compile",
        "--select",       model_name,
        "--target",       target_val,
        "--vars",         v_j,
        "--project-dir",  project_dir,
        "--profiles-dir", profile_dir,
    ]
    rc = subprocess.run(args, cwd=project_dir, capture_output=True, text=True)
    if rc.returncode != 0:
        raise RuntimeError(rc.stderr.strip() or rc.stdout.strip())
    sql = get_compiled_sql(project_dir, model_name)
    if not sql:
        raise RuntimeError("compile 성공했으나 compiled SQL 파일을 찾을 수 없습니다.")
    return sql


def val_get_columns_from_query(db_config, cte_sql, conn=None):
    """CTE SQL을 LIMIT 0으로 실행해 컬럼 목록(이름, 타입코드) 반환"""
    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        cur = c.cursor()
        cur.execute(f"SELECT * FROM ({cte_sql}) _q LIMIT 0")
        return [(desc[0], desc[1]) for desc in cur.description]


def val_get_pk(db_config, schema, table):
    """타겟 테이블의 PK 컬럼 목록 반환"""
    with get_conn(db_config) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "  AND tc.table_schema   = kcu.table_schema "
            "WHERE tc.constraint_type = 'PRIMARY KEY' "
            "  AND tc.table_schema = %s AND tc.table_name = %s "
            "ORDER BY kcu.ordinal_position",
            (schema, table)
        )
        return [r[0] for r in cur.fetchall()]


def val_get_tgt_columns(db_config, schema, table):
    """타겟 테이블의 (컬럼명, data_type) 목록 반환"""
    with get_conn(db_config) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (schema, table)
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def val_src_count(db_config, compiled_sql, conn=None):
    """소스: CTE 기반 COUNT → (count, executed_sql)"""
    sql = f"SELECT COUNT(*) FROM (\n{compiled_sql}\n) _src"
    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        cur = c.cursor()
        cur.execute(sql)
        return cur.fetchone()[0], sql


def val_tgt_count(db_config, schema, table, date_filter=None, conn=None):
    """타겟: 테이블 직접 COUNT → (count, executed_sql)"""
    if date_filter:
        _validate_where_clause(date_filter["where_clause"])
    where = f'\nWHERE {date_filter["where_clause"]}' if date_filter else ""
    sql   = f'SELECT COUNT(*)\nFROM "{schema}"."{table}"{where}'
    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        cur = c.cursor()
        cur.execute("SET TRANSACTION READ ONLY")  # Design Ref: §3.3 FR-01
        cur.execute(sql)
        return cur.fetchone()[0], sql


def val_src_sum(db_config, compiled_sql, num_cols, conn=None):
    """소스: CTE 기반 숫자컬럼 SUM → (sums_dict, executed_sql)"""
    if not num_cols:
        return {}, None
    sum_expr = ",\n  ".join([f'SUM("{c}") AS "{c}"' for c in num_cols])
    sql = f'SELECT\n  {sum_expr}\nFROM (\n{compiled_sql}\n) _src'
    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        cur = c.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return {c: (float(row[i]) if row[i] is not None else None) for i, c in enumerate(num_cols)}, sql


def val_tgt_sum(db_config, schema, table, num_cols, date_filter=None, conn=None):
    """타겟: 테이블 직접 숫자컬럼 SUM → (sums_dict, executed_sql)"""
    if not num_cols:
        return {}, None
    if date_filter:
        _validate_where_clause(date_filter["where_clause"])
    sum_expr = ",\n  ".join([f'SUM("{c}") AS "{c}"' for c in num_cols])
    where    = f'\nWHERE {date_filter["where_clause"]}' if date_filter else ""
    sql      = f'SELECT\n  {sum_expr}\nFROM "{schema}"."{table}"{where}'
    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        cur = c.cursor()
        cur.execute("SET TRANSACTION READ ONLY")  # Design Ref: §3.3 FR-01
        cur.execute(sql)
        row = cur.fetchone()
        return {c: (float(row[i]) if row[i] is not None else None) for i, c in enumerate(num_cols)}, sql


def val_src_sample(db_config, compiled_sql, limit, conn=None):
    """소스: CTE 기반 샘플 → (df, executed_sql)"""
    # Design Ref: §3.3 FR-07 — LIMIT 파라미터화
    sql = f"SELECT * FROM (\n{compiled_sql}\n) _src LIMIT %s"
    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        return pd.read_sql(sql, c, params=(limit,)), sql


def val_tgt_sample_matched(db_config, schema, table, src_row, pk_cols, all_cols, conn=None):
    """
    타겟: 소스 샘플 행 1건에 대응하는 행을 타겟 테이블에서 조회 (fallback용).
    PK 있으면 PK로, 없으면 전 컬럼 WHERE 조건. NULL은 IS NULL 처리.
    → (df, actual_sql)
    """
    key_cols = pk_cols if pk_cols else all_cols
    conditions, params = [], []
    for col in key_cols:
        val = src_row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            conditions.append(f'"{col}" IS NULL')
        else:
            native_val = val.item() if hasattr(val, 'item') else val  # numpy scalar → Python native
            conditions.append(f'"{col}" = %s')
            params.append(native_val)

    where_clause = " AND ".join(conditions) if conditions else "TRUE"
    sql = f'SELECT * FROM "{schema}"."{table}" WHERE {where_clause} LIMIT 1'

    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        cur = c.cursor()
        actual_sql = cur.mogrify(sql, params if params else None).decode('utf-8')
        df = pd.read_sql(sql, c, params=params if params else None)
    return df, actual_sql


def val_tgt_sample_batch(db_config, schema, table, src_df, pk_cols, all_cols, conn=None):
    """
    타겟: 소스 샘플 N건을 IN 절 일괄 조회 (개별 쿼리 대비 N배 빠름).
    - 단일 PK: WHERE col IN (v1, v2, ...)
    - 복합 PK: WHERE (c1, c2) IN ((v1, v2), ...)
    - PK 없음: val_tgt_sample_matched 개별 쿼리로 폴백
    → (tgt_df, actual_sql)
    """
    if not pk_cols:
        rows, sqls = [], []
        for _, row in src_df.iterrows():
            df, q = val_tgt_sample_matched(db_config, schema, table,
                                           row.to_dict(), [], all_cols, conn=conn)
            rows.append(df)
            sqls.append(q)
        tgt_df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=all_cols)
        return tgt_df, "\n\n-- 다음 행\n".join(sqls)

    # numpy scalar → native Python type (np.float64 등이 psycopg2 파라미터로 전달되면 오류)
    def _to_native(v):
        """numpy scalar를 Python 기본 타입으로 변환. psycopg2 파라미터 전달 오류 방지."""
        return v.item() if hasattr(v, 'item') else v

    rows_data = [tuple(_to_native(row[c]) for c in pk_cols) for _, row in src_df.iterrows()]

    if len(pk_cols) == 1:
        col   = pk_cols[0]
        vals  = [r[0] for r in rows_data]
        sql   = f'SELECT * FROM "{schema}"."{table}" WHERE "{col}" IN ({",".join(["%s"] * len(vals))})'
        params = vals
    else:
        # PostgreSQL row value comparison: (c1, c2) IN ((v1a, v1b), ...)
        col_list = ",".join([f'"{c}"' for c in pk_cols])
        row_ph   = ",".join(["(" + ",".join(["%s"] * len(pk_cols)) + ")"] * len(rows_data))
        sql      = f'SELECT * FROM "{schema}"."{table}" WHERE ({col_list}) IN ({row_ph})'
        params   = [v for row in rows_data for v in row]

    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        cur = c.cursor()
        actual_sql = cur.mogrify(sql, params).decode('utf-8')
        tgt_df = pd.read_sql(sql, c, params=params)
    return tgt_df, actual_sql


def val_compare_results(src_result, tgt_result):
    """테이블별 결과 비교 → summary rows 반환"""
    rows = []
    for table in src_result:
        src = src_result[table]
        tgt = tgt_result.get(table, {})

        src_cnt = src.get('count')
        tgt_cnt = tgt.get('count')
        count_match = (src_cnt == tgt_cnt) if (src_cnt is not None and tgt_cnt is not None) else None

        row = {
            'table': table,
            'src_count': src_cnt,
            'tgt_count': tgt_cnt,
            'count_diff': (tgt_cnt - src_cnt) if (src_cnt is not None and tgt_cnt is not None) else None,
            'count_match': count_match,
            'sum_details': [],
            'sum_all_match': True,
        }

        src_sums = src.get('sums', {})
        tgt_sums = tgt.get('sums', {})
        for col in src_sums:
            sv = src_sums[col]
            tv = tgt_sums.get(col)
            match = (sv == tv) if (sv is not None and tv is not None) else None
            if match is False:
                row['sum_all_match'] = False
            row['sum_details'].append({
                'column': col, 'src_sum': sv, 'tgt_sum': tv,
                'diff': (tv - sv) if (sv is not None and tv is not None) else None,
                'match': match,
            })
        rows.append(row)
    return rows


def insert_verification_to_db(db_config, vr, r, compiled_sql):
    """검증 결과를 admin 스키마 4개 테이블에 저장하고 uuid 반환"""
    tbl      = vr['model']
    run_uuid = str(uuid.uuid4())
    run_at   = datetime.strptime(vr['run_at'], '%Y-%m-%d %H:%M:%S')
    queries  = vr.get('queries', {})

    try:
        sdt = datetime.strptime(vr['sdt'], '%Y-%m-%d') if vr.get('sdt') else None
        edt = datetime.strptime(vr['edt'], '%Y-%m-%d') if vr.get('edt') else None
    except Exception:
        sdt = edt = None

    # count_status
    count_status = None
    if vr.get('do_count') and r.get('count_match') is not None:
        count_status = 'PASS' if r['count_match'] else 'FAIL'

    # sum_status
    sum_status = None
    if vr.get('do_sum'):
        sum_status = 'PASS' if r.get('sum_all_match') else 'FAIL'

    # sample_status: src/tgt DataFrame 비교
    sample_status = None
    if vr.get('do_sample'):
        src_df = vr['src'][tbl].get('sample')
        tgt_df = vr['tgt'][tbl].get('sample')
        if src_df is not None and tgt_df is not None and not src_df.empty and not tgt_df.empty:
            common_cols = [c for c in src_df.columns if c in tgt_df.columns]
            has_diff = any(
                cells_differ(src_df.iloc[idx][col], tgt_df.iloc[idx][col])
                for idx in range(min(len(src_df), len(tgt_df)))
                for col in common_cols
            )
            sample_status = 'FAIL' if has_diff else 'PASS'
        elif src_df is not None and tgt_df is not None:
            sample_status = 'PASS'

    with get_conn(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO admin.verification_summary
                    (model_name, uuid, query_condition_start, query_condition_end,
                     count_status, sum_status, sample_status, compiled_sql, verification_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (tbl, run_uuid, sdt, edt, count_status, sum_status, sample_status,
                 compiled_sql, run_at),
            )

            if vr.get('do_count'):
                cur.execute(
                    """
                    INSERT INTO admin.verification_count
                        (model_name, uuid, source_count_result, target_count_result,
                         source_count_sql, target_count_sql, verification_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        tbl, run_uuid,
                        r.get('src_count'), r.get('tgt_count'),
                        queries.get('count', {}).get('src'),
                        queries.get('count', {}).get('tgt'),
                        run_at,
                    ),
                )

            if vr.get('do_sum'):
                src_sums = vr['src'][tbl].get('sums', {})
                tgt_sums = vr['tgt'][tbl].get('sums', {})
                cur.execute(
                    """
                    INSERT INTO admin.verification_sum
                        (model_name, uuid, source_sum_result, target_sum_result,
                         source_sum_sql, target_sum_sql, verification_date)
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
                    """,
                    (
                        tbl, run_uuid,
                        json.dumps(src_sums, default=str),
                        json.dumps(tgt_sums, default=str),
                        queries.get('sum', {}).get('src'),
                        queries.get('sum', {}).get('tgt'),
                        run_at,
                    ),
                )

            if vr.get('do_sample'):
                _src_df = vr['src'][tbl].get('sample')
                _tgt_df = vr['tgt'][tbl].get('sample')
                src_json = json.dumps(_src_df.to_dict(orient='records'), default=str, ensure_ascii=False) if _src_df is not None else None
                tgt_json = json.dumps(_tgt_df.to_dict(orient='records'), default=str, ensure_ascii=False) if _tgt_df is not None else None
                cur.execute(
                    """
                    INSERT INTO admin.verification_sample
                        (model_name, uuid, source_sample_result, target_sample_result,
                         source_sample_sql, target_sample_sql, verification_date)
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s)
                    """,
                    (
                        tbl, run_uuid,
                        src_json, tgt_json,
                        queries.get('sample', {}).get('src'),
                        queries.get('sample', {}).get('tgt'),
                        run_at,
                    ),
                )

        conn.commit()

    return run_uuid


def _style_sample_df(df, other_df, common_cols, diff_color):
    """other_df와 값이 다른 셀을 diff_color로 하이라이트한 Styler 반환."""
    n = min(len(df), len(other_df))
    style_df = pd.DataFrame('', index=df.index, columns=df.columns)
    for idx in range(n):
        for col in common_cols:
            if col not in df.columns or col not in other_df.columns:
                continue
            if cells_differ(df.iloc[idx][col], other_df.iloc[idx][col]):
                style_df.iloc[idx, df.columns.get_loc(col)] = f'background-color: {diff_color}'
    return df.style.apply(lambda _: style_df, axis=None)


def render_validation_ui(project_dir, profile_dir, target_val):
    """검증 UI — 탭과 다이얼로그에서 공용으로 사용"""
    _vv = st.session_state['_val_key_ver']   # 맨 위로 이동 — btn_reset_val 키에 사용
    _vh1, _vh2 = st.columns([4, 1])
    with _vh1:
        st.subheader("🔍 데이터 검증")
    with _vh2:
        st.write("")
        if st.button("🔄 초기화", key=f"btn_reset_val_{_vv}", use_container_width=True):
            st.session_state['_val_key_ver']       += 1   # 위젯 key 변경 → 자동 초기화
            st.session_state['val_tgt_sc']         = None
            st.session_state['val_results']        = None
            st.session_state['val_compiled_sql']   = None
            st.session_state['val_compiled_model'] = None
            st.session_state['val_before_sql']     = None
            st.session_state['val_date_filter']    = None
            st.session_state['val_direct_where']   = ""
            st.session_state['val_custom_start']   = ""
            st.session_state['val_custom_end']     = ""
            st.session_state['val_exclude_cols']   = ['dbt_dtm']
            st.session_state['runner_to_val']      = None
            st.rerun()

    if not target_val:
        st.warning("사이드바에서 Target을 먼저 선택해주세요.")
        return

    db_config_val = get_db_config(profile_dir, target_val)
    if not db_config_val:
        st.error("DB 설정을 불러올 수 없습니다.")
        return

    try:
        val_schemas = get_schemas(db_config_val)
    except Exception as e:
        st.error(f"DB 연결 실패: {e}"); return


    # ── 1. 모델 / 타겟 스키마 선택 ──────────────────────────
    st.markdown("#### 1. 모델 및 타겟 스키마 선택")

    val_hierarchy, _ = get_dbt_model_hierarchy(project_dir)

    def _val_reset():
        """모델/날짜 위젯 변경 시 검증 결과와 컴파일 캐시를 초기화."""
        st.session_state['val_results']      = None
        st.session_state['val_compiled_sql']  = None
        st.session_state['val_compiled_model'] = None
        st.session_state['val_before_sql']    = None
        st.session_state['val_date_filter']   = None
        st.session_state['val_direct_where']  = ""

    val_gp_p  = "📂 그룹 선택"
    vi1, vi2  = st.columns(2)
    with vi1:
        val_sel_g = st.selectbox(
            "📁 모델 그룹", [val_gp_p] + list(val_hierarchy.keys()),
            key=f"val_sb_group_{_vv}", on_change=_val_reset
        )
        val_sel_m = None
        if val_sel_g != val_gp_p:
            val_mp_p  = "📂 모델 선택"
            val_mv    = st.selectbox(
                "📂 모델", [val_mp_p] + val_hierarchy[val_sel_g],
                key=f"val_sb_model_{_vv}", on_change=_val_reset
            )
            if val_mv != val_mp_p:
                val_sel_m = val_mv

    # 타겟 스키마 자동 조회 (모델명과 일치하는 테이블을 가진 스키마 탐색)
    with vi2:
        _schema_warn       = None
        _schema_candidates = []

        if val_sel_m:
            try:
                with get_conn(db_config_val) as conn_tmp:
                    cur_tmp = conn_tmp.cursor()
                    cur_tmp.execute(
                        "SELECT table_schema FROM information_schema.tables "
                        "WHERE table_name = %s AND table_type = 'BASE TABLE' "
                        "ORDER BY table_schema",
                        (val_sel_m,)
                    )
                    _schema_candidates = [r[0] for r in cur_tmp.fetchall()]
            except Exception:
                pass

            if len(_schema_candidates) == 0:
                _schema_warn = f"⚠️ DB에 `{val_sel_m}` 테이블이 없습니다."
            elif len(_schema_candidates) == 1:
                # 후보가 1개면 session_state를 자동으로 덮어써서 selectbox에 반영
                if st.session_state.get(f'val_sb_tgt_sc_{_vv}') != _schema_candidates[0]:
                    st.session_state[f'val_sb_tgt_sc_{_vv}'] = _schema_candidates[0]

        val_tgt_sc = st.selectbox(
            "📂 타겟 스키마",
            val_schemas,
            key=f"val_sb_tgt_sc_{_vv}",
            help="모델명과 일치하는 테이블이 있는 스키마를 자동 탐색합니다."
        )
        st.session_state['val_tgt_sc'] = val_tgt_sc

        # 피드백 — 실제 selectbox 선택값(val_tgt_sc) 기준으로 테이블 존재 여부 확인
        if val_sel_m:
            if _schema_warn:
                st.error(_schema_warn)
            elif len(_schema_candidates) > 1:
                st.warning(
                    f"⚠️ `{val_sel_m}` 테이블이 여러 스키마에 존재합니다: "
                    f"{', '.join(_schema_candidates)}. 직접 선택하세요."
                )
            elif val_tgt_sc in _schema_candidates:
                st.caption(f"✅ `{val_tgt_sc}` 스키마에서 테이블 확인됨")
            else:
                st.error(f"⚠️ `{val_tgt_sc}` 스키마에 `{val_sel_m}` 테이블이 없습니다.")

    # ── 2. 날짜 선택 (compile vars) ─────────────────────────
    st.markdown("#### 2. 날짜 범위 선택 (dbt compile vars)")
    vd1, vd2 = st.columns(2)
    with vd1:
        val_sdt = st.date_input(
            "📅 data_interval_start",
            value=datetime.now().date() - timedelta(days=4),
            key=f"val_sdt_{_vv}", on_change=_val_reset
        )
    with vd2:
        val_edt = st.date_input(
            "📅 data_interval_end",
            value=datetime.now().date() - timedelta(days=1),
            key=f"val_edt_{_vv}", on_change=_val_reset
        )

    val_date_invalid = False
    if val_edt < val_sdt:
        st.error("⚠️ 종료 날짜는 시작 날짜보다 빠를 수 없습니다.")
        val_date_invalid = True

    # ── 3. 검증 항목 ────────────────────────────────────────
    st.markdown("#### 3. 검증 항목")
    vo1, vo2, vo3 = st.columns(3)
    with vo1:
        do_count  = st.checkbox("행 수 (COUNT) 비교", value=True, key=f"val_chk_count_{_vv}")
    with vo2:
        do_sum    = st.checkbox("숫자 컬럼 SUM 비교", value=True, key=f"val_chk_sum_{_vv}")
    with vo3:
        do_sample = st.checkbox("샘플 데이터 비교",   value=True, key=f"val_chk_sample_{_vv}")

    sample_limit = 5
    if do_sample:
        sample_limit = st.slider(
            "샘플 행 수", min_value=5, max_value=100,
            value=5, step=5, key=f"val_sample_limit_{_vv}"
        )

    # ── 4. 타겟 날짜 필터 (COUNT / SUM 용) ──────────────────
    _sql_has_dates = False   # 검증 실행 버튼 disable 판단용 (section 4 내부에서 설정)
    if (do_count or do_sum) and val_sel_m and _schema_candidates:
        st.markdown("#### 4. 타겟 날짜 조건 (COUNT / SUM)")
        st.caption("타겟 테이블에 적용할 날짜 필터를 설정하세요. 선택하지 않으면 전체 데이터 기준으로 비교합니다.")

        try:
            _tgt_cols = val_get_tgt_columns(db_config_val, val_tgt_sc, val_sel_m)
        except Exception:
            _tgt_cols = []

        _date_first = [c for c, t in _tgt_cols if any(d in t for d in ['date', 'timestamp', 'time'])]
        _other_cols = [c for c, t in _tgt_cols if c not in _date_first]
        _col_type   = {c: t for c, t in _tgt_cols}

        # before_sql 날짜 컬럼 감지 + {{start}}/{{end}} 존재 여부
        _raw_model_sql = _get_model_raw_sql(project_dir, val_sel_m)
        _before_col    = _detect_before_sql_date_col(_raw_model_sql)
        _sql_has_dates = _has_date_vars(_raw_model_sql)

        # ⚡ 아이콘: before_sql에서 감지된 컬럼에 접두사
        _col_opts_plain   = ["(조건 없음)"] + _date_first + _other_cols
        _col_opts_display = [
            f"⚡ {c}" if (_before_col and c == _before_col) else c
            for c in _col_opts_plain
        ]
        _col_opts_display.append("✏️ 직접 입력")

        # PostgreSQL 타입 → 캐스트 문자열
        _cast_map = {
            'timestamp without time zone': '::timestamp',
            'timestamp with time zone':    '::timestamptz',
            'date':                        '::date',
            'time without time zone':      '::time',
            'time with time zone':         '::timetz',
        }

        vf1, vf2 = st.columns([2, 2])
        with vf1:
            _sel_filter_col_raw = st.selectbox(
                "날짜 컬럼 선택", _col_opts_display, key=f"val_filter_col_{_vv}"
            )
            _is_direct_input = (_sel_filter_col_raw == "✏️ 직접 입력")
            _sel_filter_col  = _sel_filter_col_raw.replace("⚡ ", "") if not _is_direct_input else "✏️ 직접 입력"
            if not _sql_has_dates and not _is_direct_input:
                st.caption("ℹ️ 모델에 날짜 조건이 없습니다.")

        # 직접 입력 모드
        _direct_where = ""
        if _is_direct_input:
            _direct_where = st.text_area(
                "WHERE 절 직접 입력 (컬럼명 포함 전체 조건)",
                value=st.session_state.get('val_direct_where', ''),
                placeholder="예) customer_id IN (SELECT customer_id FROM stg.stg_receipts WHERE order_date BETWEEN '2026-01-01 00:00:00'::timestamp AND '2026-01-02 23:59:59'::timestamp)",
                height=100,
                key=f"val_direct_where_input_{_vv}",
            )

        if not _is_direct_input:
            # 타입 판별
            _sel_col_type = _col_type.get(_sel_filter_col, "") if _sel_filter_col != "(조건 없음)" else ""
            _is_dt        = any(t in _sel_col_type for t in ['date', 'timestamp', 'time'])
            _cast         = next((v for k, v in _cast_map.items() if k in _sel_col_type), '')

            # 사용자 정의 체크박스: date/timestamp면 비활성화
            with vf2:
                if _sel_filter_col != "(조건 없음)" and not _is_dt:
                    _use_custom = st.checkbox(
                        "사용자 정의 조건 입력",
                        key=f"val_chk_custom_{_vv}",
                    )
                else:
                    _use_custom = False
                    if _sel_filter_col != "(조건 없음)":
                        st.checkbox(
                            "사용자 정의 조건 입력",
                            key=f"val_chk_custom_{_vv}",
                            value=False,
                            disabled=True,
                            help="date/timestamp 타입은 자동으로 타임스탬프 형태로 조건이 생성됩니다."
                        )

            # 사용자 정의 입력 (varchar 등 비날짜 컬럼)
            _custom_start, _custom_end = None, None
            if _sel_filter_col != "(조건 없음)" and _use_custom:
                _default_start = f"'{val_sdt} 00:00:00'"
                _default_end   = f"'{val_edt} 23:59:59'"
                vc1, vc2 = st.columns(2)
                with vc1:
                    _custom_start = st.text_input(
                        "시작 값",
                        value=st.session_state.get('val_custom_start') or _default_start,
                        placeholder=f'예) {_default_start}',
                        key=f"val_custom_start_input_{_vv}"
                    )
                with vc2:
                    _custom_end = st.text_input(
                        "종료 값",
                        value=st.session_state.get('val_custom_end') or _default_end,
                        placeholder=f'예) {_default_end}',
                        key=f"val_custom_end_input_{_vv}"
                    )
                if _custom_start and _custom_end:
                    st.caption(
                        f"적용 예시: `\"{_sel_filter_col}\" >= {_custom_start}"
                        f" AND \"{_sel_filter_col}\" <= {_custom_end}`"
                    )
        else:
            _use_custom   = False
            _sel_col_type = ""
            _is_dt        = False
            _cast         = ""
            _custom_start = None
            _custom_end   = None

        # 쿼리에 반영 버튼
        if _is_direct_input:
            _apply_disabled = not _direct_where.strip()
        else:
            _apply_disabled = _sel_filter_col == "(조건 없음)"
        bc1, bc2 = st.columns([3, 1])
        with bc2:
            if st.button("✅ 쿼리에 반영", use_container_width=True, disabled=_apply_disabled):
                if _is_direct_input:
                    _where = _direct_where.strip()
                    # macOS 스마트 따옴표 → ASCII 직선 따옴표 정규화
                    _where = _where.replace('\u2018', "'").replace('\u2019', "'")
                    _where = _where.replace('\u201c', '"').replace('\u201d', '"')
                    st.session_state['val_direct_where'] = _where
                    st.session_state['val_date_filter'] = {
                        'col':          '직접 입력',
                        'start':        '',
                        'end':          '',
                        'where_clause': _where,
                    }
                elif _use_custom:
                    # varchar 등: 입력값 그대로 사용
                    _sv    = _custom_start or ""
                    _ev    = _custom_end   or ""
                    _where = f'"{_sel_filter_col}" >= {_sv} AND "{_sel_filter_col}" <= {_ev}'
                    st.session_state['val_custom_start'] = _custom_start
                    st.session_state['val_custom_end']   = _custom_end
                    st.session_state['val_date_filter'] = {
                        'col':          _sel_filter_col,
                        'start':        _sv,
                        'end':          _ev,
                        'where_clause': _where,
                    }
                else:
                    # date/timestamp: 값은 타임스탬프 문자열 + ::타입 캐스팅
                    _sv    = f"'{val_sdt} 00:00:00'{_cast}"
                    _ev    = f"'{val_edt} 23:59:59'{_cast}"
                    _where = f'"{_sel_filter_col}" BETWEEN {_sv} AND {_ev}'
                    st.session_state['val_date_filter'] = {
                        'col':          _sel_filter_col,
                        'start':        _sv,
                        'end':          _ev,
                        'where_clause': _where,
                    }
        with bc1:
            _cur_filter = st.session_state.get('val_date_filter')
            if _cur_filter:
                st.code(_cur_filter['where_clause'], language="sql")

        # 필터 초기화
        if _sel_filter_col == "(조건 없음)" and not _is_direct_input and st.session_state.get('val_date_filter'):
            if st.button("🗑 필터 초기화"):
                st.session_state['val_date_filter'] = None
    else:
        st.session_state['val_date_filter'] = None

    # ── 5. 샘플 비교 제외 컬럼 ──────────────────────────────
    if do_sample and val_sel_m and _schema_candidates:
        st.markdown("#### 5. 샘플 비교 제외 컬럼")
        st.caption("소스/타겟 샘플 데이터프레임과 타겟 쿼리 조건에서 제외할 컬럼을 선택하세요.")
        try:
            _excl_tgt_cols = val_get_tgt_columns(db_config_val, val_tgt_sc, val_sel_m)
            _excl_col_opts = [c for c, _ in _excl_tgt_cols]
        except Exception:
            _excl_col_opts = []
        _default_excl = [c for c in st.session_state.get('val_exclude_cols', ['dbt_dtm']) if c in _excl_col_opts]
        _selected_excl = st.multiselect(
            "제외 컬럼",
            options=_excl_col_opts,
            default=_default_excl,
            key=f"val_exclude_cols_ms_{_vv}",
        )
        st.session_state['val_exclude_cols'] = _selected_excl

    st.divider()

    # ── Compile / 검증 실행 버튼 분리 ───────────────────────
    _tbl_missing  = bool(val_sel_m and not _schema_candidates)
    _base_disabled = not val_sel_m or val_date_invalid or _tbl_missing
    _date_apply_needed = (
        _sql_has_dates and val_sel_m and _schema_candidates and
        (do_count or do_sum) and
        st.session_state.get('val_date_filter') is None
    )
    _run_disabled  = _base_disabled or not (do_count or do_sum or do_sample) or _date_apply_needed

    btn_c1, btn_c2 = st.columns(2)

    # ── Compile 버튼 ─────────────────────────────────────────
    with btn_c1:
        if st.button("⚙️ Compile", use_container_width=True, disabled=_base_disabled):
            with st.spinner(f"`{val_sel_m}` compile 중..."):
                try:
                    _compiled = val_compile_model(
                        project_dir, profile_dir, target_val,
                        val_sel_m, val_sdt, val_edt
                    )
                    st.session_state['val_compiled_sql']   = _compiled
                    st.session_state['val_compiled_model'] = val_sel_m
                    st.session_state['val_before_sql']     = get_before_sql_from_model(
                        project_dir, val_sel_m,
                        f"{val_sdt} 00:00:00",
                        f"{val_edt} 23:59:59"
                    )
                    st.success("✅ Compile 완료 — 아래에서 SQL 및 컬럼을 확인하세요.")
                except RuntimeError as e:
                    st.error(f"❌ compile 실패:\n```\n{e}\n```")

    # ── 검증 실행 버튼 ───────────────────────────────────────
    with btn_c2:
        _compiled_ready = bool(
            st.session_state.get('val_compiled_sql') and
            st.session_state.get('val_compiled_model') == val_sel_m
        )
        _run_help = (
            "날짜 조건을 선택 후 '✅ 쿼리에 반영'을 눌러주세요." if _date_apply_needed else
            "Compile을 먼저 실행하세요." if not _compiled_ready else None
        )
        if st.button(
            "▶️ 검증 실행", type="primary", use_container_width=True,
            disabled=_run_disabled or not _compiled_ready,
            help=_run_help
        ):
            compiled_sql = st.session_state['val_compiled_sql']
            status_box   = st.empty()
            prog         = st.progress(0)

            # ── before_sql 체크된 구문 수집 ─────────────────────
            _bsql_all = [
                s.strip() for s in
                (st.session_state.get('val_before_sql') or '').split(';') if s.strip()
            ]
            _bsql_to_run = [
                s for i, s in enumerate(_bsql_all)
                if st.session_state.get(f'val_bsql_check_{i}_{_vv}', False)
            ]

            # before_sql 체크된 구문이 있으면 단일 커넥션 생성 (temp table 공유)
            _raw_conn = None
            if _bsql_to_run:
                _raw_conn = psycopg2.connect(
                    host=db_config_val['host'], port=db_config_val['port'],
                    dbname=db_config_val['dbname'],
                    user=db_config_val['user'], password=db_config_val['password']
                )

            # before_sql 실행 (temp table 생성 등)
            if _bsql_to_run and _raw_conn:
                status_box.info(f"before_sql 실행 중 ({len(_bsql_to_run)}개 구문)...")
                try:
                    _bsql_cur = _raw_conn.cursor()
                    for _bstmt in _bsql_to_run:
                        _bsql_cur.execute(_bstmt)
                    _raw_conn.commit()
                except Exception as _be:
                    _raw_conn.close()
                    status_box.empty(); prog.empty()
                    st.error(f"❌ before_sql 실행 실패: {_be}"); return

            _conn_arg = _raw_conn  # None이면 각 함수가 자체 커넥션 사용

            # 컬럼 메타 수집 (before_sql 실행과 동일 conn — temp table 참조 가능)
            status_box.info("컬럼 메타 분석 중...")
            try:
                col_meta = val_get_columns_from_query(db_config_val, compiled_sql, conn=_conn_arg)
                all_cols = [c[0] for c in col_meta]
                num_cols = [c[0] for c in col_meta if c[1] in _NUMERIC_OIDS]
                pk_cols  = val_get_pk(db_config_val, val_tgt_sc, val_sel_m)
            except Exception as e:
                if _raw_conn:
                    _raw_conn.close()
                status_box.empty(); prog.empty()
                st.error(f"❌ 컬럼 분석 실패: {e}"); return

            # ── 검증 수행 ────────────────────────────────────────
            src_result, tgt_result = {}, {}
            tbl         = val_sel_m
            src_entry, tgt_entry = {}, {}
            queries     = {}
            date_filter = st.session_state.get('val_date_filter')

            try:
                # COUNT
                if do_count:
                    status_box.info("COUNT 비교 중...")
                    src_entry['count'], _q_src_count = val_src_count(db_config_val, compiled_sql, conn=_conn_arg)
                    tgt_entry['count'], _q_tgt_count = val_tgt_count(
                        db_config_val, val_tgt_sc, tbl, date_filter, conn=_conn_arg
                    )
                    queries['count'] = {'src': _q_src_count, 'tgt': _q_tgt_count}

                # SUM
                if do_sum:
                    status_box.info("SUM 비교 중...")
                    src_entry['sums'], _q_src_sum = val_src_sum(db_config_val, compiled_sql, num_cols, conn=_conn_arg)
                    tgt_entry['sums'], _q_tgt_sum = val_tgt_sum(
                        db_config_val, val_tgt_sc, tbl, num_cols, date_filter, conn=_conn_arg
                    )
                    if _q_src_sum:
                        queries['sum'] = {'src': _q_src_sum, 'tgt': _q_tgt_sum}
                else:
                    src_entry['sums'] = {}
                    tgt_entry['sums'] = {}

                # 샘플
                if do_sample:
                    status_box.info("샘플 데이터 조회 중...")
                    _exclude_cols = set(st.session_state.get('val_exclude_cols', ['dbt_dtm']))

                    src_df, _q_src_sample = val_src_sample(db_config_val, compiled_sql, sample_limit, conn=_conn_arg)
                    src_df = src_df.drop(columns=[c for c in _exclude_cols if c in src_df.columns])
                    src_entry['sample'] = src_df

                    _match_cols = [c for c in all_cols if c not in _exclude_cols]

                    # IN 절 일괄 조회 (개별 쿼리 대비 N배 빠름)
                    _tgt_sample, _q_tgt_batch = val_tgt_sample_batch(
                        db_config_val, val_tgt_sc, tbl, src_df, pk_cols, _match_cols, conn=_conn_arg
                    )
                    tgt_entry['sample'] = _tgt_sample.drop(columns=[c for c in _exclude_cols if c in _tgt_sample.columns])
                    tgt_entry['sample_key_type'] = 'PK' if pk_cols else 'ALL_COLS'
                    tgt_entry['sample_key_cols'] = pk_cols if pk_cols else _match_cols
                    queries['sample'] = {
                        'src': _q_src_sample,
                        'tgt': _q_tgt_batch,
                    }

                src_entry['error'] = None
                tgt_entry['error'] = None

            except Exception as e:
                src_entry['error'] = str(e)
                tgt_entry['error'] = str(e)
            finally:
                if _raw_conn:
                    _raw_conn.close()

            prog.progress(1.0)
            status_box.empty()

            src_result[tbl] = src_entry
            tgt_result[tbl] = tgt_entry

            st.session_state['val_results'] = {
                'model':          val_sel_m,
                'tgt_sc':         val_tgt_sc,
                'sdt':            str(val_sdt),
                'edt':            str(val_edt),
                'src':            src_result,
                'tgt':            tgt_result,
                'num_cols':       num_cols,
                'pk_cols':        pk_cols,
                'all_cols':       all_cols,
                'do_count':       do_count,
                'do_sum':         do_sum,
                'do_sample':      do_sample,
                'queries':        queries,
                'before_sql_ran': _bsql_to_run,
                'run_at':         datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            st.session_state.pop(f'_ins_val_msg_{_vv}', None)  # 새 검증 시 이전 저장 메시지 초기화
            status_box.empty()
            prog.empty()

    # ── compiled SQL 미리보기 (최근 compile 결과) ────────────
    if st.session_state.get('val_compiled_sql'):
        with st.expander(
            f"📄 compiled SQL 보기 — `{st.session_state.get('val_compiled_model', '')}`",
            expanded=False
        ):
            if st.session_state.get('val_before_sql'):
                _bsql_stmts = [s.strip() for s in st.session_state['val_before_sql'].split(';') if s.strip()]
                for _bi, _bstmt in enumerate(_bsql_stmts):
                    _is_del_trunc = bool(re.match(r'\s*(?:delete|truncate)\b', _bstmt, re.IGNORECASE))
                    _cap = "🗑️ before_sql (DELETE / TRUNCATE)" if _is_del_trunc else "📄 before_sql (기타 SQL)"
                    _bch1, _bch2 = st.columns([5, 1])
                    with _bch1:
                        st.caption(_cap)
                    with _bch2:
                        st.checkbox(
                            "검증 전 실행",
                            key=f"val_bsql_check_{_bi}_{_vv}",
                            value=not _is_del_trunc,  # DELETE/TRUNCATE는 기본 미선택
                            help="검증 실행 전 이 구문을 먼저 실행합니다."
                        )
                    st.code(_bstmt, language="sql")
                st.caption("📄 compiled SQL")
            st.code(st.session_state['val_compiled_sql'], language="sql")

    # ── 결과 표시 ────────────────────────────────────────────
    if st.session_state.get('val_results'):
        vr  = st.session_state['val_results']
        tbl = vr['model']
        tgt_sc_label = vr['tgt_sc']

        st.caption(
            f"검증 시각: {vr['run_at']}  |  모델: `{tbl}`  |  "
            f"타겟: `{tgt_sc_label}`  |  "
            f"기간: {vr['sdt']} ~ {vr['edt']}"
        )

        cmp_rows = val_compare_results(vr['src'], vr['tgt'])
        r        = cmp_rows[0]   # 단일 모델
        err      = vr['src'][tbl].get('error')
        queries  = vr.get('queries', {})

        def _show_query_expander(key, label="🔍 실행 쿼리 보기"):
            """소스/타겟 쿼리를 접힌 expander로 표시 (스크롤 고정 높이)"""
            q = queries.get(key)
            if not q:
                return
            _before_ran = vr.get('before_sql_ran') or []
            _src_display = (
                ";\n\n".join(_before_ran) + ";\n\n" + q['src']
                if _before_ran else q['src']
            )
            with st.expander(label, expanded=False):
                qc1, qc2 = st.columns(2)
                with qc1:
                    st.caption("소스 쿼리 (compiled SQL 기반)")
                    st.code(_src_display, language="sql", height=300)
                with qc2:
                    st.caption("타겟 쿼리")
                    st.code(q['tgt'], language="sql", height=300)

        # ── 요약 메트릭 ──────────────────────────────────────
        if not err and (vr['do_count'] or vr['do_sum']):
            st.markdown("#### 📊 요약")
            mc1, mc2, mc3, mc4, mc_btn = st.columns(5)
            if vr['do_count']:
                mc1.metric("소스 COUNT (compile)", f"{r['src_count']:,}" if r['src_count'] is not None else '-')
                mc2.metric(
                    f"타겟 COUNT ({tgt_sc_label})",
                    f"{r['tgt_count']:,}" if r['tgt_count'] is not None else '-',
                    delta=f"{r['count_diff']:+,}" if r['count_diff'] is not None else None,
                    delta_color="off" if r['count_match'] else "inverse"
                )
                mc3.metric("COUNT 일치", "✅" if r['count_match'] else "❌")
            if vr['do_sum']:
                mc4.metric("SUM 전체 일치", "✅" if r['sum_all_match'] else "❌")
            with mc_btn:
                if st.button(
                    "💾 검증결과 저장",
                    key=f"btn_insert_val_{_vv}",
                    type="primary",
                    use_container_width=True,
                ):
                    try:
                        _ins_uuid = insert_verification_to_db(
                            db_config_val, vr, r,
                            st.session_state.get('val_compiled_sql'),
                        )
                        st.session_state[f'_ins_val_msg_{_vv}'] = ('ok', _ins_uuid)
                    except Exception as _ins_e:
                        st.session_state[f'_ins_val_msg_{_vv}'] = ('err', str(_ins_e))
            _ins_msg = st.session_state.get(f'_ins_val_msg_{_vv}')
            if _ins_msg:
                if _ins_msg[0] == 'ok':
                    st.success(f"✅ 저장 완료  UUID: `{_ins_msg[1]}`")
                else:
                    st.error(f"❌ 저장 실패: {_ins_msg[1]}")
            if vr['do_count']:
                _show_query_expander('count')

        # ── 오류 표시 ────────────────────────────────────────
        if err:
            st.error(f"❌ 검증 오류: {err}")

        else:
            # ── 전체 상태 배지 ────────────────────────────────
            _count_ok  = r.get('count_match')    if vr.get('do_count')  else None
            _sum_ok    = r.get('sum_all_match')  if vr.get('do_sum')    else None
            _sample_ok = None
            if vr.get('do_sample'):
                _s2 = vr['src'][tbl].get('sample')
                _t2 = vr['tgt'][tbl].get('sample')
                if _s2 is not None and _t2 is not None and not _s2.empty and not _t2.empty:
                    _cc2 = [c for c in _s2.columns if c in _t2.columns]
                    _hd2 = any(
                        cells_differ(_s2.iloc[_ri][_rc], _t2.iloc[_ri][_rc])
                        for _ri in range(min(len(_s2), len(_t2)))
                        for _rc in _cc2
                    )
                    _sample_ok = not _hd2
                elif _s2 is not None and _t2 is not None:
                    _sample_ok = True

            _all_chk   = [v for v in [_count_ok, _sum_ok, _sample_ok] if v is not None]
            _overall_ok = all(_all_chk) if _all_chk else True

            _badge_html = ' &nbsp; '.join(filter(None, [
                mk_badge('전체',   _overall_ok, '#c0392b'),
                mk_badge('COUNT',  _count_ok,   '#e67e22'),
                mk_badge('SUM',    _sum_ok,     '#2980b9'),
                mk_badge('SAMPLE', _sample_ok,  '#8e44ad'),
            ]))
            st.markdown(_badge_html, unsafe_allow_html=True)

            # ── SUM 상세 ────────────────────────────────────
            if vr['do_sum'] and r['sum_details']:
                st.markdown("#### 🔢 숫자 컬럼 SUM 비교")
                sum_rows = []
                for sd in r['sum_details']:
                    match_icon = ('✅' if sd['match'] else '❌') if sd['match'] is not None else '-'
                    sum_rows.append({
                        '컬럼':     sd['column'],
                        '소스 SUM': sd['src_sum'],
                        '타겟 SUM': sd['tgt_sum'],
                        '차이':     sd['diff'],
                        '일치':     match_icon,
                    })
                sum_df = pd.DataFrame(sum_rows)
                st.dataframe(sum_df, use_container_width=True, hide_index=True)
                _show_query_expander('sum')
                st.download_button(
                    "⬇️ SUM 비교 CSV",
                    data=sum_df.to_csv(index=False, encoding='utf-8-sig'),
                    file_name=f"val_sum_{tbl}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )

            # ── 샘플 비교 ───────────────────────────────────
            if vr['do_sample']:
                src_df = vr['src'][tbl].get('sample')
                tgt_df = vr['tgt'][tbl].get('sample')
                key_type = vr['tgt'][tbl].get('sample_key_type', '-')
                key_cols = vr['tgt'][tbl].get('sample_key_cols', [])

                _key_label = f"**{key_type}** ({', '.join(key_cols)})" if key_type == 'PK' else ', '.join(key_cols)
                st.markdown(
                    f"#### 🔎 샘플 데이터 비교  "
                    f"<span style='font-size:13px;color:gray;'>"
                    f"타겟 조회 기준: {_key_label}</span>",
                    unsafe_allow_html=True
                )
                _show_query_expander('sample')

                # 불일치 셀 사전 계산 (DataFrame 하이라이트 + diff table 공용)
                common_cols = []
                diff_cells  = []
                if src_df is not None and tgt_df is not None and not src_df.empty and not tgt_df.empty:
                    common_cols = [c for c in src_df.columns if c in tgt_df.columns]
                    for idx in range(min(len(src_df), len(tgt_df))):
                        for col in common_cols:
                            sv = src_df.iloc[idx][col]
                            tv = tgt_df.iloc[idx][col]
                            if cells_differ(sv, tv):
                                diff_cells.append({
                                    '행': idx + 1,
                                    '컬럼': col,
                                    '소스 값': sv,
                                    '타겟 값': tv,
                                })

                sp1, sp2 = st.columns(2)
                with sp1:
                    st.caption("소스 (compiled SQL 결과)")
                    if src_df is not None and not src_df.empty:
                        _src_styled = (_style_sample_df(src_df, tgt_df, common_cols, '#ffe0b2')
                                       if common_cols else src_df)
                        st.dataframe(_src_styled, use_container_width=True, hide_index=True)
                    else:
                        st.info("데이터 없음")
                with sp2:
                    st.caption(f"타겟 (`{tgt_sc_label}.{tbl}` 매칭 결과)")
                    if tgt_df is not None and not tgt_df.empty:
                        _tgt_styled = (_style_sample_df(tgt_df, src_df, common_cols, '#ffe0b2')
                                       if common_cols else tgt_df)
                        st.dataframe(_tgt_styled, use_container_width=True, hide_index=True)
                    else:
                        st.info("매칭된 데이터 없음")

                # 소스 행 중 타겟에서 매칭 안 된 행 표시
                if src_df is not None and tgt_df is not None:
                    _src_cnt = len(src_df)
                    _tgt_cnt = len(tgt_df)
                    if _src_cnt != _tgt_cnt:
                        st.warning(
                            f"⚠️ 소스 {_src_cnt}행 중 타겟에서 {_tgt_cnt}행만 매칭됨 "
                            f"({_src_cnt - _tgt_cnt}행 미매칭)"
                        )

                # 불일치 테이블 (이미 계산된 diff_cells 재사용)
                if diff_cells:
                    st.warning(f"⚠️ {len(diff_cells)}개 셀 불일치 발견")
                    diff_df = pd.DataFrame(diff_cells)
                    st.dataframe(diff_df, use_container_width=True, hide_index=True)
                elif src_df is not None and tgt_df is not None and not src_df.empty and not tgt_df.empty:
                    st.success("✅ 샘플 범위 내 모든 값 일치")

                # 샘플 합본 CSV 다운로드
                if src_df is not None and tgt_df is not None:
                    src_exp = src_df.copy(); src_exp.insert(0, '_source', 'compiled_sql')
                    tgt_exp = tgt_df.copy(); tgt_exp.insert(0, '_source', f'{tgt_sc_label}.{tbl}')
                    combined = pd.concat([src_exp, tgt_exp], ignore_index=True)
                    st.download_button(
                        "⬇️ 샘플 비교 CSV",
                        data=combined.to_csv(index=False, encoding='utf-8-sig'),
                        file_name=f"val_sample_{tbl}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                    )
