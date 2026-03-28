# Design Ref: §3.4 — dbt 실행 로그 및 검증 이력 UI 렌더링
# 의존성: db_utils (get_conn), validator (mk_badge)
import json
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from db_utils import get_conn
from validator import mk_badge


def fetch_verification_history(db_config, model_name=None, start_dt=None, end_dt=None):
    """admin.verification_summary 조회 → DataFrame"""
    conditions = []
    params     = []
    if model_name:
        conditions.append("model_name = %s")
        params.append(model_name)
    if start_dt:
        conditions.append("verification_date >= %s")
        params.append(start_dt)
    if end_dt:
        conditions.append("verification_date < %s")
        params.append(end_dt)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"""
        SELECT uuid, model_name, verification_date,
               query_condition_start, query_condition_end,
               count_status, sum_status, sample_status
          FROM admin.verification_summary
        {where}
         ORDER BY verification_date DESC
         LIMIT 200
    """
    with get_conn(db_config) as conn:
        return pd.read_sql(sql, conn, params=params or None)


def fetch_verification_detail(db_config, uuid):
    """uuid 기준으로 count/sum/sample 상세 조회 → dict"""
    result = {}
    with get_conn(db_config) as conn:
        for tbl, key in [
            ('verification_count',  'count'),
            ('verification_sum',    'sum'),
            ('verification_sample', 'sample'),
        ]:
            try:
                df = pd.read_sql(
                    f"SELECT * FROM admin.{tbl} WHERE uuid = %s", conn, params=(uuid,)
                )
                result[key] = df.iloc[0].to_dict() if not df.empty else None
            except Exception:
                result[key] = None
    return result


def fetch_dbt_log(db_config, model_name=None, start_dt=None, end_dt=None):
    """admin.dbt_log 조회 → DataFrame"""
    conditions = []
    params     = []
    if model_name:
        conditions.append("model_name = %s")
        params.append(model_name)
    if start_dt:
        conditions.append("start_time >= %s")
        params.append(start_dt)
    if end_dt:
        conditions.append("start_time < %s")
        params.append(end_dt)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"""
        SELECT dbt_invocation_id, model_name, status,
               start_time, end_time, execution_time_seconds,
               rows_affected, airflow_run_id, variables
          FROM admin.dbt_log
        {where}
         ORDER BY start_time DESC
         LIMIT 200
    """
    with get_conn(db_config) as conn:
        return pd.read_sql(sql, conn, params=params or None)


def render_history_ui(db_config, veri_exists, dbtlog_exists):
    """검증 이력 + dbt 실행 로그 sub-tab UI"""
    st.subheader("📋 이력 조회")

    if not veri_exists and not dbtlog_exists:
        st.warning("admin 스키마 테이블이 존재하지 않습니다. DDL을 먼저 실행해주세요.")
        st.code(
            "python refs/edu/tools/execute_ddl.py airflow/dbconf.json "
            "refs/edu/ddls/admin_00_verification_summary.sql\n"
            "python refs/edu/tools/execute_ddl.py airflow/dbconf.json "
            "refs/edu/ddls/admin_verification_count.sql\n"
            "python refs/edu/tools/execute_ddl.py airflow/dbconf.json "
            "refs/edu/ddls/admin_verification_sum.sql\n"
            "python refs/edu/tools/execute_ddl.py airflow/dbconf.json "
            "refs/edu/ddls/admin_verification_sample.sql",
            language="bash"
        )
        return

    # ── 공통 필터 ────────────────────────────────────────────
    hf1, hf2, hf3, hf4 = st.columns([2, 1, 1, 1])
    with hf1:
        _h_model = st.text_input("모델명 (빈칸=전체)", key="hist_model")
    with hf2:
        _h_sdt = st.date_input("시작일", value=datetime.now().date() - timedelta(days=30), key="hist_sdt")
    with hf3:
        _h_edt = st.date_input("종료일", value=datetime.now().date(), key="hist_edt")
    with hf4:
        st.write("")
        _h_search = st.button("🔍 조회", key="hist_search", use_container_width=True)

    _h_model_val = _h_model.strip() or None
    _h_sdt_val   = datetime.combine(_h_sdt, datetime.min.time())
    _h_edt_val   = datetime.combine(_h_edt, datetime.min.time()) + timedelta(days=1)

    sub_veri, sub_log = st.tabs(["🔍 검증 이력", "📊 실행 로그"])

    # ── 검증 이력 sub-tab ────────────────────────────────────
    with sub_veri:
        if not veri_exists:
            st.warning("admin.verification_summary 테이블이 없습니다.")
        else:
            _veri_refresh = False
            _vthr1, _vthr2 = st.columns([5, 1])
            with _vthr2:
                _veri_refresh = st.button("🔄", key="hist_veri_refresh",
                                          use_container_width=True, help="검증 이력 새로고침")

            if _h_search or _veri_refresh or st.session_state.get('_hist_veri_df') is None:
                try:
                    _veri_df = fetch_verification_history(
                        db_config, _h_model_val, _h_sdt_val, _h_edt_val
                    )
                    st.session_state['_hist_veri_df'] = _veri_df
                except Exception as _e:
                    st.error(f"조회 실패: {_e}")
                    st.session_state['_hist_veri_df'] = pd.DataFrame()

            _veri_df = st.session_state.get('_hist_veri_df', pd.DataFrame())

            if _veri_df.empty:
                with _vthr1:
                    st.info("조회 결과가 없습니다.")
            else:
                def _status_icon(v):
                    if v == 'PASS': return '✅'
                    if v == 'FAIL': return '❌'
                    return '-'

                _disp = _veri_df.copy()
                _disp['COUNT']  = _disp['count_status'].map(_status_icon)
                _disp['SUM']    = _disp['sum_status'].map(_status_icon)
                _disp['SAMPLE'] = _disp['sample_status'].map(_status_icon)
                _disp['verification_date'] = pd.to_datetime(_disp['verification_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
                _disp['기간'] = (
                    _disp['query_condition_start'].astype(str).str[:10] + ' ~ ' +
                    _disp['query_condition_end'].astype(str).str[:10]
                )
                _show_cols = ['verification_date', 'model_name', '기간', 'COUNT', 'SUM', 'SAMPLE', 'uuid']

                with _vthr1:
                    st.caption("행을 클릭하면 상세 조회가 표시됩니다.")
                _veri_sel = st.dataframe(
                    _disp[_show_cols], use_container_width=True, hide_index=True,
                    on_select="rerun", selection_mode="single-row",
                    key="hist_veri_table"
                )

                # 선택 행 → 상세 조회
                _sel_rows = _veri_sel.selection.rows if _veri_sel.selection else []
                if _sel_rows:
                    _sel_idx  = _sel_rows[0]
                    _sel_uuid = _veri_df.iloc[_sel_idx]['uuid']
                    _sel_info = _disp.iloc[_sel_idx]
                    _detail   = fetch_verification_detail(db_config, _sel_uuid)

                    st.markdown(
                        f"#### 📄 상세 조회 — `{_sel_info['model_name']}` "
                        f"<span style='font-size:13px;color:gray;'>{_sel_info['verification_date']} / {_sel_info['기간']}</span>",
                        unsafe_allow_html=True
                    )

                    # ── Summary 배지 ─────────────────────────────
                    def _det_ok(status): return True if status == 'PASS' else (False if status == 'FAIL' else None)
                    _d_count_ok  = _det_ok(_sel_info.get('COUNT', '').replace('✅','PASS').replace('❌','FAIL'))
                    _d_sum_ok    = _det_ok(_sel_info.get('SUM', '').replace('✅','PASS').replace('❌','FAIL'))
                    _d_sample_ok = _det_ok(_sel_info.get('SAMPLE', '').replace('✅','PASS').replace('❌','FAIL'))
                    _d_all_chk   = [v for v in [_d_count_ok, _d_sum_ok, _d_sample_ok] if v is not None]
                    _d_overall   = all(_d_all_chk) if _d_all_chk else True

                    _d_badge_html = ' &nbsp; '.join(filter(None, [
                        mk_badge('전체',   _d_overall,   '#c0392b'),
                        mk_badge('COUNT',  _d_count_ok,  '#e67e22'),
                        mk_badge('SUM',    _d_sum_ok,    '#2980b9'),
                        mk_badge('SAMPLE', _d_sample_ok, '#8e44ad'),
                    ]))
                    st.markdown(_d_badge_html, unsafe_allow_html=True)

                    # ── COUNT metric 요약 ─────────────────────────
                    if _detail.get('count'):
                        _dc = _detail['count']
                        _dm1, _dm2 = st.columns(2)
                        _dm1.metric("소스 COUNT", f"{_dc.get('source_count_result', 0):,}" if _dc.get('source_count_result') is not None else '-')
                        _dm2.metric("타겟 COUNT", f"{_dc.get('target_count_result', 0):,}" if _dc.get('target_count_result') is not None else '-')

                    st.divider()

                    # ── 상세 expander (모두 collapsed) ────────────
                    if _detail.get('count'):
                        with st.expander("COUNT 상세", expanded=False):
                            d = _detail['count']
                            dc1, dc2 = st.columns(2)
                            dc1.metric("소스 COUNT", f"{d.get('source_count_result', 0):,}" if d.get('source_count_result') is not None else '-')
                            dc2.metric("타겟 COUNT", f"{d.get('target_count_result', 0):,}" if d.get('target_count_result') is not None else '-')
                            if d.get('source_count_sql'):
                                with st.expander("쿼리 보기"):
                                    qc1, qc2 = st.columns(2)
                                    qc1.code(d['source_count_sql'], language="sql")
                                    qc2.code(d.get('target_count_sql', ''), language="sql")
                    if _detail.get('sum'):
                        with st.expander("SUM 상세", expanded=False):
                            d = _detail['sum']
                            sc1, sc2 = st.columns(2)
                            with sc1:
                                st.caption("소스 SUM")
                                _src_sums = d.get('source_sum_result') or {}
                                if isinstance(_src_sums, str):
                                    _src_sums = json.loads(_src_sums)
                                st.dataframe(pd.DataFrame(list(_src_sums.items()), columns=['컬럼', '소스 SUM']), hide_index=True)
                            with sc2:
                                st.caption("타겟 SUM")
                                _tgt_sums = d.get('target_sum_result') or {}
                                if isinstance(_tgt_sums, str):
                                    _tgt_sums = json.loads(_tgt_sums)
                                st.dataframe(pd.DataFrame(list(_tgt_sums.items()), columns=['컬럼', '타겟 SUM']), hide_index=True)
                            if d.get('source_sum_sql'):
                                with st.expander("쿼리 보기"):
                                    qs1, qs2 = st.columns(2)
                                    qs1.code(d['source_sum_sql'], language="sql")
                                    qs2.code(d.get('target_sum_sql', ''), language="sql")
                    if _detail.get('sample'):
                        with st.expander("SAMPLE 상세", expanded=False):
                            d = _detail['sample']
                            _src_json = d.get('source_sample_result') or '[]'
                            _tgt_json = d.get('target_sample_result') or '[]'
                            if isinstance(_src_json, str):
                                _src_json = json.loads(_src_json)
                            if isinstance(_tgt_json, str):
                                _tgt_json = json.loads(_tgt_json)
                            sp1, sp2 = st.columns(2)
                            with sp1:
                                st.caption("소스")
                                st.dataframe(pd.DataFrame(_src_json), use_container_width=True, hide_index=True)
                            with sp2:
                                st.caption("타겟")
                                st.dataframe(pd.DataFrame(_tgt_json), use_container_width=True, hide_index=True)
                            if d.get('source_sample_sql'):
                                with st.expander("쿼리 보기"):
                                    ssp1, ssp2 = st.columns(2)
                                    ssp1.code(d['source_sample_sql'], language="sql")
                                    ssp2.code(d.get('target_sample_sql', ''), language="sql")

    # ── 실행 로그 sub-tab ────────────────────────────────────
    with sub_log:
        if not dbtlog_exists:
            st.warning("admin.dbt_log 테이블이 없습니다.")
        else:
            _log_refresh = False
            _lthr1, _lthr2 = st.columns([5, 1])
            with _lthr2:
                _log_refresh = st.button("🔄", key="hist_log_refresh",
                                         use_container_width=True, help="실행 로그 새로고침")

            if _h_search or _log_refresh or st.session_state.get('_hist_log_df') is None:
                try:
                    _log_df = fetch_dbt_log(db_config, _h_model_val, _h_sdt_val, _h_edt_val)
                    st.session_state['_hist_log_df'] = _log_df
                except Exception as _e:
                    st.error(f"조회 실패: {_e}")
                    st.session_state['_hist_log_df'] = pd.DataFrame()

            _log_df = st.session_state.get('_hist_log_df', pd.DataFrame())

            if _log_df.empty:
                with _lthr1:
                    st.info("조회 결과가 없습니다.")
            else:
                _ldisp = _log_df.drop(columns=['variables'], errors='ignore').copy()
                _ldisp['start_time'] = pd.to_datetime(_ldisp['start_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
                _ldisp['status'] = _ldisp['status'].map(
                    lambda v: '✅ success' if v == 'success' else (f'❌ {v}' if v else '-')
                )

                with _lthr1:
                    st.caption("행을 클릭하면 실행 변수 확인 및 검증 탭으로 전송할 수 있습니다.")
                _log_sel = st.dataframe(
                    _ldisp, use_container_width=True, hide_index=True,
                    on_select="rerun", selection_mode="single-row",
                    key="hist_log_table"
                )

                _log_sel_rows = _log_sel.selection.rows if _log_sel.selection else []
                if _log_sel_rows:
                    _log_sel_idx = _log_sel_rows[0]
                    _log_row     = _log_df.iloc[_log_sel_idx]
                    _vars        = _log_row.get('variables')
                    _vars_raw    = (_vars if isinstance(_vars, (dict, list))
                                    else json.loads(_vars)) if _vars else {}
                    if isinstance(_vars_raw, dict):
                        _vars_dict = _vars_raw
                    elif isinstance(_vars_raw, list):
                        # [{"key": "...", "value": "..."}] 형태 변환
                        _vars_dict = {
                            item['key']: item['value']
                            for item in _vars_raw
                            if isinstance(item, dict) and 'key' in item and 'value' in item
                        }
                    else:
                        _vars_dict = {}

                    _ld1, _ld2 = st.columns([4, 1])
                    with _ld1:
                        st.markdown(
                            f"#### 📄 `{_log_row['model_name']}` "
                            f"<span style='font-size:13px;color:gray;'>{_ldisp.iloc[_log_sel_idx]['start_time']}</span>",
                            unsafe_allow_html=True
                        )
                    with _ld2:
                        st.write("")
                        if st.button("🔍 검증 탭으로", key="hist_log_to_val",
                                     use_container_width=True, type="primary"):
                            _v_sdt = _vars_dict.get('data_interval_start', '')
                            _v_edt = _vars_dict.get('data_interval_end', '')
                            try:
                                _v_sdt_d = datetime.strptime(_v_sdt[:19], '%Y-%m-%d %H:%M:%S').date()
                                _v_edt_d = datetime.strptime(_v_edt[:19], '%Y-%m-%d %H:%M:%S').date()
                            except Exception:
                                _v_sdt_d = datetime.now().date() - timedelta(days=4)
                                _v_edt_d = datetime.now().date() - timedelta(days=1)
                            _log_model = _log_row['model_name']
                            st.session_state['runner_to_val'] = {
                                'model': _log_model,
                                'group': st.session_state.get('model_to_group', {}).get(_log_model),
                                'sdt':   _v_sdt_d,
                                'edt':   _v_edt_d,
                            }
                            st.session_state['_open_val_dialog'] = True
                            st.rerun()

                    with st.expander("variables (dbt vars)", expanded=True):
                        if _vars_raw:
                            st.json(_vars_raw)
                        else:
                            st.info("변수 없음")
