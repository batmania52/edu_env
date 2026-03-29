import streamlit as st
import os
import yaml
import json
import subprocess
import pandas as pd
import glob
import re
import shutil
from datetime import datetime, timedelta

from db_utils import (
    get_conn, get_db_config, check_history_tables_exist,
    get_schemas, get_db_tables, get_table_detail,
)
from manifest_utils import (
    convert_to_dbt_ts, check_model_schema_exists, get_dbt_model_hierarchy,
    get_compiled_sql, get_before_sql_from_model,
    cleanup_old_runs_by_date, get_latest_run_results, get_lineage_from_manifest,
)
from validator import render_validation_ui
from history import render_history_ui

# ============================================================
# 0. 설정 및 캐시
# ============================================================
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".dbt_unified_cache.json")

def load_cache():
    """캐시 파일이 존재하면 JSON을 dict로 로드하고, 없거나 오류 시 빈 dict 반환."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(data):
    """dict를 캐시 파일에 JSON 형식으로 저장."""
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ============================================================
# 3. 파일 / YAML 유틸리티 (공통)
# ============================================================
def is_pure_model_yml(full_path):
    """sources 키 없이 models만 있는 YAML 파일 여부"""
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            return data is not None and 'sources' not in data
    except Exception:
        return False

def get_all_yml_files(project_dir):
    """models 하위의 모든 yml 파일 (bak 제외)"""
    yml_files = []
    models_root = os.path.join(project_dir, 'models')
    for root, _, files in os.walk(models_root):
        if 'bak' in root:
            continue
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                yml_files.append(os.path.relpath(os.path.join(root, file), project_dir))
    return sorted(yml_files)

def create_backup(project_dir, rel_path):
    """수정 전 파일을 models/bak 에 타임스탬프 백업"""
    src = os.path.join(project_dir, rel_path)
    if not os.path.exists(src):
        return None
    bak_dir = os.path.join(project_dir, 'models', 'bak')
    os.makedirs(bak_dir, exist_ok=True)
    bak_path = os.path.join(
        bak_dir,
        f"{rel_path.replace(os.sep, '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
    )
    shutil.copy2(src, bak_path)
    return bak_path

def find_existing_model_definition(project_dir, model_name):
    """프로젝트 내 YAML 파일에서 특정 모델 정의 위치 탐색"""
    models_root = os.path.join(project_dir, 'models')
    for root, _, files in os.walk(models_root):
        if 'bak' in root:
            continue
        for file in files:
            if not file.endswith(('.yml', '.yaml')):
                continue
            fp = os.path.join(root, file)
            if not is_pure_model_yml(fp):
                continue
            try:
                with open(fp, 'r', encoding='utf-8') as yf:
                    data = yaml.safe_load(yf)
                    if data and 'models' in data:
                        for m in data['models']:
                            if m.get('name') == model_name:
                                return os.path.relpath(fp, project_dir), m
            except Exception:
                continue
    return None, None

def remove_model_from_others(project_dir, model_name, target_rel_path):
    """target 파일 외 다른 YAML에서 동일 모델명 제거 (중복 방지)"""
    for yml in get_all_yml_files(project_dir):
        fp = os.path.join(project_dir, yml)
        if yml == target_rel_path or not is_pure_model_yml(fp):
            continue
        try:
            with open(fp, 'r', encoding='utf-8') as yf:
                data = yaml.safe_load(yf)
            if data and 'models' in data:
                orig_len = len(data['models'])
                data['models'] = [m for m in data['models'] if m.get('name') != model_name]
                if len(data['models']) < orig_len:
                    create_backup(project_dir, yml)
                    with open(fp, 'w', encoding='utf-8') as yf:
                        yaml.dump(data, yf, sort_keys=False, allow_unicode=True, default_flow_style=False)
        except Exception:
            continue

def build_model_entry(model_name, columns, pk, table_comment):
    """YAML에 삽입할 모델 dict 생성 (공통 포맷)"""
    entry = {
        "name": model_name,
        "description": table_comment,
        "config": {
            "materialized": "incremental",
            "incremental_strategy": "append"
        },
        "columns": columns
    }
    if pk:
        entry["config"]["unique_key"] = pk[0] if len(pk) == 1 else pk
    return entry

# ============================================================
# 4. Runner 전용 유틸리티 — manifest_utils_new 모듈로 이동한 함수는 import로 대체
# ============================================================

def _render_before_sql(before_sql_text):
    """before_sql을 구문 유형별로 분리하여 표시.
    DELETE/TRUNCATE: 🗑️ 캡션, 기타 SQL: 📄 캡션으로 구분.
    """
    _stmts = [s.strip() for s in before_sql_text.split(';') if s.strip()]
    for _stmt in _stmts:
        if re.match(r'\s*(?:delete|truncate)\b', _stmt, re.IGNORECASE):
            st.caption("🗑️ before_sql (DELETE / TRUNCATE)")
        else:
            st.caption("📄 before_sql (기타 SQL)")
        st.code(_stmt, language="sql")

# ============================================================
# 5. 세션 상태 초기화
# ============================================================
# 앱 최초 로드 시 한 번만 실행되는 초기화 블록 — 이후 rerun에서는 건너뜀
if 'init_unified' not in st.session_state:
    st.session_state.update({
        'init_unified': True,
        'compiled_sql': None,
        'before_sql': None,
        'up_list': {}, 'down_list': {},
        'full_up_list': {}, 'full_down_list': {},
        'lineage_focus_model': None,
        'up_depth': 0, 'down_depth': 0,        # 리니지 시각화 전용
        # Run 설정 (모델 선택 영역)
        'cb_upstream': False, 'cnt_upstream': 1,
        'cb_downstream': False, 'cnt_downstream': 1,
        'ms_exclude': [],
        'cmd_reviewed': False,
        'sb_group': "📂 모델 그룹을 선택하세요",
        'gen_analysis_data': None,
        'gen_is_applied': False,
        'start_dt_widget': datetime.now().date() - timedelta(days=4),
        'end_dt_widget': datetime.now().date() - timedelta(days=1),
        # Generator 전용
        'gen_sc': None,
        'gen_db_config': None,
        'runner_to_gen': None,   # Runner → Generator 탭 간 이관 데이터 (YAML 미등록 모델 전달)
        'last_run_df': None,     # 마지막 dbt run 결과 (rerun 후에도 유지)
        # 검증 탭 전용
        'val_results': None,
        'val_src_sc': None,
        'val_tgt_sc': None,
        'val_sdt': datetime.now().date() - timedelta(days=4),
        'val_edt': datetime.now().date() - timedelta(days=1),
        'val_sb_group': "📂 그룹 선택",
        'val_sb_model': "📂 모델 선택",
        # 날짜 필터
        'val_date_filter': None,   # 확정된 date_filter dict (버튼 클릭 후 반영)
        'val_direct_where': "",    # 직접 입력 WHERE 절
        'val_custom_start': "",
        'val_custom_end': "",
        # 샘플 비교 제외 컬럼
        'val_exclude_cols': ['dbt_dtm'],
        # 탭별 위젯 key 버전 카운터 (초기화 시 증가 → Streamlit이 새 위젯으로 인식)
        '_gen_key_ver': 0,
        '_val_key_ver': 0,
        # 이력 탭 캐시
        '_hist_veri_df': None,
        '_hist_log_df':  None,
    })

# 기존 세션에서 신규 키가 누락된 경우 보완
st.session_state.setdefault('_gen_key_ver', 0)
st.session_state.setdefault('_val_key_ver', 0)
st.session_state.setdefault('full_up_list', {})
st.session_state.setdefault('full_down_list', {})
st.session_state.setdefault('lineage_focus_model', None)
st.session_state.setdefault('cb_upstream', False)
st.session_state.setdefault('cnt_upstream', 1)
st.session_state.setdefault('cb_downstream', False)
st.session_state.setdefault('cnt_downstream', 1)
st.session_state.setdefault('ms_exclude', [])

def on_ui_change():
    """날짜/모드 위젯 변경 시 호출 — SQL/실행 상태만 초기화. 리니지는 건드리지 않음."""
    st.session_state.compiled_sql = None
    st.session_state.before_sql = None
    st.session_state.cmd_reviewed = False
    st.session_state['last_run_df']   = None
    st.session_state['runner_to_val'] = None

def reset_lineage():
    """리니지 시각화 state 전체 초기화. exclude 선택은 유지."""
    st.session_state['up_list']             = {}
    st.session_state['down_list']           = {}
    st.session_state['full_up_list']        = {}
    st.session_state['full_down_list']      = {}
    st.session_state['lineage_focus_model'] = None
    st.session_state['up_depth']            = 0
    st.session_state['down_depth']          = 0

def on_model_change():
    """모델 selectbox 변경 시 호출 — SQL/실행 상태 + 리니지 모두 초기화."""
    on_ui_change()
    reset_lineage()
    st.session_state['ms_exclude'] = []  # 모델 변경 시 exclude는 무효화

def on_counter_change():
    """Upstream/Downstream counter 변경 시 호출 — SQL 초기화 + exclude 선택 리셋."""
    on_ui_change()
    st.session_state['ms_exclude'] = []

def focus_lineage_model(model_name):
    """리니지 버튼 클릭 시 해당 모델로 포커스 이동 (순수 시각화 전용)."""
    st.session_state['lineage_focus_model'] = model_name
    st.session_state['_lineage_auto_run']   = True
    st.session_state['up_list']        = {}
    st.session_state['down_list']      = {}
    st.session_state['full_up_list']   = {}
    st.session_state['full_down_list'] = {}

# ============================================================
# 6. 앱 레이아웃
# ============================================================
st.set_page_config(page_title="dbt Unified Dashboard", layout="wide")
cache = load_cache()

# 현재 선택 모델 버튼 강조 스타일 (disabled 버튼 색상 override)
st.markdown("""
<style>
button[data-testid="stBaseButton-secondary"][disabled][key="lineage_center_model"],
div[data-testid="stButton"] button:disabled#lineage_center_model {
    opacity: 1 !important;
    color: #ff4b4b !important;
    font-weight: bold !important;
    border-color: #ff4b4b !important;
    cursor: default !important;
}
/* key 속성으로 직접 타겟팅이 안될 경우 data-testid 기반으로 처리 */
div[data-testid="stButton"]:has(button:disabled) button:disabled {
    opacity: 1 !important;
}
</style>
""", unsafe_allow_html=True)

# --- 사이드바 ---
with st.sidebar:
    st.header("⚙️ dbt 공통 설정")
    _default_project = os.path.join(os.path.expanduser("~"), "projects", "edu_project", "dbt_projects", "edu001")
    project_dir = st.text_input("Project Directory", value=cache.get("project_dir", _default_project))
    profile_dir = st.text_input("Profiles Directory", value=cache.get("profile_dir", os.path.expanduser("~/.dbt")))
    if st.button("설정 저장"):
        save_cache({"project_dir": project_dir, "profile_dir": profile_dir,
                    "target": st.session_state.get("sidebar_target", cache.get("target"))})
        st.success("저장됨")
    st.divider()
    if st.button("🔄 초기화", key="btn_reset_all", use_container_width=True):
        st.session_state.clear()
        st.rerun()

# Target 선택
target_val = None
profiles_path = os.path.join(profile_dir, 'profiles.yml')
if os.path.exists(profiles_path):
    with open(profiles_path, 'r', encoding='utf-8') as f:
        pfs = yaml.safe_load(f)
        pk  = list(pfs.keys())[0]
        _target_opts    = list(pfs[pk]['outputs'].keys())
        _cached_target  = cache.get("target")
        _target_default = _target_opts.index(_cached_target) if _cached_target in _target_opts else 0
        target_val = st.sidebar.selectbox("🎯 Target 선택", _target_opts, index=_target_default, key="sidebar_target")

# 이력 탭 레이블 — 테이블 미존재 시 시각적 표시
_hist_db_config = get_db_config(profile_dir, target_val) if target_val else None
_veri_exists, _dbtlog_exists = (
    check_history_tables_exist(_hist_db_config) if _hist_db_config else (False, False)
)
_history_tab_label = "📋 이력" if (_veri_exists or _dbtlog_exists) else "📋 이력 (미설정)"

tab_runner, tab_generator, tab_validator, tab_history = st.tabs([
    "🚀 dbt Runner", "📝 YAML Generator", "🔍 데이터 검증", _history_tab_label
])

# ============================================================
# TAB 1 : dbt Runner
# ============================================================
with tab_runner:
    hierarchy, m2g = get_dbt_model_hierarchy(project_dir)
    st.session_state.model_to_group = m2g

    # 위젯 key를 직접 조작하려면 해당 위젯이 생성되기 전에 session_state를 수정해야 함.
    # rerun 직후 이 시점에서 처리하지 않으면 이미 렌더링된 위젯의 key가 무시된다.
    # pending flag 처리 — 위젯 생성 전에 실행해야 함
    if st.session_state.pop('_runner_reset_pending', False):
        st.session_state['sb_group']          = "📂 모델 그룹을 선택하세요"
        st.session_state.pop('sb_model', None)
        st.session_state.pop('rb_run_mode', None)
        st.session_state['start_dt_widget']   = datetime.now().date() - timedelta(days=4)
        st.session_state['end_dt_widget']     = datetime.now().date() - timedelta(days=1)

    cr1, cr2, cr3 = st.columns([2, 1, 1])
    with cr1:
        gp_p = "📂 모델 그룹을 선택하세요"
        sel_g = st.selectbox("📁 그룹 선택", [gp_p] + list(hierarchy.keys()),
                             key="sb_group", on_change=on_model_change)
        mp_p = "📂 모델을 선택하세요"
        sel_m = None
        if sel_g != gp_p:
            mv = st.selectbox("📂 모델 선택", [mp_p] + hierarchy[sel_g],
                              key="sb_model", on_change=on_model_change)
            if mv != mp_p:
                sel_m = mv
    with cr2:
        run_m = st.radio("🏃 모드", ["manual", "schedule"],
                         key="rb_run_mode", horizontal=True, on_change=on_ui_change)
    with cr3:
        st.write("")
        if st.button("🔄 초기화", key="btn_reset_runner", use_container_width=True):
            # 위젯 key가 아닌 값들은 즉시 초기화
            st.session_state['up_list']             = {}
            st.session_state['down_list']           = {}
            st.session_state['full_up_list']        = {}
            st.session_state['full_down_list']      = {}
            st.session_state['lineage_focus_model'] = None
            st.session_state['up_depth']            = 0
            st.session_state['down_depth']          = 0
            st.session_state['cb_upstream']         = False
            st.session_state['cnt_upstream']        = 1
            st.session_state['cb_downstream']       = False
            st.session_state['cnt_downstream']      = 1
            st.session_state['ms_exclude']          = []
            st.session_state['compiled_sql']        = None
            st.session_state['before_sql']          = None
            st.session_state['cmd_reviewed']        = False
            st.session_state['runner_to_gen']       = None
            st.session_state['runner_to_val']       = None
            # 위젯 key는 다음 rerun에서 위젯 생성 전에 처리
            st.session_state['_runner_reset_pending'] = True
            st.rerun()

    schema_ok, sc_err = check_model_schema_exists(project_dir, sel_m) if sel_m else (True, None)

    # 리니지 포커스 모델 자동 분석 (focus_lineage_model 콜백 후)
    if st.session_state.pop('_lineage_auto_run', False):
        focus_m = st.session_state.get('lineage_focus_model') or sel_m
        if focus_m:
            up, dn, err = get_lineage_from_manifest(project_dir, focus_m, 10, 10)
            if not err:
                full_up = up or {}
                full_dn = dn or {}
                st.session_state['full_up_list']   = full_up
                st.session_state['full_down_list'] = full_dn
                st.session_state.up_depth   = max(full_up.keys()) if full_up else 0
                st.session_state.down_depth = max(full_dn.keys()) if full_dn else 0
                st.session_state['up_list']   = {d: v for d, v in full_up.items() if d <= st.session_state.up_depth}
                st.session_state['down_list'] = {d: v for d, v in full_dn.items() if d <= st.session_state.down_depth}
                st.rerun()


    if sel_m:
        if not schema_ok:
            st.error(sc_err)
            # YAML Generator 탭으로 이관할 정보 저장
            st.session_state['runner_to_gen'] = {
                'model': sel_m,
                'group': m2g.get(sel_m),
            }
            st.info("💡 YAML Generator 탭에서 해당 모델의 스키마를 바로 생성할 수 있습니다.")
        st.divider()

        # ── Run 설정: Upstream / Downstream / Exclude ──────────────────────
        rx1, rx2, rx3, rx4 = st.columns(4)
        with rx1:
            cb_up = st.checkbox("⬆️ Upstream", key="cb_upstream",
                                on_change=on_ui_change, disabled=not schema_ok)
        with rx2:
            cnt_up_val = st.number_input("Upstream Depth", min_value=1, max_value=10, step=1,
                                         key="cnt_upstream", label_visibility="collapsed",
                                         disabled=not cb_up, on_change=on_counter_change)
        with rx3:
            cb_dn = st.checkbox("⬇️ Downstream", key="cb_downstream",
                                on_change=on_ui_change, disabled=not schema_ok)
        with rx4:
            cnt_dn_val = st.number_input("Downstream Depth", min_value=1, max_value=10, step=1,
                                         key="cnt_downstream", label_visibility="collapsed",
                                         disabled=not cb_dn, on_change=on_counter_change)

        if cb_up or cb_dn:
            # 리니지 데이터 없으면 지금 즉시 로드 (manifest 캐시 사용)
            if not (st.session_state.get('full_up_list') or st.session_state.get('full_down_list')):
                _l_up, _l_dn, _l_err = get_lineage_from_manifest(project_dir, sel_m, 10, 10)
                if not _l_err:
                    st.session_state['full_up_list']   = _l_up or {}
                    st.session_state['full_down_list'] = _l_dn or {}
                    st.rerun()
            _ex_opts = []
            if cb_up:
                for _d, _models in st.session_state.get('full_up_list', {}).items():
                    if _d <= int(cnt_up_val):
                        for _m in (_models if isinstance(_models, (list, set)) else []):
                            if "(Source)" not in _m:
                                _ex_opts.append(f"⬆️ {_m}")
            if cb_dn:
                for _d, _models in st.session_state.get('full_down_list', {}).items():
                    if _d <= int(cnt_dn_val):
                        for _m in (_models if isinstance(_models, (list, set)) else []):
                            _ex_opts.append(f"⬇️ {_m}")
            _ex_opts = sorted(set(_ex_opts))
            if _ex_opts:
                st.multiselect("🚫 Exclude 모델", options=_ex_opts,
                               key="ms_exclude", on_change=on_ui_change)
            else:
                st.caption("💡 해당 depth 범위에 exclude 가능한 모델이 없습니다.")

        st.divider()

        # Lineage 컨트롤
        _has_full = bool(st.session_state.get('full_up_list') or st.session_state.get('full_down_list'))
        dc1, dc2, dc3, dc4 = st.columns([1, 1, 1, 1])
        with dc1:
            u1, u2 = st.columns(2)
            if u1.button("➖", key="um", disabled=not _has_full):
                st.session_state.up_depth = max(0, st.session_state.up_depth - 1)
                full_up = st.session_state.get('full_up_list', {})
                st.session_state['up_list'] = {d: v for d, v in full_up.items() if d <= st.session_state.up_depth}
            if u2.button("➕", key="up", disabled=not _has_full):
                st.session_state.up_depth += 1
                full_up = st.session_state.get('full_up_list', {})
                st.session_state['up_list'] = {d: v for d, v in full_up.items() if d <= st.session_state.up_depth}
            st.write(f"**업스트림 Depth: {st.session_state.up_depth}**")
        with dc2:
            if st.button("🧬 Lineage 분석", use_container_width=True, disabled=not schema_ok):
                with st.spinner("분석 중..."):
                    # 분석 시작: 포커스를 현재 선택 모델로 초기화
                    st.session_state['lineage_focus_model'] = sel_m
                    up, dn, err = get_lineage_from_manifest(project_dir, sel_m, 10, 10)
                    if err:
                        st.error(err)
                    else:
                        full_up = up or {}
                        full_dn = dn or {}
                        st.session_state['full_up_list']   = full_up
                        st.session_state['full_down_list'] = full_dn
                        # 탐색 depth를 최대 가용값으로 자동 설정
                        st.session_state.up_depth   = max(full_up.keys()) if full_up else 0
                        st.session_state.down_depth = max(full_dn.keys()) if full_dn else 0
                        st.session_state['up_list']   = {d: v for d, v in full_up.items() if d <= st.session_state.up_depth}
                        st.session_state['down_list'] = {d: v for d, v in full_dn.items() if d <= st.session_state.down_depth}
                        st.rerun()  # dc1/dc3 depth 표시를 업데이트된 값으로 재렌더링
        with dc3:
            d1, d2 = st.columns(2)
            if d1.button("➖", key="dm", disabled=not _has_full):
                st.session_state.down_depth = max(0, st.session_state.down_depth - 1)
                full_dn = st.session_state.get('full_down_list', {})
                st.session_state['down_list'] = {d: v for d, v in full_dn.items() if d <= st.session_state.down_depth}
            if d2.button("➕", key="dp", disabled=not _has_full):
                st.session_state.down_depth += 1
                full_dn = st.session_state.get('full_down_list', {})
                st.session_state['down_list'] = {d: v for d, v in full_dn.items() if d <= st.session_state.down_depth}
            st.write(f"**다운스트림 Depth: {st.session_state.down_depth}**")
        with dc4:
            st.write("")
            if st.button("🔄 Lineage 초기화", use_container_width=True, disabled=not _has_full):
                reset_lineage()   # up/down_depth 초기화, exclude 유지
                st.rerun()

        # Lineage 결과 표시
        if schema_ok and (st.session_state.up_list or st.session_state.down_list):
            up_d   = st.session_state.up_list
            down_d = st.session_state.down_list
            max_up_d   = max(up_d.keys(),   default=0)
            max_down_d = max(down_d.keys(), default=0)
            total_cols = max_up_d + 1 + max_down_d
            font_size  = max(9, 14 - max(0, total_cols - 4))

            if font_size < 14:
                st.markdown(f"""<style>
div[data-testid="stHorizontalBlock"]:has(button[key="lineage_center_model"]) button p {{
    font-size: {font_size}px !important;
}}
</style>""", unsafe_allow_html=True)

            all_cols = st.columns(total_cols) if total_cols > 0 else []
            lineage_center = st.session_state.get('lineage_focus_model') or sel_m

            # 업스트림
            for d in range(max_up_d, 0, -1):
                col_idx = max_up_d - d
                with all_cols[col_idx]:
                    st.caption(f"⬆️ Depth {d}")
                    for m in up_d.get(d, []):
                        if "(Source)" in m:
                            st.button(f"🔌 {m}", key=f"src_{d}_{m}",
                                      use_container_width=True, disabled=True)
                        else:
                            st.button(m, key=f"ub_{d}_{m}",
                                      on_click=focus_lineage_model, args=(m,),
                                      use_container_width=True)

            # 포커스 모델 (중앙)
            with all_cols[max_up_d]:
                is_focus_diff = lineage_center != sel_m
                st.caption("▶ 포커스 모델" if is_focus_diff else "▶ 현재 모델")
                st.button(
                    lineage_center, key="lineage_center_model",
                    use_container_width=True, disabled=True
                )

            # 다운스트림
            for d in range(1, max_down_d + 1):
                col_idx = max_up_d + d
                with all_cols[col_idx]:
                    st.caption(f"⬇️ Depth {d}")
                    for m in down_d.get(d, []):
                        st.button(m, key=f"db_{d}_{m}",
                                  on_click=focus_lineage_model, args=(m,),
                                  use_container_width=True)

    st.divider()

    # 날짜 선택
    dr1, dr2 = st.columns(2)
    with dr1:
        sdt = st.date_input("📅 시작", key="start_dt_widget",
                            on_change=on_ui_change, disabled=not sel_m or not schema_ok)
    with dr2:
        edt = st.date_input("📅 종료", key="end_dt_widget",
                            on_change=on_ui_change, disabled=not sel_m or not schema_ok)

    date_invalid = False
    if sel_m and schema_ok and edt < sdt:
        st.error("⚠️ 종료 날짜는 시작 날짜보다 빠를 수 없습니다.")
        date_invalid = True

    # Compile / Run
    if sel_m and target_val:
        _cb_up  = st.session_state.get('cb_upstream', False)
        _cnt_up = int(st.session_state.get('cnt_upstream', 1))
        _cb_dn  = st.session_state.get('cb_downstream', False)
        _cnt_dn = int(st.session_state.get('cnt_downstream', 1))
        up_part = f"{_cnt_up}+" if _cb_up else ""
        dn_part = f"+{_cnt_dn}" if _cb_dn else ""
        slc = f"{up_part}{sel_m}{dn_part}"

        _ms_ex    = st.session_state.get('ms_exclude', []) or []
        clean_ex  = [m.split(" ", 1)[1] for m in _ms_ex] if _ms_ex else []
        v_j = json.dumps({
            "data_interval_start": convert_to_dbt_ts(sdt),
            "data_interval_end": convert_to_dbt_ts(edt, True),
            "run_mode": run_m
        })
        args = ["--select", slc]
        if clean_ex:
            args += ["--exclude"] + clean_ex
        args += ["--target", target_val, "--vars", v_j,
                 "--project-dir", project_dir, "--profiles-dir", profile_dir]

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("⚙️ Compile")
            if st.button("DBT Compile", use_container_width=True,
                         disabled=not schema_ok or date_invalid):
                with st.spinner("컴파일 중..."):
                    rc = subprocess.run(["dbt", "-q", "compile"] + args,
                                        cwd=project_dir, capture_output=True, text=True)
                    if rc.returncode == 0:
                        st.session_state.compiled_sql = get_compiled_sql(project_dir, sel_m)
                        st.session_state.before_sql = get_before_sql_from_model(
                            project_dir, sel_m,
                            convert_to_dbt_ts(sdt),
                            convert_to_dbt_ts(edt, True)
                        )
                    else:
                        st.error(rc.stderr)
            if st.session_state.before_sql:
                _render_before_sql(st.session_state.before_sql)
            if st.session_state.compiled_sql:
                st.caption("📄 compiled SQL")
                st.code(st.session_state.compiled_sql, language="sql")

        with c2:
            st.subheader("🚀 Run")
            run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            tp = f"target/run_{run_id}"

            if st.button("🔍 Command Review", use_container_width=True,
                         disabled=not schema_ok or date_invalid):
                st.session_state.cmd_reviewed = True
                # args에서 --vars 값만 따옴표 처리하여 표시
                display_args = []
                i = 0
                while i < len(args):
                    if args[i] == "--vars" and i + 1 < len(args):
                        display_args += ["--vars", f"'{args[i+1]}'"]
                        i += 2
                    else:
                        display_args.append(args[i])
                        i += 1
                review_cmd = ["dbt", "run"] + display_args
                st.info(" ".join(review_cmd))

            if st.button("▶️ Run Execution", type="primary", use_container_width=True,
                         disabled=not st.session_state.cmd_reviewed or not schema_ok or date_invalid):
                with st.spinner("실행 중..."):
                    rr = subprocess.run(["dbt", "run"] + args + ["--target-path", tp],
                                        cwd=project_dir, capture_output=True, text=True)
                if rr.returncode == 0:
                    st.success("Done!")
                    cleanup_old_runs_by_date(project_dir)
                    rdf = get_latest_run_results(project_dir)
                    if rdf:
                        # 현재 select 범위(업/다운스트림 포함) 모델만 필터링
                        _sel_models = set()
                        for _, _ms in st.session_state.up_list.items():
                            _sel_models.update(_ms)
                        _sel_models.add(sel_m)
                        for _, _ms in st.session_state.down_list.items():
                            _sel_models.update(_ms)
                        _df = pd.DataFrame(rdf)
                        _filtered = _df[_df['Model Name'].isin(_sel_models)] if _sel_models else _df
                        # 실행 결과를 session_state에 저장해 rerun 후에도 유지
                        st.session_state['last_run_df'] = _filtered if not _filtered.empty else _df
                    # 검증 이력 저장
                    st.session_state['runner_to_val'] = {
                        'model': sel_m,
                        'group': m2g.get(sel_m),
                        'sdt':   sdt,
                        'edt':   edt,
                    }
                else:
                    st.session_state['last_run_df'] = None
                    st.error("❌ Failed")
                    st.code(rr.stdout + (rr.stderr or ""), language="bash")

            # 실행 결과 표시 (session_state에서 유지)
            if st.session_state.get('last_run_df') is not None:
                st.dataframe(st.session_state['last_run_df'],
                             use_container_width=True, hide_index=True)

            # 검증 실행 버튼 — 직전 실행 이력이 있을 때 상시 표시
            _r2v_exists = bool(st.session_state.get('runner_to_val'))
            if _r2v_exists:
                if st.button("🔍 검증 실행", type="primary", use_container_width=True,
                             key="btn_open_val_dialog"):
                    st.session_state['_open_val_dialog'] = True
                    st.rerun()  # tab_generator st.stop() 전에 rerun 종료

# ============================================================
# TAB 2 : YAML Generator (구 Streamlit Generator + 구 CLI 통합)
# ============================================================
with tab_generator:
    _gh1, _gh2 = st.columns([4, 1])
    with _gh1:
        st.subheader("🛠 YAML Generator")
    with _gh2:
        st.write("")
        if st.button("🔄 초기화", key="btn_reset_gen", use_container_width=True):
            st.session_state['_gen_key_ver']      += 1   # 위젯 key 변경 → 자동 초기화
            st.session_state['gen_sc']            = None
            st.session_state['gen_analysis_data'] = None
            st.session_state['gen_is_applied']    = False
            st.session_state['runner_to_gen']     = None
            st.rerun()

    _gv = st.session_state['_gen_key_ver']

    if not target_val:
        st.warning("사이드바에서 Target을 먼저 선택해주세요.")
        st.stop()

    db_config = get_db_config(profile_dir, target_val)
    if not db_config:
        st.error("DB 설정을 불러올 수 없습니다. profiles.yml 경로를 확인하세요.")
        st.stop()

    # --- DB 연결 및 스키마/테이블 선택 ---
    try:
        schemas = get_schemas(db_config)
    except Exception as e:
        st.error(f"DB 연결 실패: {e}")
        st.stop()

    # ── Runner 이력 가져오기 버튼 ────────────────────────────
    r2g = st.session_state.get('runner_to_gen')
    if r2g:
        gc1, gc2 = st.columns([3, 1])
        with gc1:
            st.caption(
                f"📋 Runner 미등록 모델 — `{r2g['model']}`  "
                f"| YAML이 없어 실행이 불가한 모델입니다."
            )
        with gc2:
            if st.button("📥 이력 가져오기", key="gen_r2g_btn", use_container_width=True):
                # 모델명으로 DB에서 스키마 자동 탐색
                try:
                    with get_conn(db_config) as _conn:
                        _cur = _conn.cursor()
                        _cur.execute(
                            "SELECT table_schema FROM information_schema.tables "
                            "WHERE table_name = %s AND table_type = 'BASE TABLE' "
                            "ORDER BY table_schema LIMIT 1",
                            (r2g['model'],)
                        )
                        _row = _cur.fetchone()
                    _found_sc = _row[0] if _row else None
                except Exception:
                    _found_sc = None

                # 스키마 selectbox 및 테이블 multiselect key 직접 덮어쓰기
                if _found_sc and _found_sc in schemas:
                    st.session_state['gen_sb_schema'] = _found_sc
                    st.session_state['gen_sc']        = _found_sc
                st.session_state['gen_ms_tables']  = [r2g['model']]
                st.session_state['runner_to_gen']  = None  # 배너 클리어
                st.rerun()

    def _gen_reset_analysis():
        """스키마/테이블 선택 변경 시 이전 분석 결과를 초기화."""
        st.session_state.gen_analysis_data = None
        st.session_state.gen_is_applied    = False

    cg1, cg2 = st.columns(2)
    with cg1:
        gen_sc = st.selectbox("📂 DB Schema 선택", schemas,
                              index=None, placeholder="📂 스키마를 선택하세요",
                              key=f"gen_sb_schema_{_gv}",
                              on_change=_gen_reset_analysis)

    # gen_sc 세션 동기화 (분석 시작 버튼 클릭 시점 스코프 보장)
    if gen_sc:
        st.session_state['gen_sc'] = gen_sc

    if gen_sc:
        try:
            all_tables = get_db_tables(db_config, gen_sc)
            st.multiselect("📊 대상 테이블 선택", all_tables, key=f"gen_ms_tables_{_gv}",
                           on_change=_gen_reset_analysis)
        except Exception as e:
            st.error(f"테이블 목록 조회 실패: {e}")

    # --- 분석 실행 ---
    if st.button("🔍 분석 시작 (Run Analysis)", type="primary"):
        current_sc = st.session_state.get('gen_sc')
        selected_tables = st.session_state.get(f'gen_ms_tables_{_gv}', [])

        if not selected_tables:
            st.warning("테이블을 1개 이상 선택하세요.")
        else:
            st.session_state.gen_is_applied = False
            res_list = []

            with st.spinner("테이블 분석 중..."):
                for t in selected_tables:
                    try:
                        cols, pk, t_comment = get_table_detail(db_config, current_sc, t)
                        new_def = build_model_entry(t, cols, pk, t_comment)
                        ex_file, ex_def = find_existing_model_definition(project_dir, t)
                        is_same = (json.dumps(ex_def, sort_keys=True) ==
                                   json.dumps(new_def, sort_keys=True)) if ex_def else False
                        res_list.append({
                            "name": t,
                            "status": f"존재함 ({ex_file})" if ex_file else "신규",
                            "is_same": is_same,
                            "ex_file": ex_file,
                            "model_def": new_def,
                            "applied": False
                        })
                    except Exception as e:
                        st.warning(f"⚠️ {t} 분석 실패: {e}")

            st.session_state.gen_analysis_data = res_list
            st.rerun()

    # --- 분석 결과 표시 및 적용 ---
    # 현재 선택된 테이블 목록과 직전 분석 결과의 테이블 목록이 일치할 때만 결과를 표시.
    # 선택이 바뀐 경우 stale 결과 표시를 방지.
    _current_tables  = st.session_state.get(f'gen_ms_tables_{_gv}', [])
    _analyzed_tables = [itm['name'] for itm in (st.session_state.gen_analysis_data or [])]
    _result_valid    = (
        st.session_state.gen_analysis_data and
        sorted(_current_tables) == sorted(_analyzed_tables)
    )
    if _result_valid:
        st.divider()
        ap_tasks = []
        v_yml = [f for f in get_all_yml_files(project_dir)]

        for idx, itm in enumerate(st.session_state.gen_analysis_data):
            if itm['applied']:
                color_tag = ":green"
                label_tag = "✅ 반영됨"
            elif "존재함" in itm['status']:
                color_tag = ":orange"
                label_tag = itm['status']
            else:
                color_tag = ":blue"
                label_tag = itm['status']

            if not itm['applied'] and itm['is_same']:
                label_tag += " | 내용 동일"

            with st.expander(
                f"📄 {itm['name']} | {color_tag}[{label_tag}]",
                expanded=not (itm['applied'] or itm['is_same'])
            ):
                if itm['applied']:
                    st.success("이미 반영되었습니다.")
                else:
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        do_apply = st.checkbox(
                            "반영 포함", key=f"chk_{itm['name']}",
                            value=not itm['is_same']
                        )
                    with col2:
                        path_opts = v_yml + ["[신규 파일 생성]"]
                        default_idx = (path_opts.index(itm['ex_file'])
                                       if itm['ex_file'] in path_opts
                                       else len(path_opts) - 1)
                        target_path = st.selectbox(
                            "저장 경로", path_opts,
                            index=default_idx, key=f"path_{itm['name']}"
                        )
                        if target_path == "[신규 파일 생성]":
                            _new_default = f"models/{st.session_state.get('gen_sc', 'schema')}/schema.yml"
                            _new_path = st.text_input(
                                "신규 경로 입력",
                                value=_new_default,
                                key=f"new_{itm['name']}"
                            )
                            # .yml 확장자 강제
                            if _new_path and not _new_path.endswith(('.yml', '.yaml')):
                                st.warning("⚠️ 경로는 .yml 또는 .yaml 확장자로 끝나야 합니다.")
                            target_path = _new_path
                    if do_apply:
                        ap_tasks.append({
                            "index": idx,
                            "name": itm['name'],
                            "path": target_path,
                            "model_def": itm['model_def']
                        })

        # YAML 미리보기
        if st.button("📄 YAML 내용 미리보기", use_container_width=True):
            if ap_tasks:
                st.code(
                    yaml.dump(
                        {"version": 2, "models": [t['model_def'] for t in ap_tasks]},
                        sort_keys=False, allow_unicode=True
                    ),
                    language="yaml"
                )
            else:
                st.warning("선택된 모델이 없습니다.")

        # 일괄 적용
        if st.button(
            "🚀 모든 변경사항 일괄 적용", type="primary", use_container_width=True,
            disabled=not ap_tasks or st.session_state.gen_is_applied
        ):
            pb = st.progress(0)
            for i, task in enumerate(ap_tasks):
                # 다른 파일에서 중복 제거
                remove_model_from_others(project_dir, task['name'], task['path'])

                # 대상 파일 로드 및 백업
                f_path = os.path.join(project_dir, task['path'])
                create_backup(project_dir, task['path'])

                data = {"version": 2, "models": []}
                if os.path.exists(f_path):
                    with open(f_path, 'r', encoding='utf-8') as f:
                        data = yaml.safe_load(f) or {"version": 2, "models": []}

                # 기존 모델 교체 or 신규 추가
                models = data.get('models', [])
                ex_idx = next((mi for mi, m in enumerate(models)
                               if m.get('name') == task['name']), -1)
                if ex_idx != -1:
                    models[ex_idx] = task['model_def']
                else:
                    models.append(task['model_def'])

                data['models'] = models
                _dir = os.path.dirname(f_path)
                if _dir:
                    os.makedirs(_dir, exist_ok=True)
                with open(f_path, 'w', encoding='utf-8') as f:
                    yaml.dump(data, f, sort_keys=False, allow_unicode=True, default_flow_style=False)

                st.session_state.gen_analysis_data[task['index']]['applied'] = True
                pb.progress((i + 1) / len(ap_tasks))

            # bak 파일 30일 이상 된 것 정리
            _bak_dir = os.path.join(project_dir, 'models', 'bak')
            if os.path.exists(_bak_dir):
                _cutoff = datetime.now().timestamp() - 30 * 86400
                for _bf in os.listdir(_bak_dir):
                    _bp = os.path.join(_bak_dir, _bf)
                    try:
                        if os.path.getmtime(_bp) < _cutoff:
                            os.remove(_bp)
                    except Exception:
                        pass

            st.session_state.gen_is_applied = True
            st.rerun()

        # 적용 완료 후 재분석 버튼
        if st.session_state.gen_is_applied:
            st.success("✅ 모든 변경사항이 적용되었습니다.")
            if st.button("🔄 다시 분석하기", use_container_width=True):
                st.session_state.gen_analysis_data = None
                st.session_state.gen_is_applied    = False
                st.rerun()


# ============================================================
# 다이얼로그 래퍼 — render_validation_ui 정의 이후에 위치해야 함
# ============================================================
@st.dialog("🔍 데이터 검증", width="large")
def open_validation_dialog():
    """검증 다이얼로그 래퍼 — render_validation_ui를 팝업으로 호출"""
    # 탭과 위젯 키 충돌 방지: 다이얼로그는 _vv + 1000 사용
    _orig_vv = st.session_state['_val_key_ver']
    st.session_state['_val_key_ver'] = _orig_vv + 1000
    try:
        render_validation_ui(project_dir, profile_dir, target_val)
    finally:
        st.session_state['_val_key_ver'] = _orig_vv

def _apply_runner_to_val():
    """runner_to_val 이력을 위젯 key에 반영 — 위젯 생성 전에 호출해야 함.
    runner_to_val 자체는 삭제하지 않음 (버튼 가시성 유지 목적).
    _r2v_applied 플래그로 중복 적용 방지.
    """
    _r2v = st.session_state.get('runner_to_val')
    if _r2v and not st.session_state.get('_r2v_applied'):
        _vv  = st.session_state['_val_key_ver']
        _dvv = _vv + 1000  # 다이얼로그 키 버전
        for _v in [_vv, _dvv]:
            st.session_state[f'val_sb_group_{_v}'] = _r2v.get('group', '')
            st.session_state[f'val_sb_model_{_v}'] = _r2v['model']
            st.session_state[f'val_sdt_{_v}']      = _r2v['sdt']
            st.session_state[f'val_edt_{_v}']      = _r2v['edt']
        st.session_state['val_date_filter']    = None  # 이전 탭의 where 조건 초기화
        st.session_state['val_results']        = None  # 이전 검증 결과 초기화
        st.session_state['val_compiled_sql']   = None
        st.session_state['val_compiled_model'] = None
        st.session_state['val_before_sql']     = None
        st.session_state['_r2v_applied'] = True  # 재적용 방지 (runner_to_val은 유지)

_show_val_dialog = st.session_state.pop('_open_val_dialog', False)

# 팝업 닫힘 감지: 열려 있던 팝업이 닫힌 직후 탭 초기화
if not _show_val_dialog and st.session_state.pop('_val_dialog_was_open', False):
    st.session_state['val_results']        = None
    st.session_state['val_compiled_sql']   = None
    st.session_state['val_compiled_model'] = None

if _show_val_dialog:
    st.session_state['_val_dialog_was_open'] = True   # 팝업 열림 기록
    # pop 전에 먼저 위젯 세팅 — st.rerun()으로 인해 위젯 state가 초기화된 경우 복원
    st.session_state.pop('_r2v_applied', None)   # 재적용 허용을 위해 플래그만 먼저 제거
    _apply_runner_to_val()                        # runner_to_val이 살아있는 상태에서 적용
    st.session_state.pop('runner_to_val', None)  # 적용 후 정리
    open_validation_dialog()

# ============================================================
# TAB 3 : 데이터 검증
# ============================================================
with tab_validator:
    # 다이얼로그가 열려있어도 항상 렌더링 (키 충돌 없음 — 다이얼로그는 _vv+1000 사용)
    _apply_runner_to_val()
    render_validation_ui(project_dir, profile_dir, target_val)

# ============================================================
# TAB 4 : 이력
# ============================================================
with tab_history:
    render_history_ui(_hist_db_config, _veri_exists, _dbtlog_exists)