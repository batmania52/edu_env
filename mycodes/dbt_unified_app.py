import streamlit as st
import os
import yaml
import json
import subprocess
import pandas as pd
import glob
import shutil
import psycopg2
import psycopg2.pool
import re
import uuid
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta

# ============================================================
# 0. 설정 및 캐시
# ============================================================
CACHE_FILE = ".dbt_unified_cache.json"

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

def convert_to_dbt_ts(date_obj, is_end=False):
    """date 객체를 dbt vars용 타임스탬프 문자열로 변환. is_end=True이면 23:59:59, 아니면 00:00:00."""
    if is_end:
        return date_obj.strftime('%Y-%m-%d 23:59:59')
    return date_obj.strftime('%Y-%m-%d 00:00:00')

# ============================================================
# 2. DB 연결 및 조회 (공통)
# ============================================================
@st.cache_resource
def _make_pool(host, port, dbname, user, password):
    """앱 전체에서 공유하는 DB 커넥션 풀 (최소 1, 최대 5)"""
    return psycopg2.pool.SimpleConnectionPool(
        1, 5,
        host=host, port=port, dbname=dbname, user=user, password=password
    )

@contextmanager
def get_conn(db_config):
    """풀에서 커넥션을 대여 후 자동 반납하는 컨텍스트 매니저"""
    p = _make_pool(
        db_config['host'], db_config['port'],
        db_config['dbname'], db_config['user'], db_config['password']
    )
    conn = p.getconn()
    try:
        yield conn
    finally:
        p.putconn(conn)

def get_db_config(profile_dir, target_name):
    """profiles.yml에서 DB 접속 정보 추출"""
    profile_path = os.path.join(profile_dir, 'profiles.yml')
    if not os.path.exists(profile_path):
        return None
    try:
        with open(profile_path, 'r', encoding='utf-8') as f:
            profiles = yaml.safe_load(f)
            profile_key = list(profiles.keys())[0]
            config = profiles[profile_key]['outputs'][target_name]
            return {
                "host": config.get('host'),
                "user": config.get('user'),
                "password": config.get('password'),
                "dbname": config.get('dbname', config.get('database')),
                "port": config.get('port', 5432)
            }
    except Exception:
        return None

def check_history_tables_exist(db_config):
    """admin.verification_summary, admin.dbt_log 두 테이블 존재 여부 반환 (bool, bool)"""
    try:
        with get_conn(db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'admin'
                      AND table_name IN ('verification_summary', 'dbt_log')
                """)
                found = {row[0] for row in cur.fetchall()}
        return 'verification_summary' in found, 'dbt_log' in found
    except Exception:
        return False, False


def get_schemas(db_config):
    """시스템 스키마를 제외한 사용자 스키마 목록"""
    with get_conn(db_config) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
              AND schema_name NOT LIKE 'pg_temp_%'
              AND schema_name NOT LIKE 'pg_toast_temp_%'
            ORDER BY schema_name
        """)
        return [row[0] for row in cur.fetchall()]

def get_db_tables(db_config, schema_name):
    """특정 스키마의 BASE TABLE 목록"""
    with get_conn(db_config) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY table_name",
            (schema_name,)
        )
        return [row[0] for row in cur.fetchall()]

def get_table_detail(db_config, schema_name, table_name):
    """컬럼 정보 및 PK 조회"""
    # ::regclass 사용 전 식별자를 검증 (영숫자, _, . 만 허용)
    if not re.match(r'^[\w.]+$', f"{schema_name}.{table_name}"):
        raise ValueError(f"유효하지 않은 스키마/테이블명: {schema_name}.{table_name}")
    with get_conn(db_config) as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT a.attname,
                   format_type(a.atttypid, a.atttypmod),
                   pg_catalog.col_description(a.attrelid, a.attnum)
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = '{schema_name}.{table_name}'::regclass
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
        """)
        columns = [{"name": r[0], "data_type": r[1], "description": r[2] or ""} for r in cur.fetchall()]
        cur.execute(f"SELECT obj_description('{schema_name}.{table_name}'::regclass, 'pg_class')")
        row = cur.fetchone()
        table_comment = row[0] if row and row[0] else f"Model for {table_name}"
        cur.execute(
            "SELECT kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "WHERE tc.constraint_type = 'PRIMARY KEY' "
            "  AND tc.table_name = %s AND tc.table_schema = %s",
            (table_name, schema_name)
        )
        pk = [row[0] for row in cur.fetchall()]
        return columns, pk, table_comment

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
# 4. Runner 전용 유틸리티
# ============================================================
def check_model_schema_exists(project_dir, model_name):
    """모델명이 models 하위 YAML 파일에 정의되어 있는지 확인. (bool, 경로 또는 오류 메시지) 반환."""
    if not model_name:
        return True, None
    m_path = os.path.join(project_dir, 'models')
    if not os.path.exists(m_path):
        return False, "⚠️ models 디렉토리 없음"
    yml_files = (
        glob.glob(os.path.join(m_path, "**", "*.yml"), recursive=True) +
        glob.glob(os.path.join(m_path, "**", "*.yaml"), recursive=True)
    )
    for p in yml_files:
        try:
            with open(p, 'r', encoding='utf-8') as f:
                if re.search(rf"-?\s*name:\s*['\"]?{model_name}['\"]?", f.read(), re.IGNORECASE):
                    return True, p
        except Exception:
            continue
    return False, f"⚠️ 모델 '{model_name}'의 정의가 YAML 파일에 없습니다."

def get_dbt_model_hierarchy(project_dir):
    """models 디렉토리를 탐색해 {그룹명: [모델명, ...]} 계층과 {모델명: 그룹명} 역매핑을 반환."""
    h, m2g = {}, {}
    m_path = os.path.join(project_dir, 'models')
    if not os.path.exists(m_path):
        return h, m2g
    for root, _, files in os.walk(m_path):
        sqls = [f[:-4] for f in files if f.endswith('.sql')]
        if sqls:
            rel = os.path.relpath(root, m_path)
            g = "root" if rel == "." else rel
            h[g] = sorted(sqls)
            for m in sqls:
                m2g[m] = g
    return h, m2g

def get_compiled_sql(project_dir, model_name):
    """target/compiled 디렉토리에서 모델의 컴파일된 SQL 파일을 찾아 내용을 반환. 없으면 None."""
    t_base = os.path.join(project_dir, 'target', 'compiled')
    for r, _, fs in os.walk(t_base):
        if f"{model_name}.sql" in fs:
            with open(os.path.join(r, f"{model_name}.sql"), 'r', encoding='utf-8') as f:
                return f.read().strip()
    return None

def _get_this_from_manifest(project_dir, model_name):
    """manifest.json에서 모델의 relation_name 추출 ({{ this }} 치환용)"""
    manifest_path = os.path.join(project_dir, 'target', 'manifest.json')
    if not os.path.exists(manifest_path):
        return None
    try:
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        for key, node in manifest.get('nodes', {}).items():
            if key.startswith('model.') and node.get('name') == model_name:
                return node.get('relation_name')
    except Exception:
        pass
    return None

def _get_ref_from_manifest(project_dir, ref_model_name):
    """manifest.json에서 ref('model') → relation_name 추출"""
    manifest_path = os.path.join(project_dir, 'target', 'manifest.json')
    if not os.path.exists(manifest_path):
        return None
    try:
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        for key, node in manifest.get('nodes', {}).items():
            if key.startswith('model.') and node.get('name') == ref_model_name:
                return node.get('relation_name')
    except Exception:
        pass
    return None

def _get_source_from_manifest(project_dir, source_name, table_name):
    """manifest.json에서 source('source_name', 'table_name') → relation_name 추출"""
    manifest_path = os.path.join(project_dir, 'target', 'manifest.json')
    if not os.path.exists(manifest_path):
        return None
    try:
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        for key, node in manifest.get('sources', {}).items():
            if node.get('source_name') == source_name and node.get('name') == table_name:
                return node.get('relation_name')
    except Exception:
        pass
    return None


def get_before_sql_from_model(project_dir, model_name, start_ts, end_ts):
    """모델 소스 파일에서 before_sql 블록을 추출하고 변수를 치환하여 반환"""
    models_dir = os.path.join(project_dir, 'models')
    src_file = None
    for root, _, files in os.walk(models_dir):
        if f"{model_name}.sql" in files:
            src_file = os.path.join(root, f"{model_name}.sql")
            break
    if not src_file:
        return None
    with open(src_file, 'r', encoding='utf-8') as f:
        content = f.read()
    pattern = r'\{%-?\s*set before_sql\s*-?%\}(.*?)\{%-?\s*endset\s*-?%\}'
    match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
    if not match:
        return None
    before_sql = match.group(1).strip()
    this_ref = _get_this_from_manifest(project_dir, model_name)
    if this_ref:
        before_sql = re.sub(r'\{\{\s*this\s*\}\}', this_ref, before_sql)
    before_sql = re.sub(r"'\{\{\s*start\s*\}\}'", f"'{start_ts}'", before_sql)
    before_sql = re.sub(r"'\{\{\s*end\s*\}\}'", f"'{end_ts}'", before_sql)
    before_sql = re.sub(r"\{\{\s*start\s*\}\}", start_ts, before_sql)
    before_sql = re.sub(r"\{\{\s*end\s*\}\}", end_ts, before_sql)
    # {{ source('source_name', 'table_name') }} → manifest relation_name 또는 "source_name"."table_name" 폴백
    def _replace_source(m):
        sn, tn = m.group(1), m.group(2)
        return _get_source_from_manifest(project_dir, sn, tn) or f'"{sn}"."{tn}"'
    before_sql = re.sub(
        r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}",
        _replace_source, before_sql
    )
    # {{ ref('model_name') }} → manifest relation_name 또는 "model_name" 폴백
    def _replace_ref(m):
        mn = m.group(1)
        return _get_ref_from_manifest(project_dir, mn) or f'"{mn}"'
    before_sql = re.sub(
        r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}",
        _replace_ref, before_sql
    )
    return before_sql


def _get_model_raw_sql(project_dir, model_name):
    """모델 SQL 파일 raw 내용 반환 (스키마.모델명 또는 모델명 형태 지원)"""
    _mn = model_name.split('.')[-1]
    _models_dir = os.path.join(project_dir, 'models')
    for _root, _, _files in os.walk(_models_dir):
        if f"{_mn}.sql" in _files:
            with open(os.path.join(_root, f"{_mn}.sql"), 'r', encoding='utf-8') as _f:
                return _f.read()
    return ''

def _detect_before_sql_date_col(raw_sql):
    """before_sql 블록 내 DELETE 문에서 {{start}}/{{end}} 비교에 사용된 컬럼명 추출.
    DELETE 문이 없으면 None 반환.
    BETWEEN '{{ start }}' 및 col >= '{{ start }}' 패턴 모두 지원.
    """
    _m = re.search(r'\{%-?\s*set before_sql\s*-?%\}(.*?)\{%-?\s*endset\s*-?%\}',
                   raw_sql, re.DOTALL | re.IGNORECASE)
    if not _m:
        return None
    _block = _m.group(1)
    # DELETE 문이 있는 구문에서만 감지
    _stmts = [s.strip() for s in _block.split(';') if s.strip()]
    _delete_stmts = [s for s in _stmts if re.match(r'\s*delete\b', s, re.IGNORECASE)]
    if not _delete_stmts:
        return None
    _delete_block = ' '.join(_delete_stmts)
    _cm = re.search(
        r'"?(\w+)"?\s+(?:between\s*[\'"]?\{\{\s*(?:start|end)|[<>=!]+\s*[\'"]?\{\{\s*(?:start|end))',
        _delete_block, re.IGNORECASE
    )
    return _cm.group(1) if _cm else None


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

def _has_date_vars(raw_sql):
    """SQL 문자열에 {{start}} 또는 {{end}} 존재 여부"""
    return bool(re.search(r'\{\{\s*(?:start|end)\s*\}\}', raw_sql))


def cleanup_old_runs_by_date(project_dir):
    """target/run_YYYYMMDD_* 디렉토리 중 오늘 날짜가 아닌 것을 삭제해 디스크를 정리."""
    today_str = datetime.now().strftime('%Y%m%d')
    for run_dir in glob.glob(os.path.join(project_dir, 'target', 'run_*')):
        try:
            if os.path.basename(run_dir).split('_')[1] != today_str:
                shutil.rmtree(run_dir)
        except Exception:
            continue

def get_latest_run_results(project_dir):
    """가장 최근 run 디렉토리의 run_results.json을 파싱해 모델별 실행 결과 목록을 반환."""
    run_dirs = glob.glob(os.path.join(project_dir, 'target', 'run_*'))
    if run_dirs:
        p = os.path.join(max(run_dirs, key=os.path.getmtime), 'run_results.json')
    else:
        p = os.path.join(project_dir, 'target', 'run_results.json')
    if not os.path.exists(p):
        return []
    res = []
    try:
        with open(p, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for r in data.get('results', []):
            uid = r.get('unique_id', '')
            if not uid.startswith('model.'):
                continue
            msg = r.get('adapter_response', {}).get('_message', '')
            rows = r.get('adapter_response', {}).get('rows_affected')
            if rows is None and msg:
                parts = str(msg).split()
                if parts and parts[-1].isdigit():
                    rows = int(parts[-1])
            res.append({
                "Model Name": uid.split('.')[-1],
                "Status": r.get('status'),
                "Rows": f"{rows:,}" if rows is not None else "-",
                "Time(s)": round(r.get('execution_time', 0), 2)
            })
    except Exception:
        pass
    return res

def get_lineage_from_manifest(project_dir, model_name, up_depth, down_depth):
    """
    반환: up_by_depth, down_by_depth, error
    up_by_depth  : {depth(1-based): [model_or_source_name, ...]}
    down_by_depth: {depth(1-based): [model_name, ...]}
    """
    manifest_path = os.path.join(project_dir, 'target', 'manifest.json')
    if not os.path.exists(manifest_path):
        return None, None, "manifest.json 없음"
    try:
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        nodes   = manifest.get('nodes', {})
        sources = manifest.get('sources', {})
        target_uid = next((uid for uid, n in nodes.items() if n.get('name') == model_name), None)
        if not target_uid:
            return None, None, "모델 탐색 실패"

        # depth별 중복 없이 수집: {depth: set()}
        up_d   = {}   # {depth(int): set of name}
        down_d = {}

        def find_up(uid, depth):
            if depth >= up_depth:
                return
            node = nodes.get(uid)
            if node:
                for p_uid in node.get('depends_on', {}).get('nodes', []):
                    if p_uid.startswith('model.'):
                        name = nodes[p_uid]['name']
                        up_d.setdefault(depth + 1, set()).add(name)
                        find_up(p_uid, depth + 1)
                    elif p_uid.startswith('source.'):
                        src = sources.get(p_uid)
                        if src:
                            name = f"{src['source_name']}.{src['name']} (Source)"
                            up_d.setdefault(depth + 1, set()).add(name)

        def find_down(uid, depth):
            if depth >= down_depth:
                return
            for c_uid, node in nodes.items():
                if uid in node.get('depends_on', {}).get('nodes', []) and c_uid.startswith('model.'):
                    name = node['name']
                    down_d.setdefault(depth + 1, set()).add(name)
                    find_down(c_uid, depth + 1)

        find_up(target_uid, 0)
        find_down(target_uid, 0)

        # set → sorted list
        up_by_depth   = {d: sorted(list(s)) for d, s in sorted(up_d.items())}
        down_by_depth = {d: sorted(list(s)) for d, s in sorted(down_d.items())}
        return up_by_depth, down_by_depth, None
    except Exception as e:
        return None, None, str(e)

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
        'up_depth': 0, 'down_depth': 0,
        'cmd_reviewed': False,
        'sb_group': "📂 모델 그룹을 선택하세요",
        'gen_analysis_data': None,
        'gen_is_applied': False,
        'start_dt_widget': datetime.now() - timedelta(days=4),
        'end_dt_widget': datetime.now() - timedelta(days=1),
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

def on_ui_change():
    """모델/날짜/모드 위젯 변경 시 호출되어 컴파일 결과와 실행 상태를 초기화."""
    st.session_state.compiled_sql = None
    st.session_state.before_sql = None
    st.session_state.cmd_reviewed = False
    st.session_state['last_run_df']    = None
    st.session_state['runner_to_val']  = None   # Runner → Validator 탭 간 이관 데이터 (실행 후 검증 연계)

def select_new_model(model_name):
    """Lineage 버튼 클릭 시 호출되어 해당 모델로 그룹/모델 selectbox를 전환하고 UI를 초기화."""
    g = st.session_state.model_to_group.get(model_name)
    if g:
        st.session_state.sb_group = g
        st.session_state.sb_model = model_name
        st.session_state.up_depth = 0
        st.session_state.down_depth = 0
        on_ui_change()

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
    project_dir = st.text_input("Project Directory", value=cache.get("project_dir", os.getcwd()))
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
        st.session_state['sb_group']     = "📂 모델 그룹을 선택하세요"
        st.session_state.pop('sb_model', None)
        st.session_state.pop('rb_run_mode', None)

    cr1, cr2, cr3 = st.columns([2, 1, 1])
    with cr1:
        gp_p = "📂 모델 그룹을 선택하세요"
        sel_g = st.selectbox("📁 그룹 선택", [gp_p] + list(hierarchy.keys()),
                             key="sb_group", on_change=on_ui_change)
        mp_p = "📂 모델을 선택하세요"
        sel_m = None
        if sel_g != gp_p:
            mv = st.selectbox("📂 모델 선택", [mp_p] + hierarchy[sel_g],
                              key="sb_model", on_change=on_ui_change)
            if mv != mp_p:
                sel_m = mv
    with cr2:
        run_m = st.radio("🏃 모드", ["manual", "schedule"],
                         key="rb_run_mode", horizontal=True, on_change=on_ui_change)
    with cr3:
        st.write("")
        if st.button("🔄 초기화", key="btn_reset_runner", use_container_width=True):
            # 위젯 key가 아닌 값들은 즉시 초기화
            st.session_state['up_list']       = {}
            st.session_state['down_list']     = {}
            st.session_state['up_depth']      = 0
            st.session_state['down_depth']    = 0
            st.session_state['compiled_sql']  = None
            st.session_state['before_sql']    = None
            st.session_state['cmd_reviewed']  = False
            st.session_state['runner_to_gen'] = None
            st.session_state['runner_to_val'] = None
            # 위젯 key는 다음 rerun에서 위젯 생성 전에 처리
            st.session_state['_runner_reset_pending'] = True
            st.rerun()

    schema_ok, sc_err = check_model_schema_exists(project_dir, sel_m) if sel_m else (True, None)

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

        # Lineage 컨트롤
        dc1, dc2, dc3, dc4 = st.columns([1, 1, 1, 1])
        with dc1:
            u1, u2 = st.columns(2)
            if u1.button("➖", key="um", disabled=not schema_ok):
                st.session_state.up_depth = max(0, st.session_state.up_depth - 1); on_ui_change()
            if u2.button("➕", key="up", disabled=not schema_ok):
                st.session_state.up_depth += 1; on_ui_change()
            st.write(f"**업스트림 Depth: {st.session_state.up_depth}**")
        with dc2:
            if st.button("🧬 Lineage 분석", use_container_width=True, disabled=not schema_ok):
                with st.spinner("분석 중..."):
                    up, dn, err = get_lineage_from_manifest(
                        project_dir, sel_m,
                        st.session_state.up_depth, st.session_state.down_depth
                    )
                    if err:
                        st.error(err)
                    else:
                        st.session_state.up_list = up
                        st.session_state.down_list = dn
        with dc3:
            d1, d2 = st.columns(2)
            if d1.button("➖", key="dm", disabled=not schema_ok):
                st.session_state.down_depth = max(0, st.session_state.down_depth - 1); on_ui_change()
            if d2.button("➕", key="dp", disabled=not schema_ok):
                st.session_state.down_depth += 1; on_ui_change()
            st.write(f"**다운스트림 Depth: {st.session_state.down_depth}**")
        with dc4:
            st.write("")
            if st.button("🔄 Lineage 초기화", use_container_width=True,
                         disabled=not (st.session_state.up_list or st.session_state.down_list)):
                st.session_state.up_list    = {}
                st.session_state.down_list  = {}
                st.session_state.up_depth   = 0
                st.session_state.down_depth = 0
                on_ui_change()
                st.rerun()

        # Lineage 결과 표시
        if schema_ok and (st.session_state.up_list or st.session_state.down_list):
            up_d   = st.session_state.up_list
            down_d = st.session_state.down_list
            max_up_d   = max(up_d.keys(),   default=0)
            max_down_d = max(down_d.keys(), default=0)

            total_cols = max_up_d + 1 + max_down_d
            all_cols   = st.columns(total_cols) if total_cols > 0 else []

            # 공통 카드 스타일 - 버튼과 동일한 높이/폰트 기준
            _card_common = (
                "display:block;width:100%;box-sizing:border-box;"
                "border:1px solid rgba(49,51,63,0.2);border-radius:6px;"
                "padding:6px 12px;margin-bottom:4px;"
                "font-size:14px;line-height:1.6;text-align:center;"
                "font-family:inherit;"
            )
            _card_model  = _card_common + "cursor:default;background:transparent;"
            _card_source = _card_common + "background:transparent;color:#888;"
            _card_center = _card_common + (
                "font-weight:bold;color:#ff4b4b;background:transparent;"
            )

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
                                      on_click=select_new_model, args=(m,),
                                      use_container_width=True)

            # 선택 모델 (중앙)
            with all_cols[max_up_d]:
                st.caption("▶ 현재 모델")
                st.button(
                    sel_m, key="lineage_center_model",
                    use_container_width=True, disabled=True
                )

            # 다운스트림
            for d in range(1, max_down_d + 1):
                col_idx = max_up_d + d
                with all_cols[col_idx]:
                    st.caption(f"⬇️ Depth {d}")
                    for m in down_d.get(d, []):
                        st.button(m, key=f"db_{d}_{m}",
                                  on_click=select_new_model, args=(m,),
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
        slc = f"{st.session_state.up_depth}+{sel_m}+{st.session_state.down_depth}"  # dbt --select 인자: upstream_depth+model+downstream_depth 형식
        v_j = json.dumps({
            "data_interval_start": convert_to_dbt_ts(sdt),
            "data_interval_end": convert_to_dbt_ts(edt, True),
            "run_mode": run_m
        })
        args = ["--select", slc, "--target", target_val, "--vars", v_j,
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
                display_vars = f"'{v_j}'"
                review_cmd = ["dbt", "run", "--select", slc, "--target", target_val,
                              "--vars", display_vars,
                              "--project-dir", project_dir, "--profiles-dir", profile_dir]
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
# TAB 3 : 데이터 검증 (COUNT / 숫자컬럼 SUM / 샘플 비교)
# ============================================================

# ============================================================
# 검증 탭 전용 함수
# ============================================================

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

# psycopg2 숫자 타입 OID (pg_type.oid)
_NUMERIC_OIDS = {20, 21, 23, 700, 701, 1700, 790}  # int8,int2,int4,float4,float8,numeric,money
# psycopg2 날짜/타임스탬프 타입 OID
_DATE_OIDS    = {1082, 1114, 1184, 1083, 1266}      # date, timestamp, timestamptz, time, timetz

DATE_FORMATS = ["yyyymmdd", "yyyy-mm-dd", "yyyymm", "yyyy-mm"]

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
    where = f'\nWHERE {date_filter["where_clause"]}' if date_filter else ""
    sql   = f'SELECT COUNT(*)\nFROM "{schema}"."{table}"{where}'
    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        cur = c.cursor()
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
    sum_expr = ",\n  ".join([f'SUM("{c}") AS "{c}"' for c in num_cols])
    where    = f'\nWHERE {date_filter["where_clause"]}' if date_filter else ""
    sql      = f'SELECT\n  {sum_expr}\nFROM "{schema}"."{table}"{where}'
    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        cur = c.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return {c: (float(row[i]) if row[i] is not None else None) for i, c in enumerate(num_cols)}, sql

def val_src_sample(db_config, compiled_sql, limit, conn=None):
    """소스: CTE 기반 샘플 → (df, executed_sql)"""
    sql = f"SELECT * FROM (\n{compiled_sql}\n) _src LIMIT {limit}"
    with (nullcontext(conn) if conn else get_conn(db_config)) as c:
        return pd.read_sql(sql, c), sql

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
            has_diff    = False
            for idx in range(min(len(src_df), len(tgt_df))):
                for col in common_cols:
                    sv, tv = src_df.iloc[idx][col], tgt_df.iloc[idx][col]
                    both_nan = pd.isna(sv) and pd.isna(tv)
                    try:
                        differ = float(sv) != float(tv)
                    except (TypeError, ValueError):
                        differ = str(sv) != str(tv)
                    if not both_nan and differ:
                        has_diff = True
                        break
                if has_diff:
                    break
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
            sv = df.iloc[idx][col]
            ov = other_df.iloc[idx][col]
            both_nan = pd.isna(sv) and pd.isna(ov)
            try:
                differ = float(sv) != float(ov)
            except (TypeError, ValueError):
                differ = str(sv) != str(ov)
            if not both_nan and differ:
                style_df.iloc[idx, df.columns.get_loc(col)] = f'background-color: {diff_color}'
    return df.style.apply(lambda _: style_df, axis=None)


def render_validation_ui():
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

        # 날짜 컬럼 선택(드롭다운)과 직접 입력(텍스트) 두 경로로 분기.
        # _is_direct_input=True이면 WHERE 절 전체를 사용자가 직접 입력하고,
        # False이면 컬럼명을 선택해 날짜 범위 조건을 자동 생성한다.
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
                placeholder='예) customer_id IN (SELECT customer_id FROM stg.stg_receipts WHERE order_date BETWEEN \'2026-01-01 00:00:00\'::timestamp AND \'2026-01-02 23:59:59\'::timestamp)',
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
            # 체크박스가 선택된 구문만 추려 단일 커넥션으로 순차 실행.
            # 미체크 구문(예: DELETE/TRUNCATE)은 이 목록에 포함되지 않아 실행 생략.
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

            # ── Step 3: 검증 수행 ────────────────────────────────
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
                # 각 before_sql 구문의 체크 여부(val_bsql_check_i)가
                # 검증 실행 시 단일 커넥션(conn) 공유 여부를 결정한다.
                # 체크된 구문이 하나라도 있으면 temp table이 살아있는 동일 커넥션을 재사용.
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
                    _hd2 = False
                    for _ri in range(min(len(_s2), len(_t2))):
                        for _rc in _cc2:
                            _sv2, _tv2 = _s2.iloc[_ri][_rc], _t2.iloc[_ri][_rc]
                            if not (pd.isna(_sv2) and pd.isna(_tv2)):
                                try:
                                    _d2 = float(_sv2) != float(_tv2)
                                except (TypeError, ValueError):
                                    _d2 = str(_sv2) != str(_tv2)
                                if _d2:
                                    _hd2 = True
                                    break
                        if _hd2:
                            break
                    _sample_ok = not _hd2
                elif _s2 is not None and _t2 is not None:
                    _sample_ok = True

            _all_chk   = [v for v in [_count_ok, _sum_ok, _sample_ok] if v is not None]
            _overall_ok = all(_all_chk) if _all_chk else True

            def _mk_badge(label, ok, fail_color):
                if ok is None:
                    return ''
                bg = '#27ae60' if ok else fail_color
                return (f'<span style="background:{bg};color:white;padding:3px 10px;'
                        f'border-radius:4px;font-size:13px;font-weight:500;">'
                        f'{label} {"PASS" if ok else "FAIL"}</span>')

            _badge_html = ' &nbsp; '.join(filter(None, [
                _mk_badge('전체',   _overall_ok, '#c0392b'),
                _mk_badge('COUNT',  _count_ok,   '#e67e22'),
                _mk_badge('SUM',    _sum_ok,     '#2980b9'),
                _mk_badge('SAMPLE', _sample_ok,  '#8e44ad'),
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
                            both_nan = (pd.isna(sv) and pd.isna(tv))
                            try:
                                differ = (float(sv) != float(tv))
                            except (TypeError, ValueError):
                                differ = (str(sv) != str(tv))
                            if not both_nan and differ:
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



# ============================================================
# 이력 탭 UI
# ============================================================
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

                    def _det_badge(label, ok, fail_color):
                        if ok is None: return ''
                        bg = '#27ae60' if ok else fail_color
                        return (f'<span style="background:{bg};color:white;padding:3px 10px;'
                                f'border-radius:4px;font-size:13px;font-weight:500;">'
                                f'{label} {"PASS" if ok else "FAIL"}</span>')

                    _d_badge_html = ' &nbsp; '.join(filter(None, [
                        _det_badge('전체',   _d_overall,   '#c0392b'),
                        _det_badge('COUNT',  _d_count_ok,  '#e67e22'),
                        _det_badge('SUM',    _d_sum_ok,    '#2980b9'),
                        _det_badge('SAMPLE', _d_sample_ok, '#8e44ad'),
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
        render_validation_ui()
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
    render_validation_ui()

# ============================================================
# TAB 4 : 이력
# ============================================================
with tab_history:
    render_history_ui(_hist_db_config, _veri_exists, _dbtlog_exists)