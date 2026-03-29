# Design Ref: §3.2 — manifest.json 캐시 로드, SQL 치환, dbt 모델 계층/lineage
# 의존성 없음 (최하위 모듈)
import os
import re
import json
import glob
import shutil
import yaml
import streamlit as st
from datetime import datetime


def convert_to_dbt_ts(date_obj, is_end=False):
    """date 객체를 dbt vars용 타임스탬프 문자열로 변환. is_end=True이면 23:59:59, 아니면 00:00:00."""
    if is_end:
        return date_obj.strftime('%Y-%m-%d 23:59:59')
    return date_obj.strftime('%Y-%m-%d 00:00:00')


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

# Design Ref: §3.2 FR-03 — manifest.json 1회 로드 (mtime 기반 캐시)
@st.cache_data
def _load_manifest_cached(manifest_path: str, mtime: float) -> dict:
    """manifest.json을 캐시하여 1회만 파싱. mtime 변경 시 자동 재로드."""
    try:
        with open(manifest_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def _get_manifest(project_dir: str) -> dict:
    """manifest.json 경로 확인 후 캐시 로드 반환."""
    # Plan SC: SC-02 — manifest.json 로드 호출 1회
    path = os.path.join(project_dir, 'target', 'manifest.json')
    if not os.path.exists(path):
        return {}
    return _load_manifest_cached(path, os.path.getmtime(path))

def _get_this_from_manifest(project_dir, model_name):
    """manifest.json에서 모델의 relation_name 추출 ({{ this }} 치환용)"""
    manifest = _get_manifest(project_dir)
    for key, node in manifest.get('nodes', {}).items():
        if key.startswith('model.') and node.get('name') == model_name:
            return node.get('relation_name')
    return None

def _get_ref_from_manifest(project_dir, ref_model_name):
    """manifest.json에서 ref('model') → relation_name 추출"""
    manifest = _get_manifest(project_dir)
    for key, node in manifest.get('nodes', {}).items():
        if key.startswith('model.') and node.get('name') == ref_model_name:
            return node.get('relation_name')
    return None

def _get_source_from_manifest(project_dir, source_name, table_name):
    """manifest.json에서 source('source_name', 'table_name') → relation_name 추출"""
    manifest = _get_manifest(project_dir)
    for key, node in manifest.get('sources', {}).items():
        if node.get('source_name') == source_name and node.get('name') == table_name:
            return node.get('relation_name')
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
    def _replace_source(m):
        sn, tn = m.group(1), m.group(2)
        return _get_source_from_manifest(project_dir, sn, tn) or f'"{sn}"."{tn}"'
    before_sql = re.sub(
        r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}",
        _replace_source, before_sql
    )
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
    """before_sql 블록 내 DELETE 문에서 {{start}}/{{end}} 비교에 사용된 컬럼명 추출."""
    _m = re.search(r'\{%-?\s*set before_sql\s*-?%\}(.*?)\{%-?\s*endset\s*-?%\}',
                   raw_sql, re.DOTALL | re.IGNORECASE)
    if not _m:
        return None
    _block = _m.group(1)
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

def _has_date_vars(raw_sql):
    """SQL 문자열에 {{start}} 또는 {{end}} 존재 여부"""
    return bool(re.search(r'\{\{\s*(?:start|end)\s*\}\}', raw_sql))

def cleanup_old_runs_by_date(project_dir):
    """target/run_YYYYMMDD_* 디렉토리 중 오늘 날짜가 아닌 것을 삭제."""
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
    """반환: up_by_depth, down_by_depth, error"""
    manifest = _get_manifest(project_dir)
    if not manifest:
        return None, None, "manifest.json 없음"
    try:
        nodes   = manifest.get('nodes', {})
        sources = manifest.get('sources', {})
        target_uid = next((uid for uid, n in nodes.items() if n.get('name') == model_name), None)
        if not target_uid:
            return None, None, "모델 탐색 실패"
        up_d, down_d = {}, {}

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
        up_by_depth   = {d: sorted(list(s)) for d, s in sorted(up_d.items())}
        down_by_depth = {d: sorted(list(s)) for d, s in sorted(down_d.items())}
        return up_by_depth, down_by_depth, None
    except Exception as e:
        return None, None, str(e)
