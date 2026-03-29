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
import networkx as nx


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

def get_lineage_graph(project_dir, model_name, up_depth, down_depth):
    """NetworkX를 사용하여 리니지 그래프와 모델 메타데이터를 추출하여 반환."""
    manifest = _get_manifest(project_dir)
    if not manifest:
        return None, None, "manifest.json 없음"

    nodes_info = manifest.get('nodes', {})
    sources_info = manifest.get('sources', {})

    # 1. 전체 그래프 생성
    G = nx.DiGraph()
    for uid, node in nodes_info.items():
        if uid.startswith('model.'):
            G.add_node(uid, name=node['name'], type='model')
            for p_uid in node.get('depends_on', {}).get('nodes', []):
                if p_uid.startswith('model.') or p_uid.startswith('source.'):
                    G.add_edge(p_uid, uid)
        elif uid.startswith('source.'):
            # source는 depends_on이 없으므로 간선 추가만 위해 노드 선행 등록 (필요시)
            pass

    for uid, src in sources_info.items():
        G.add_node(uid, name=f"{src['source_name']}.{src['name']}", type='source')

    # 2. 타겟 노드 찾기
    target_uid = next((uid for uid, n in nodes_info.items() if n.get('name') == model_name), None)
    if not target_uid:
        return None, None, "모델 탐색 실패"

    # 3. Upstream/Downstream Subgraph 추출
    up_nodes = nx.single_source_shortest_path_length(G.reverse(), target_uid, cutoff=up_depth).keys()
    down_nodes = nx.single_source_shortest_path_length(G, target_uid, cutoff=down_depth).keys()
    subgraph_nodes = set(up_nodes) | set(down_nodes)
    subgraph = G.subgraph(subgraph_nodes)

    # 4. 실행 상태 매핑 (최신 run_results 로드)
    run_results = get_latest_run_results(project_dir)
    status_map = {r['Model Name']: r for r in run_results}

    # 5. 결과 가공 (UI 렌더링용)
    nodes_data = {}
    for uid in subgraph.nodes():
        node_type = G.nodes[uid]['type']
        name = G.nodes[uid]['name']
        
        # 메타데이터 추출
        mdata = nodes_info.get(uid) if node_type == 'model' else sources_info.get(uid)
        desc = mdata.get('description', '') if mdata else ''
        cols = []
        if mdata and 'columns' in mdata:
            for cname, cinfo in mdata['columns'].items():
                cols.append({
                    "name": cname,
                    "type": cinfo.get('data_type', 'unknown'),
                    "description": cinfo.get('description', '')
                })

        # 직계 관계 추출 (Subgraph 내 기준)
        parents = [G.nodes[p]['name'] for p in G.predecessors(uid)]
        children = [G.nodes[c]['name'] for c in G.successors(uid)]

        nodes_data[name] = {
            "uid": uid,
            "name": name,
            "type": node_type,
            "status": status_map.get(name, {}).get('Status', 'not run'),
            "rows": status_map.get(name, {}).get('Rows', '-'),
            "description": desc,
            "columns": cols,
            "parents": parents,
            "children": children,
            "materialized": mdata.get('config', {}).get('materialized', '') if node_type == 'model' else ''
        }

    # Depth별 그룹화 (기존 UI 호환용)
    up_by_depth = {}
    for node_uid, depth in nx.single_source_shortest_path_length(G.reverse(), target_uid, cutoff=up_depth).items():
        if depth == 0: continue
        name = G.nodes[node_uid]['name']
        display_name = name + " (Source)" if node_uid.startswith('source.') else name
        up_by_depth.setdefault(depth, set()).add(display_name)

    down_by_depth = {}
    for node_uid, depth in nx.single_source_shortest_path_length(G, target_uid, cutoff=down_depth).items():
        if depth == 0: continue
        name = G.nodes[node_uid]['name']
        down_by_depth.setdefault(depth, set()).add(name)

    up_by_depth = {d: sorted(list(s)) for d, s in sorted(up_by_depth.items())}
    down_by_depth = {d: sorted(list(s)) for d, s in sorted(down_by_depth.items())}

    return up_by_depth, down_by_depth, nodes_data, None

def get_lineage_from_manifest(project_dir, model_name, up_depth, down_depth):
    """하위 호환성을 위한 래퍼 함수."""
    up, down, _, err = get_lineage_graph(project_dir, model_name, up_depth, down_depth)
    return up, down, err

def calculate_meta_diff(db_cols, db_pk, db_comment, yaml_model_def):
    """DB 정보와 YAML 정의를 비교하여 상세 차이점 반환."""
    diff = {
        "columns": {},
        "pk_changed": False,
        "comment_changed": False,
        "has_critical": False,
        "summary": []
    }
    
    # 1. 컬럼 비교
    yaml_cols = {c['name'].lower(): c for c in yaml_model_def.get('columns', [])}
    db_cols_map = {c['name'].lower(): c for c in db_cols}
    
    all_col_names = sorted(set(yaml_cols.keys()) | set(db_cols_map.keys()))
    
    for cname in all_col_names:
        y_c = yaml_cols.get(cname)
        d_c = db_cols_map.get(cname)
        
        if not y_c:
            diff["columns"][cname] = {"status": "added", "db_type": d_c['data_type']}
            diff["has_critical"] = True
        elif not d_c:
            diff["columns"][cname] = {"status": "removed"}
            diff["has_critical"] = True
        else:
            c_diff = {}
            # 타입 비교 (contract 준수용: YAML에 아예 없거나 값이 다를 때 감지)
            y_type = (y_c.get('data_type') or '').lower().strip()
            d_type = d_c['data_type'].lower().strip()
            
            if not y_type:
                c_diff["type_mismatch"] = ("(누락됨)", d_type)
                diff["has_critical"] = True
            elif y_type != d_type:
                c_diff["type_mismatch"] = (y_type, d_type)
                diff["has_critical"] = True
            
            # 설명 비교
            y_desc = (y_c.get('description') or '').strip()
            d_desc = (d_c.get('description') or '').strip()
            if y_desc != d_desc:
                c_diff["desc_diff"] = (y_desc, d_desc)
            
            if c_diff:
                diff["columns"][cname] = {"status": "changed", "details": c_diff}

    # 2. PK 비교
    y_pk = yaml_model_def.get('config', {}).get('unique_key', [])
    if isinstance(y_pk, str): y_pk = [y_pk]
    y_pk = [k.lower() for k in y_pk]
    d_pk = [k.lower() for k in db_pk]
    
    if sorted(y_pk) != sorted(d_pk):
        diff["pk_changed"] = (y_pk, d_pk)
        diff["has_critical"] = True

    # 3. 테이블 코멘트 비교
    y_comm = (yaml_model_def.get('description') or '').strip()
    d_comm = (db_comment or '').strip()
    if y_comm != d_comm:
        diff["comment_changed"] = (y_comm, d_comm)

    # 요약 정보 생성
    added = len([c for c in diff["columns"].values() if c['status'] == "added"])
    removed = len([c for c in diff["columns"].values() if c['status'] == "removed"])
    type_m = len([c for c in diff["columns"].values() if c.get('details', {}).get('type_mismatch')])
    desc_m = len([c for c in diff["columns"].values() if c.get('details', {}).get('desc_diff')])
    
    if added: diff["summary"].append(f"🆕 컬럼 추가 {added}")
    if removed: diff["summary"].append(f"🗑️ 컬럼 삭제 {removed}")
    if type_m: diff["summary"].append(f"📏 타입 불일치 {type_m}")
    if desc_m: diff["summary"].append(f"📝 Column Comment 다름 {desc_m}")
    if diff["pk_changed"]: diff["summary"].append("🔑 PK 변경")
    if diff["comment_changed"]: diff["summary"].append("📁 Table Comment 다름")
    
    return diff

def apply_smart_sync(yaml_model_def, db_cols, db_pk, db_comment, sync_desc=True):
    """지능형 병합 로직 적용하여 새로운 YAML 모델 정의 생성."""
    new_def = yaml_model_def.copy()
    
    # 1. Config (PK) 업데이트
    if 'config' not in new_def: new_def['config'] = {}
    if len(db_pk) == 1:
        new_def['config']['unique_key'] = db_pk[0]
    elif len(db_pk) > 1:
        new_def['config']['unique_key'] = sorted(db_pk)
    else:
        new_def['config'].pop('unique_key', None)

    # 2. 테이블 설명
    y_desc = (new_def.get('description') or '').strip()
    if not y_desc or sync_desc:
        new_def['description'] = db_comment

    # 3. 컬럼 병합
    yaml_cols_map = {c['name'].lower(): c for c in new_def.get('columns', [])}
    new_cols = []
    
    for d_c in db_cols:
        cname = d_c['name']
        cname_l = cname.lower()
        y_c = yaml_cols_map.get(cname_l, {})
        
        # 기본 구조 (필수 반영)
        updated_c = y_c.copy()
        updated_c['name'] = cname
        updated_c['data_type'] = d_c['data_type']
        
        # 설명 병합 (Smart Merge)
        y_c_desc = (y_c.get('description') or '').strip()
        if not y_c_desc or sync_desc:
            updated_c['description'] = d_c['description']
        
        # Not Null 제약 조건 (Contract 준수)
        if not d_c['is_nullable']:
            if 'constraints' not in updated_c: updated_c['constraints'] = []
            # 기존에 not_null이 있는지 확인
            has_nn = any(cons.get('type') == 'not_null' for cons in updated_c['constraints'])
            if not has_nn:
                updated_c['constraints'].append({'type': 'not_null'})
        
        new_cols.append(updated_c)
        
    new_def['columns'] = new_cols
    return new_def
