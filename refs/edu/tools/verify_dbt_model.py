import os
import yaml
import json
import subprocess
import sys
import shutil
import psycopg2
from datetime import datetime

# InquirerPy 및 프롬프트 툴킷 설정
from InquirerPy import inquirer
from InquirerPy.validator import PathValidator
from prompt_toolkit.keys import Keys

# Rich 라이브러리
from rich.console import Console
from rich.syntax import Syntax
from rich.panel import Panel
from rich.table import Table

console = Console()
CACHE_FILE = ".dbt_schema_cache.json"

# 공통 키바인딩
KB_EXIT = {"interrupt": [{"key": Keys.Escape}]}
KB_FUZZY_MULTI = {
    "interrupt": [{"key": Keys.Escape}],
    "toggle": [{"key": "right"}], 
}

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return {}
    return {}

def save_cache(data):
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def create_backup(project_dir, file_rel_path, backup_root):
    """수정 전 파일을 백업 폴더로 복사"""
    if not os.path.exists(os.path.join(project_dir, file_rel_path)):
        return
    
    os.makedirs(backup_root, exist_ok=True)
    dest_path = os.path.join(backup_root, file_rel_path.replace(os.sep, '_'))
    shutil.copy2(os.path.join(project_dir, file_rel_path), dest_path)
    console.print(f"[dim][Backup] {file_rel_path} -> {dest_path}[/dim]")

def check_exit(result):
    if result is None:
        console.print("\n[bold yellow]👋 사용자가 중단하였습니다.[/bold yellow]")
        sys.exit(0)
    return result

def get_db_config_from_profile(profile_dir, target_name):
    profile_path = os.path.join(profile_dir, 'profiles.yml')
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

def get_schemas(db_config):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute("""
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        AND schema_name NOT LIKE 'pg_temp_%' AND schema_name NOT LIKE 'pg_toast_temp_%'
        ORDER BY schema_name;
    """)
    schemas = [row[0] for row in cur.fetchall()]
    cur.close(); conn.close()
    return schemas

def get_db_tables(db_config, schema_name):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_type = 'BASE TABLE' ORDER BY table_name;")
    tables = [row[0] for row in cur.fetchall()]
    cur.close(); conn.close()
    return tables

def get_table_detail(db_config, schema_name, table_name):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod), pg_catalog.col_description(a.attrelid, a.attnum)
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = '{schema_name}.{table_name}'::regclass AND a.attnum > 0 AND NOT a.attisdropped
    """)
    columns = [{"name": r[0], "data_type": r[1], "description": r[2] or ""} for r in cur.fetchall()]
    cur.execute(f"SELECT kcu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = '{table_name}' AND tc.table_schema = '{schema_name}'")
    pk = [row[0] for row in cur.fetchall()]
    cur.close(); conn.close()
    return columns, pk

def find_model_location(project_dir, model_name):
    models_root = os.path.join(project_dir, 'models')
    for root, _, files in os.walk(models_root):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                rel_path = os.path.relpath(os.path.join(root, file), project_dir)
                with open(os.path.join(project_dir, rel_path), 'r', encoding='utf-8') as f:
                    try:
                        data = yaml.safe_load(f)
                        if data and 'models' in data:
                            for m in data['models']:
                                if m.get('name') == model_name:
                                    return rel_path
                    except: continue
    return None

def get_all_yml_files(project_dir):
    yml_files = []
    models_root = os.path.join(project_dir, 'models')
    for root, _, files in os.walk(models_root):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                yml_files.append(os.path.relpath(os.path.join(root, file), project_dir))
    return sorted(yml_files)

def remove_models_from_other_files(project_dir, model_names, target_yml, backup_root):
    """대상 파일을 제외한 다른 파일에서 중복 모델 삭제 전 백업 및 처리"""
    yml_files = get_all_yml_files(project_dir)
    for yml in yml_files:
        if yml == target_yml: continue
        full_path = os.path.join(project_dir, yml)
        with open(full_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        if data and 'models' in data:
            original_count = len(data['models'])
            new_models = [m for m in data['models'] if m['name'] not in model_names]
            if len(new_models) < original_count:
                # 변경 전 백업 실행
                create_backup(project_dir, yml, backup_root)
                data['models'] = new_models
                with open(full_path, 'w', encoding='utf-8') as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
                console.print(f"[dim][Cleanup] {yml}에서 중복 모델 제거 완료[/dim]")

def run_app():
    cache = load_cache()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_root = os.path.join(os.getcwd(), f"backup_schema_{timestamp}")
    
    # 1. 환경 설정
    project_dir = os.path.abspath(check_exit(inquirer.filepath(message="dbt 프로젝트 경로:", default=cache.get("project_dir", os.getcwd()), validate=PathValidator(is_dir=True), keybindings=KB_EXIT).execute()))
    profile_dir = os.path.abspath(check_exit(inquirer.filepath(message="profiles.yml 경로:", default=cache.get("profile_dir", os.path.join(project_dir, '.dbt')), keybindings=KB_EXIT).execute()))

    try:
        with open(os.path.join(profile_dir, 'profiles.yml'), 'r', encoding='utf-8') as f:
            p_data = yaml.safe_load(f)
            p_key = list(p_data.keys())[0]
            targets = list(p_data[p_key].get('outputs', {}).keys())
    except Exception as e:
        console.print(f"[bold red]❌ profiles.yml 로드 실패:[/bold red] {e}"); return

    target_name = check_exit(inquirer.select(message="Target 선택:", choices=targets, default=cache.get("target"), keybindings=KB_EXIT).execute())
    db_config = get_db_config_from_profile(profile_dir, target_name)
    schema_name = check_exit(inquirer.fuzzy(message="조회할 DB 스키마 선택:", choices=get_schemas(db_config), default=cache.get("schema", "public"), keybindings=KB_EXIT).execute())

    save_cache({"project_dir": project_dir, "profile_dir": profile_dir, "target": target_name, "schema": schema_name})

    # 2. 다중 모델 선택
    tables = get_db_tables(db_config, schema_name)
    selected_tables = check_exit(inquirer.fuzzy(message="대상 테이블 선택 (Right 화살표: 선택):", choices=tables, multiselect=True, match_exact=True, keybindings=KB_FUZZY_MULTI).execute())
    if not selected_tables: return

    # 3. 모델 현황 리스팅
    status_table = Table(title=f"\n[bold]선택된 모델 현황 (백업 저장소: {backup_root})[/bold]", show_header=True, header_style="bold magenta")
    status_table.add_column("모델명", style="dim")
    status_table.add_column("상태", justify="center")
    status_table.add_column("현재 위치 (YAML)", style="cyan")

    for m in selected_tables:
        loc = find_model_location(project_dir, m)
        status = "[green]신규[/green]" if not loc else "[yellow]존재함[/yellow]"
        status_table.add_row(m, status, loc if loc else "-")
    console.print(status_table)

    # 4. 작업 선택
    action = check_exit(inquirer.select(message="작업 선택:", choices=[{"name": "YAML 수정/추가 (통합)", "value": "edit"}, {"name": "YAML 출력만", "value": "print"}], keybindings=KB_EXIT).execute())
    if action == "print":
        for m in selected_tables:
            cols, pks = get_table_detail(db_config, schema_name, m)
            entry = {"name": m, "description": f"Model for {m}", "columns": cols}
            if pks: entry["config"] = {"materialized": "incremental", "unique_key": pks}
            console.print(Panel(Syntax(yaml.dump({"models": [entry]}, allow_unicode=True, sort_keys=False), "yaml", theme="monokai"), title=m))
        return

    # 5. 파일 통합 및 백업 실행
    target_yml = check_exit(inquirer.select(message="통합 저장할 YAML 파일 선택:", choices=get_all_yml_files(project_dir) + ["[신규 파일 생성]"], keybindings=KB_EXIT).execute())
    if target_yml == "[신규 파일 생성]":
        new_dir = check_exit(inquirer.filepath(message="생성 경로:", default=os.path.join(project_dir, 'models'), validate=PathValidator(is_dir=True), keybindings=KB_EXIT).execute())
        new_fn = check_exit(inquirer.text(message="파일명 (예: marts.yml):", keybindings=KB_EXIT).execute())
        target_yml = os.path.relpath(os.path.join(new_dir, new_fn), project_dir)

    if not check_exit(inquirer.confirm(message=f"선택한 {len(selected_tables)}개 모델을 '{target_yml}'로 통합하시겠습니까?", default=False).execute()): return

    # [핵심] 다른 파일에서 모델이 제거되기 전 백업 수행
    remove_models_from_other_files(project_dir, selected_tables, target_yml, backup_root)

    # [핵심] 대상 파일에 모델이 추가/수정되기 전 백업 수행
    create_backup(project_dir, target_yml, backup_root)

    full_target_path = os.path.join(project_dir, target_yml)
    os.makedirs(os.path.dirname(full_target_path), exist_ok=True)
    final_data = {"version": 2, "models": []}
    if os.path.exists(full_target_path):
        with open(full_target_path, 'r', encoding='utf-8') as f:
            final_data = yaml.safe_load(f) or {"version": 2, "models": []}

    for m in selected_tables:
        cols, pks = get_table_detail(db_config, schema_name, m)
        new_entry = {"name": m, "description": f"Model for {m}", "config": {"materialized": "incremental", "incremental_strategy": "append"}, "columns": cols}
        if pks: new_entry["config"]["unique_key"] = pks
        final_data['models'] = [existing for existing in final_data.get('models', []) if existing.get('name') != m]
        final_data['models'].append(new_entry)

    with open(full_target_path, 'w', encoding='utf-8') as f:
        yaml.dump(final_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    console.print(f"\n[bold green]✔ 백업 완료 및 {target_yml} 통합 완료![/bold green]")

if __name__ == "__main__":
    try: run_app()
    except (KeyboardInterrupt, EOFError): console.print("\n[yellow]👋 종료.[/yellow]"); sys.exit(0)