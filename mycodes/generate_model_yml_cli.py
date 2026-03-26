# mycodes/generate_model_yml_cli.py
import argparse
import os
import yaml
import json
import shutil
import sys
from datetime import datetime
import psycopg2 # psycopg2 임포트를 최상단으로 이동

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

def check_exit(result):
    if result is None:
        console.print("[bold yellow]👋 종료.[/bold yellow]"); sys.exit(0)
    return result

def is_model_yml(full_path):
    """내용을 분석하여 sources 정의 파일은 제외"""
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            if not data: return False
            if 'sources' in data: return False
            if 'models' in data or 'version' in data: return True
    except: return False
    return False

def create_backup(project_dir, file_rel_path, backup_root):
    source_path = os.path.join(project_dir, file_rel_path)
    if not os.path.exists(source_path): return
    os.makedirs(backup_root, exist_ok=True)
    timestamp = datetime.now().strftime('%H%M%S')
    backup_filename = f"{file_rel_path.replace(os.sep, '_')}_{timestamp}.bak"
    dest_path = os.path.join(backup_root, backup_filename)
    shutil.copy2(source_path, dest_path)
    console.print(f"[dim][Backup] {file_rel_path} 백업 완료[/dim]")

def get_db_config_from_profile(profile_dir, target_name):
    profile_path = os.path.join(profile_dir, 'profiles.yml')
    if not os.path.exists(profile_path):
        raise FileNotFoundError(f"profiles.yml 파일을 찾을 수 없습니다: {profile_path}")
    with open(profile_path, 'r', encoding='utf-8') as f:
        profiles = yaml.safe_load(f)
        if not profiles:
            raise ValueError(f"profiles.yml 파일이 비어있거나 형식이 잘못되었습니다: {profile_path}")
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
    try:
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
    except psycopg2.Error as e:
        raise ConnectionError(f"데이터베이스 스키마 목록 조회 중 오류 발생: {e}")

def get_db_tables(db_config, schema_name):
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_type = 'BASE TABLE' ORDER BY table_name;")
        tables = [row[0] for row in cur.fetchall()]
        cur.close(); conn.close()
        return tables
    except psycopg2.Error as e:
        raise ConnectionError(f"데이터베이스 테이블 목록 조회 중 오류 발생: {e}")

def get_table_detail(db_config, schema_name, table_name):
    """DB의 물리적 순서(attnum)에 맞춰 컬럼 정보를 가져옴"""
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        # attnum 순서로 정렬하여 DB 스키마 순서 보장
        cur.execute(f"""
            SELECT a.attname, format_type(a.atttypid, a.atttypmod), pg_catalog.col_description(a.attrelid, a.attnum)
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = '{schema_name}.{table_name}'::regclass 
              AND a.attnum > 0 
              AND NOT a.attisdropped
            ORDER BY a.attnum
        """)
        columns = [{"name": r[0], "data_type": r[1], "description": r[2] or ""} for r in cur.fetchall()]
        
        cur.execute(f"SELECT kcu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = '{table_name}' AND tc.table_schema = '{schema_name}'")
        pk = [row[0] for row in cur.fetchall()]
        cur.close(); conn.close()
        return columns, pk
    except psycopg2.Error as e:
        raise ConnectionError(f"데이터베이스 테이블 상세 정보 조회 중 오류 발생: {e}")

def find_model_location(project_dir, model_name):
    models_root = os.path.join(project_dir, 'models')
    for root, _, files in os.walk(models_root):
        if 'bak' in root.split(os.sep): continue
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                full_path = os.path.join(root, file)
                if not is_model_yml(full_path): continue
                rel_path = os.path.relpath(full_path, project_dir)
                with open(full_path, 'r', encoding='utf-8') as f:
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
    if not os.path.exists(models_root):
        console.print(f"[bold yellow]경고:[/bold yellow] 'models' 디렉토리가 없습니다: {models_root}")
        return []
    for root, _, files in os.walk(models_root):
        if 'bak' in root.split(os.sep): continue
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                full_path = os.path.join(root, file)
                if is_model_yml(full_path):
                    yml_files.append(os.path.relpath(full_path, project_dir))
    return sorted(yml_files)

def remove_models_from_other_files(project_dir, model_names, target_yml, backup_root, no_backup):
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
                if not no_backup:
                    create_backup(project_dir, yml, backup_root)
                data['models'] = new_models
                with open(full_path, 'w', encoding='utf-8') as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
                console.print(f"[dim][Cleanup] {yml} 정리 완료[/dim]")


def main():
    parser = argparse.ArgumentParser(description="DB 테이블 스키마를 기반으로 dbt schema.yml 파일을 생성 또는 업데이트합니다.")
    parser.add_argument("--project_dir", help="DBT 프로젝트의 루트 디렉토리 경로.")
    parser.add_argument("--profile_dir", help="profiles.yml 파일이 있는 경로.")
    parser.add_argument("--target", help="profiles.yml 내의 DBT 타겟 이름.")
    parser.add_argument("--db_schema", help="메타데이터를 조회할 DB 스키마 이름.")
    parser.add_argument("--tables", help="콤마로 구분된 대상 테이블 이름 목록 (예: stg_customers,stg_orders).")
    parser.add_argument("--output_yml_path", help="생성 또는 업데이트할 schema.yml 파일의 경로 (프로젝트 루트 기준 상대 경로).")
    parser.add_argument("--print_only", action="store_true", help="YAML을 파일에 저장하지 않고 콘솔에만 출력합니다.")
    parser.add_argument("--no_backup", action="store_true", help="파일 백업을 건너킵니다.")
    
    args = parser.parse_args()
    cache = load_cache()

    # CLI 모드 여부 판단
    # 모든 필수 인자가 제공되었는지 확인
    is_cli_mode = all([
        args.project_dir, args.profile_dir, args.target, 
        args.db_schema, args.tables, args.output_yml_path
    ])

    project_dir = os.path.abspath(args.project_dir) if args.project_dir else None
    profile_dir = os.path.abspath(args.profile_dir) if args.profile_dir else None
    target_name = args.target
    schema_name = args.db_schema
    selected_tables = [t.strip() for t in args.tables.split(',')] if args.tables else []
    output_yml_path = args.output_yml_path
    print_only = args.print_only
    no_backup = args.no_backup

    if not is_cli_mode:
        console.print("[bold cyan]--- 대화형 모드 시작 ---[/bold cyan]")
        project_dir = os.path.abspath(check_exit(inquirer.filepath(
            message="dbt 프로젝트 경로:", 
            default=project_dir or cache.get("project_dir", os.getcwd()), 
            validate=PathValidator(is_dir=True), 
            keybindings=KB_EXIT
        ).execute()))
        profile_dir = os.path.abspath(check_exit(inquirer.filepath(
            message="profiles.yml 경로:", 
            default=profile_dir or cache.get("profile_dir", os.path.join(project_dir, '.dbt')), 
            keybindings=KB_EXIT
        ).execute()))

        try:
            with open(os.path.join(profile_dir, 'profiles.yml'), 'r', encoding='utf-8') as f:
                p_data = yaml.safe_load(f)
                p_key = list(p_data.keys())[0]
                targets = list(p_data[p_key].get('outputs', {}).keys())
        except Exception as e:
            console.print(f"[bold red]❌ profiles.yml 로드 실패:[/bold red] {e}"); sys.exit(1)

        target_name = check_exit(inquirer.select(
            message="Target 선택:", 
            choices=targets, 
            default=target_name or cache.get("target"), 
            keybindings=KB_EXIT
        ).execute())
        
        db_config_for_schemas = get_db_config_from_profile(profile_dir, target_name)
        schema_name = check_exit(inquirer.fuzzy(
            message="조회할 DB 스키마 선택:", 
            choices=get_schemas(db_config_for_schemas), 
            default=schema_name or cache.get("schema", "public"), 
            keybindings=KB_EXIT
        ).execute())

        save_cache({"project_dir": project_dir, "profile_dir": profile_dir, "target": target_name, "schema": schema_name})

        # db_config_for_tables는 이미 db_config_for_schemas와 동일한 정보로 초기화되었을 것이므로 재호출 불필요
        tables_in_schema = get_db_tables(db_config_for_schemas, schema_name)
        selected_tables = check_exit(inquirer.fuzzy(
            message="대상 테이블 선택 (Right: 선택):", 
            choices=tables_in_schema, 
            multiselect=True, 
            match_exact=True, 
            default=selected_tables, # CLI 인자로 받은 테이블이 있다면 기본값으로 사용
            keybindings=KB_FUZZY_MULTI
        ).execute())
        if not selected_tables: 
            console.print("[bold yellow]선택된 테이블이 없어 종료합니다.[/bold yellow]"); sys.exit(0)

        # 모델 현황 리스팅 (대화형 모드에서만 보여줌)
        status_table = Table(title=f"[bold]선택된 모델 현황[/bold]", show_header=True, header_style="bold magenta")
        status_table.add_column("모델명", style="dim")
        status_table.add_column("상태", justify="center")
        status_table.add_column("현재 위치 (YAML)", style="cyan")

        for m in selected_tables:
            loc = find_model_location(project_dir, m)
            status = "[green]신규[/green]" if not loc else "[yellow]존재함[/yellow]"
            status_table.add_row(m, status, loc if loc else "-")
        console.print(status_table)

        action = check_exit(inquirer.select(
            message="작업 선택:", 
            choices=[{"name": "YAML 수정/추가 (통합)", "value": "edit"}, {"name": "YAML 출력만", "value": "print"}], 
            default="edit" if not print_only else "print", # CLI 인자로 받은 print_only가 있다면 기본값으로 사용
            keybindings=KB_EXIT
        ).execute())
        print_only = (action == "print")

        if not print_only:
            target_choices = get_all_yml_files(project_dir)
            target_yml_choice = check_exit(inquirer.select(
                message="통합 저장할 YAML 파일 선택:", 
                choices=target_choices + ["[신규 파일 생성]"], 
                default=output_yml_path, # CLI 인자로 받은 output_yml_path가 있다면 기본값으로 사용
                keybindings=KB_EXIT
            ).execute())

            if target_yml_choice == "[신규 파일 생성]":
                new_dir = check_exit(inquirer.filepath(
                    message="생성 경로:", 
                    default=os.path.join(project_dir, 'models'), 
                    validate=PathValidator(is_dir=True), 
                    keybindings=KB_EXIT
                ).execute())
                new_fn = check_exit(inquirer.text(
                    message="파일명 (예: marts.yml):", 
                    keybindings=KB_EXIT
                ).execute())
                output_yml_path = os.path.relpath(os.path.join(new_dir, new_fn), project_dir)
            else:
                output_yml_path = target_yml_choice
            
            if not check_exit(inquirer.confirm(message=f"선택한 {len(selected_tables)}개 모델을 '{output_yml_path}'로 통합하시겠습니까?", default=False).execute()): return
            
            # 대화형 모드에서 백업 여부 확인 (CLI 모드에서는 --no_backup 인자로 제어)
            no_backup = not check_exit(inquirer.confirm(message="파일 백업을 하시겠습니까?", default=True).execute())

    # --- CLI/대화형 모드 공통 로직 시작 ---

    # 대화형 모드 후 또는 CLI 모드에서 인자가 부족한 경우 재확인
    if not project_dir or not profile_dir or not target_name or not schema_name or not selected_tables or not output_yml_path:
        console.print("[bold red]❌ 오류:[/bold red] 모든 필수 정보가 제공되지 않았습니다. CLI 모드 사용 시 모든 인자를 제공하거나, 대화형 모드로 실행해주세요."); sys.exit(1)

    if not os.path.isdir(project_dir):
        console.print(f"[bold red]❌ 오류:[/bold red] DBT 프로젝트 디렉토리를 찾을 수 없습니다: {project_dir}"); sys.exit(1)
    if not os.path.isdir(profile_dir):
        console.print(f"[bold red]❌ 오류:[/bold red] profiles.yml 디렉토리를 찾을 수 없습니다: {profile_dir}"); sys.exit(1)

    try:
        db_config = get_db_config_from_profile(profile_dir, target_name)
    except (FileNotFoundError, ValueError, KeyError) as e:
        console.print(f"[bold red]❌ 오류:[/bold red] profiles.yml 설정 로드 실패: {e}"); sys.exit(1)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_root = os.path.join(project_dir, "models", "bak", "yml", timestamp)

    if print_only:
        console.print(f"[bold yellow]--- Print Only Mode ---[/bold yellow]")
        for m in selected_tables:
            try:
                cols, pks = get_table_detail(db_config, schema_name, m)
                entry = {"name": m, "description": f"Model for {m}", "columns": cols}
                if pks: entry["config"] = {"materialized": "incremental", "unique_key": pks}
                console.print(Panel(Syntax(yaml.dump({"models": [entry]}, allow_unicode=True, sort_keys=False), "yaml", theme="monokai"), title=m))
            except (ConnectionError, ImportError) as e:
                console.print(f"[bold red]❌ 오류:[/bold red] {m} 테이블 상세 정보 조회 실패: {e}"); sys.exit(1)
        console.print(f"[bold yellow]--- Print Only Mode 완료 ---[/bold yellow]")
        sys.exit(0)

    # 기존 yml 파일에서 중복 모델 제거
    remove_models_from_other_files(project_dir, selected_tables, output_yml_path, backup_root, no_backup)
    
    # 대상 yml 파일 백업
    if not no_backup:
        create_backup(project_dir, output_yml_path, backup_root)

    full_output_yml_path = os.path.join(project_dir, output_yml_path)
    os.makedirs(os.path.dirname(full_output_yml_path), exist_ok=True)
    
    final_data = {"version": 2, "models": []}
    if os.path.exists(full_output_yml_path):
        with open(full_output_yml_path, 'r', encoding='utf-8') as f:
            existing_data = yaml.safe_load(f)
            if existing_data and 'models' in existing_data:
                final_data = existing_data
            elif existing_data: # models 키는 없지만 다른 내용이 있으면 기존 내용 유지
                final_data.update(existing_data)
            
    for m in selected_tables:
        try:
            cols, pks = get_table_detail(db_config, schema_name, m)
            new_entry = {
                "name": m, 
                "description": f"Model for {m}", 
                "config": {"materialized": "incremental", "incremental_strategy": "append"},
                "columns": cols # DB 순서 보존됨
            }
            if pks: new_entry["config"]["unique_key"] = pks

            # 기존 모델이 존재하면 업데이트, 없으면 추가
            found = False
            for i, existing_model in enumerate(final_data['models']):
                if existing_model.get('name') == m:
                    final_data['models'][i] = new_entry
                    found = True
                    break
            if not found:
                final_data['models'].append(new_entry)
        except (ConnectionError, ImportError) as e:
            console.print(f"[bold red]❌ 오류:[/bold red] {m} 테이블 상세 정보 조회 실패: {e}")
            sys.exit(1)
        
    with open(full_output_yml_path, 'w', encoding='utf-8') as f:
        yaml.dump(final_data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    console.print(f"[bold green]✔ 모든 작업 완료 (DB 컬럼 순서 유지됨): {full_output_yml_path}[/bold green]")

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        console.print("[yellow]👋 종료.[/yellow]")
        sys.exit(0)
