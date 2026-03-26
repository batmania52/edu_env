import flet as ft
import os
import yaml
import json
import shutil
import sys
from datetime import datetime
import psycopg2
import io
import contextlib

# Rich 라이브러리 사용하지 않음 (Flet UI에 맞게 변경)
# Rich console 객체 대신 Flet Text 컨트롤에 로그 출력

# 기존 헬퍼 함수들 (generate_model_yml_cli.py에서 가져옴)
CACHE_FILE = ".dbt_schema_cache.json"

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

# check_exit은 GUI에서는 필요 없음 (예외 처리로 대체)

def is_model_yml(full_path):
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
    # console.print(f"[dim][Backup] {file_rel_path} 백업 완료[/dim]") # Flet 로그로 변경
    return f"[Backup] {file_rel_path} 백업 완료"

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

def get_profile_targets(profile_dir):
    profile_path = os.path.join(profile_dir, 'profiles.yml')
    if not os.path.exists(profile_path):
        return []
    with open(profile_path, 'r', encoding='utf-8') as f:
        profiles = yaml.safe_load(f)
        if not profiles:
            return []
        profile_key = list(profiles.keys())[0]
        return list(profiles[profile_key].get('outputs', {}).keys())

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
        return []
    for root, _, files in os.walk(models_root):
        if 'bak' in root.split(os.sep): continue
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                full_path = os.path.join(root, file)
                if is_model_yml(full_path):
                    yml_files.append(os.path.relpath(full_path, project_dir))
    return sorted(yml_files)

def remove_models_from_other_files(project_dir, model_names, target_yml, backup_root, no_backup, log_output_func):
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
                    log_output_func(create_backup(project_dir, yml, backup_root))
                data['models'] = new_models
                with open(full_path, 'w', encoding='utf-8') as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
                log_output_func(f"[Cleanup] {yml} 정리 완료")

# Flet 앱의 main 함수
def main(page: ft.Page):
    page.title = "DBT Schema YAML Generator"
    page.vertical_alignment = ft.MainAxisAlignment.START
    page.window_width = 800
    page.window_height = 900

    cache = load_cache()

    # 로그 출력을 위한 텍스트 필드
    log_text = ft.TextField(
        multiline=True, 
        read_only=True, 
        min_lines=10, 
        max_lines=20, 
        expand=True,
        scroll_to_cursor=True,
        border_radius=5,
        border_color=ft.colors.GREY_300,
        content_padding=10
    )

    def log_message(message):
        log_text.value += f"{message}
"
        page.update()

    # FilePicker 설정
    file_picker = ft.FilePicker()
    page.overlay.append(file_picker)

    # 이벤트 핸들러 정의
    async def pick_project_dir_result(e: ft.FilePickerResultEvent):
        if e.path:
            tf_project_dir.value = e.path
            tf_profile_dir.value = os.path.join(e.path, ".dbt")
            await page.update_async()
            await update_profile_targets()
        else:
            log_message("프로젝트 디렉토리 선택 취소됨.")

    async def pick_profile_dir_result(e: ft.FilePickerResultEvent):
        if e.path:
            tf_profile_dir.value = e.path
            await page.update_async()
            await update_profile_targets()
        else:
            log_message("프로파일 디렉토리 선택 취소됨.")

    async def update_profile_targets():
        dd_profile_target.options.clear()
        profile_dir = tf_profile_dir.value
        if profile_dir and os.path.exists(os.path.join(profile_dir, 'profiles.yml')):
            try:
                targets = get_profile_targets(profile_dir)
                dd_profile_target.options = [ft.dropdown.Option(target) for target in targets]
                if cache.get("target") in targets:
                    dd_profile_target.value = cache.get("target")
                elif targets:
                    dd_profile_target.value = targets[0]
                else:
                    dd_profile_target.value = None
                await page.update_async()
                await update_db_schemas()
            except Exception as ex:
                log_message(f"프로파일 타겟 로드 실패: {ex}")
        else:
            dd_profile_target.value = None
            await page.update_async()
            await update_db_schemas() # 스키마도 비워야 함

    async def update_db_schemas(e=None):
        dd_db_schema.options.clear()
        dd_db_schema.value = None
        if tf_profile_dir.value and dd_profile_target.value:
            try:
                db_config = get_db_config_from_profile(tf_profile_dir.value, dd_profile_target.value)
                schemas = get_schemas(db_config)
                dd_db_schema.options = [ft.dropdown.Option(schema) for schema in schemas]
                if cache.get("schema") in schemas:
                    dd_db_schema.value = cache.get("schema")
                elif schemas:
                    dd_db_schema.value = schemas[0]
                else:
                    dd_db_schema.value = None
                await page.update_async()
                await update_tables()
            except Exception as ex:
                log_message(f"DB 스키마 로드 실패: {ex}")
        else:
            await page.update_async()
            await update_tables() # 테이블도 비워야 함

    async def update_tables(e=None):
        lv_tables.controls.clear()
        if tf_profile_dir.value and dd_profile_target.value and dd_db_schema.value:
            try:
                db_config = get_db_config_from_profile(tf_profile_dir.value, dd_profile_target.value)
                tables = get_db_tables(db_config, dd_db_schema.value)
                for table in tables:
                    lv_tables.controls.append(ft.Checkbox(label=table, value=False, data=table))
                await page.update_async()
            except Exception as ex:
                log_message(f"DB 테이블 로드 실패: {ex}")
        else:
            await page.update_async()

    # UI 요소 정의
    tf_project_dir = ft.TextField(
        label="1. DBT 프로젝트 디렉토리", 
        value=cache.get("project_dir", os.getcwd()), 
        expand=True,
        on_change=lambda e: page.run_task(update_profile_targets()) # 디렉토리 변경 시 프로파일 타겟 업데이트
    )
    btn_pick_project_dir = ft.ElevatedButton(
        "선택", 
        icon=ft.icons.FOLDER_OPEN,
        on_click=lambda _: file_picker.get_directory_path(on_result=pick_project_dir_result)
    )

    tf_profile_dir = ft.TextField(
        label="2. profiles.yml 디렉토리", 
        value=cache.get("profile_dir", os.path.join(tf_project_dir.value if tf_project_dir.value else os.getcwd(), ".dbt")), 
        expand=True,
        on_change=lambda e: page.run_task(update_profile_targets()) # 디렉토리 변경 시 프로파일 타겟 업데이트
    )
    btn_pick_profile_dir = ft.ElevatedButton(
        "선택", 
        icon=ft.icons.FOLDER_OPEN,
        on_click=lambda _: file_picker.get_directory_path(on_result=pick_profile_dir_result)
    )

    dd_profile_target = ft.Dropdown(
        label="3. 프로파일 타겟",
        options=[],
        on_change=lambda e: page.run_task(update_db_schemas()),
        expand=True
    )

    dd_db_schema = ft.Dropdown(
        label="4. DB 스키마",
        options=[],
        on_change=lambda e: page.run_task(update_tables()),
        expand=True
    )

    lv_tables = ft.ListView(
        expand=True, 
        spacing=5, 
        padding=10, 
        auto_scroll=True,
        controls=[]
    )
    # 테이블 선택 영역을 Card로 감싸기
    table_selection_card = ft.Card(
        content=ft.Column(
            [
                ft.Text("5. 테이블 선택 (다중 선택 가능)", weight=ft.FontWeight.BOLD),
                ft.Container(
                    content=lv_tables,
                    height=200, # 테이블 리스트뷰의 높이 고정
                    border=ft.border.all(1, ft.colors.GREY_300),
                    border_radius=5
                )
            ]
        ),
        margin=10,
        elevation=2
    )

    btn_run = ft.ElevatedButton("Run", on_click=None) # 나중에 구현

    # 페이지에 컨트롤 추가
    page.add(
        ft.Row(
            [tf_project_dir, btn_pick_project_dir],
            alignment=ft.MainAxisAlignment.START
        ),
        ft.Row(
            [tf_profile_dir, btn_pick_profile_dir],
            alignment=ft.MainAxisAlignment.START
        ),
        ft.Row(
            [dd_profile_target, dd_db_schema],
            alignment=ft.MainAxisAlignment.START
        ),
        table_selection_card,
        btn_run,
        ft.Text("로그 출력:", weight=ft.FontWeight.BOLD),
        log_text
    )

    # 초기 로드 시 프로파일 타겟 업데이트
    page.on_connect = lambda e: page.run_task(update_profile_targets())
    
ft.app(target=main)