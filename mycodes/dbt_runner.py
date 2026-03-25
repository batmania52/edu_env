import os
import yaml
import json
import subprocess
import sys
from datetime import datetime
from InquirerPy import inquirer
from InquirerPy.validator import PathValidator, Validator, ValidationError

# Rich 라이브러리
from rich.console import Console
from rich.syntax import Syntax

console = Console()
CACHE_FILE = ".dbt_ui_cache.json"

class DateSequenceValidator(Validator):
    def __init__(self, start_date_func, message="종료일은 시작일보다 빠를 수 없습니다."):
        self._start_date_func = start_date_func
        self._message = message

    def validate(self, document):
        end_date_str = document.text
        start_date_str = self._start_date_func()
        try:
            start_dt = datetime.strptime(start_date_str, '%Y%m%d')
            end_dt = datetime.strptime(end_date_str, '%Y%m%d')
            if end_dt < start_dt:
                raise ValidationError(message=self._message, cursor_position=len(end_date_str))
        except ValueError:
            raise ValidationError(message="올바른 날짜 형식이 아닙니다 (YYYYMMDD)", cursor_position=len(end_date_str))

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

def validate_date_format(text):
    try:
        datetime.strptime(text, '%Y%m%d')
        return True
    except ValueError:
        return "올바른 날짜 형식이 아닙니다 (YYYYMMDD)"

def convert_to_dbt_ts(date_str, is_end=False):
    dt = datetime.strptime(date_str, '%Y%m%d')
    return dt.strftime('%Y-%m-%d 23:59:59') if is_end else dt.strftime('%Y-%m-%d 00:00:00')

def get_run_execution_details(project_dir, model_name):
    results_path = os.path.join(project_dir, 'target', 'run_results.json')
    if not os.path.exists(results_path): return None, None
    try:
        with open(results_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for result in data.get('results', []):
                uid = result.get('unique_id', '')
                if uid.startswith('model.') and uid.endswith(f'.{model_name}'):
                    adapter_resp = result.get('adapter_response', {})
                    rows = adapter_resp.get('rows_affected')
                    if rows is None and '_message' in adapter_resp:
                        parts = adapter_resp['_message'].split()
                        if parts and parts[-1].isdigit(): rows = int(parts[-1])
                    return rows, result.get('execution_time')
    except: return None, None
    return None, None

def get_dbt_models(project_dir):
    models = []
    models_path = os.path.join(project_dir, 'models')
    if not os.path.exists(models_path): return []
    for root, _, files in os.walk(models_path):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                try:
                    with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
                        data = yaml.safe_load(f)
                        if data and 'models' in data:
                            models.extend([m['name'] for m in data['models']])
                except: continue
    return sorted(list(set(models)))

def get_compiled_sql(project_dir, model_name):
    target_base = os.path.join(project_dir, 'target', 'compiled')
    for root, _, files in os.walk(target_base):
        if f"{model_name}.sql" in files:
            with open(os.path.join(root, f"{model_name}.sql"), 'r', encoding='utf-8') as f:
                return f.read().strip()
    return None

def run_app():
    cache = load_cache()
    
    # 입력받은 즉시 os.path.abspath로 절대 경로 변환
    raw_project_dir = inquirer.filepath(
        message="1. 프로젝트 경로 (--project-dir):",
        default=cache.get("project_dir", os.getcwd()),
        validate=PathValidator(is_dir=True)
    ).execute()
    project_dir = os.path.abspath(raw_project_dir)

    raw_profile_dir = inquirer.filepath(
        message="2. 프로파일 경로 (--profiles-dir):",
        default=cache.get("profile_dir", os.path.join(project_dir, '.dbt'))
    ).execute()
    profile_dir = os.path.abspath(raw_profile_dir)

    # 2.1 Target 추출
    try:
        with open(os.path.join(profile_dir, 'profiles.yml'), 'r', encoding='utf-8') as f:
            profiles = yaml.safe_load(f)
            profile_name = list(profiles.keys())[0]
            targets = list(profiles[profile_name]['outputs'].keys())
            target = inquirer.select(message="2.1 Target 선택:", choices=targets, default=cache.get("target")).execute()
    except Exception as e:
        console.print(f"[bold red]❌ profiles.yml 로드 실패:[/bold red] {e}"); return

    # 4. 모델 선택
    models = get_dbt_models(project_dir)
    selected_model = inquirer.fuzzy(message="4. 모델 선택:", choices=models, default=cache.get("selected_model")).execute()
    run_mode = inquirer.select(message="5. 실행 모드:", choices=["manual", "schedule"], default=cache.get("run_mode", "manual")).execute()

    # 6. 날짜 입력
    start_date_str = inquirer.text(
        message="6. 시작 날짜 (YYYYMMDD):", 
        default=cache.get("start_date", datetime.now().strftime('%Y%m%d')),
        validate=validate_date_format
    ).execute()

    end_date_str = inquirer.text(
        message="6. 종료 날짜 (YYYYMMDD):", 
        default=cache.get("end_date", start_date_str),
        validate=DateSequenceValidator(lambda: start_date_str)
    ).execute()

    # 캐시 저장 (원본 입력값 저장)
    save_cache({
        "project_dir": raw_project_dir, "profile_dir": raw_profile_dir, "target": target,
        "selected_model": selected_model, "run_mode": run_mode,
        "start_date": start_date_str, "end_date": end_date_str
    })

    start_ts = convert_to_dbt_ts(start_date_str)
    end_ts = convert_to_dbt_ts(end_date_str, is_end=True)
    vars_json = json.dumps({"data_interval_start": start_ts, "data_interval_end": end_ts, "run_mode": run_mode})

    # 공통 옵션 리스트
    common_args = [
        "--select", selected_model,
        "--target", target,
        "--vars", vars_json,
        "--project-dir", project_dir,
        "--profiles-dir", profile_dir
    ]

    # 7. dbt compile
    console.print(f"\n[bold blue]⚙️  dbt compile 실행 중... ({selected_model})[/bold blue]")
    compile_cmd = ["dbt", "-q", "compile"] + common_args
    
    try:
        # cwd를 project_dir(절대경로)로 설정하여 안정성 확보
        subprocess.run(compile_cmd, cwd=project_dir, check=True, capture_output=True)
        sql = get_compiled_sql(project_dir, selected_model)
        if sql:
            console.print(f"\n[bold green]--- COMPILED SQL START (복사 가능) ---[/bold green]")
            console.print(Syntax(sql, "sql", theme="monokai", line_numbers=False))
            console.print(f"[bold green]--- COMPILED SQL END ---[/bold green]\n")
    except subprocess.CalledProcessError as e:
        console.print(f"\n[bold red]❌ 컴파일 오류 발생[/bold red]\n{e.stderr.decode('utf-8')}"); return

    # 8. dbt run
    run_cmd = ["dbt", "-q", "run"] + common_args
    full_cmd_str = f"dbt {' '.join(run_cmd[1:])}"
    console.print(f"[bold cyan]🚀 Run Command:[/bold cyan]\n\n[yellow]{full_cmd_str}[/yellow]\n")

    if inquirer.confirm(message=f"'{selected_model}' 모델을 실행하시겠습니까?", default=False).execute():
        console.print(f"\n[bold yellow]▶ 실행 중...[/bold yellow]")
        result = subprocess.run(run_cmd, cwd=project_dir, capture_output=True, text=True)
        if result.returncode == 0:
            rows, etime = get_run_execution_details(project_dir, selected_model)
            row_info = f" | Rows: {rows}" if rows is not None else ""
            time_info = f" | Time: {etime:.2f}s" if etime is not None else ""
            console.print(f"\n[bold green]✔ {selected_model} 성공!{row_info}{time_info}[/bold green]")
        else:
            console.print(f"\n[bold red]✘ 실패[/bold red]\n\n[red]{result.stdout + result.stderr}[/red]")
    else:
        console.print("\n[yellow]⚠ 실행이 취소되었습니다.[/yellow]")

if __name__ == "__main__":
    try:
        run_app()
        console.print("\n[dim]프로그램 종료.[/dim]")
    except (KeyboardInterrupt, EOFError):
        console.print("\n\n[bold yellow]👋 종료합니다.[/bold yellow]")
        sys.exit(0)