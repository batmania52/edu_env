from airflow.operators.bash import BashOperator
import os

# dbt 프로젝트의 경로 (Airflow 컨테이너 내부)
DBT_PROJECT_DIR = "/opt/airflow/dbt_projects/edu001"
DBT_PROFILES_DIR = "/opt/airflow/dbt_projects/edu001/.dbt"

def get_dbt_bash_operator(task_id, command, select=None, vars=None):
    """
    dbt 명령을 실행하는 BashOperator를 반환합니다.
    Airflow의 run_id를 dbt 변수로 전달하도록 보완되었습니다.
    """
    
    # 1. 기본 dbt 명령 구성
    dbt_cmd = f"dbt {command} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    
    # 2. --select 추가
    if select:
        dbt_cmd += f" --select {select}"
        
    # 3. --vars 추가
    # 기본적으로 Airflow의 기간과 run_id를 dbt에 전달하도록 구성
    if vars:
        import json
        vars_json = json.dumps(vars)
        dbt_cmd += f" --vars '{vars_json}'"
    else:
        # data_interval_start/end 정규화 및 airflow_run_id 전달
        dbt_cmd += (
            " --vars '{"
            "\"data_interval_start\": \"{{ data_interval_start.strftime(\"%Y-%m-%d %H:%M:%S\") }}\", "
            "\"data_interval_end\": \"{{ data_interval_end.strftime(\"%Y-%m-%d %H:%M:%S\") }}\", "
            "\"airflow_run_id\": \"{{ run_id }}\""
            "}'"
        )

    return BashOperator(
        task_id=task_id,
        bash_command=dbt_cmd,
        env={**os.environ},
        append_env=True,
    )
