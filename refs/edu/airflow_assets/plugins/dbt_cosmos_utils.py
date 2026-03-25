from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pathlib import Path
import os

# dbt 프로젝트 루트 경로 (Airflow 컨테이너 내부 기준)
DBT_PROJECT_PATH = Path("/opt/airflow/dbt_projects/edu001")

def get_dbt_tag_task_group(dag, group_id, tag, schema="stg"):
    """
    특정 dbt tag를 기준으로 TaskGroup을 생성합니다.
    Airflow run_id 및 기간 변수를 dbt에 전달하도록 구성되었습니다.
    """
    
    profile_config = ProfileConfig(
        profile_name="edu001",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="postgres_default",
            profile_args={"schema": schema},
        ),
    )

    return DbtTaskGroup(
        group_id=group_id,
        dag=dag,
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[f"tag:{tag}"],
        ),
        # Cosmos에서 Airflow context를 dbt vars로 전달하는 설정
        operator_args={
            "vars": {
                "data_interval_start": "{{ data_interval_start.strftime('%Y-%m-%d %H:%M:%S') }}",
                "data_interval_end": "{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}",
                "airflow_run_id": "{{ run_id }}"
            }
        }
    )
