from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.constants import ExecutionMode
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pathlib import Path
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="cosmos")
warnings.filterwarnings("ignore", message=".*Artifact schema version.*")
warnings.filterwarnings("ignore", message=".*Airflow 3.0.0 Asset.*")
warnings.filterwarnings("ignore", message=".*AIP-60.*")

DBT_PROJECT_PATH = Path("/opt/airflow/dbt_projects/edu001")

def get_dbt_tag_task_group(dag, group_id, tag, schema="stg"):
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
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            dbt_executable_path="/opt/dbt-venv/bin/dbt",  # ← 변경
        ),
        render_config=RenderConfig(
            select=[f"tag:{tag}"],
            emit_datasets=False
        ),
        operator_args={
            "vars": {
                "data_interval_start": "{{ params.data_interval_start}}",
                "data_interval_end": "{{ params.data_interval_end }}",
                "run_mode": "{{ params.run_mode }}",
                "airflow_run_id": "{{ run_id }}"
            }
        }
    )