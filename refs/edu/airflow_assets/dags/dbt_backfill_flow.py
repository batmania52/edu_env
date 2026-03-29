from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param
from airflow.models.dag import DagModel
from airflow.utils.session import provide_session
from datetime import datetime
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

@provide_session
def get_dag_list_for_backfill(session=None):
    """
    Airflow DB를 쿼리하여 활성화되고 정기 스케줄(next_dagrun)이 있는 DAG 목록만 반환
    """
    try:
        query = session.query(DagModel.dag_id).filter(
            DagModel.is_active == True,
            DagModel.is_paused == False,
            DagModel.next_dagrun.isnot(None),
            ~DagModel.timetable_summary.in_(['None', 'Manual', 'null'])
        )
        dag_list = [row.dag_id for row in query.all() if row.dag_id != 'admin_cli_backfill']
        return sorted(dag_list) if dag_list else ["dbt_main_orchestrator", "dbt_daily_flow"]
    except Exception:
        # DB 연결 장애 시에만 기본값 반환
        return ["dbt_main_orchestrator", "dbt_daily_flow"]

active_dags = get_dag_list_for_backfill()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1, tzinfo=local_tz),
}

with DAG(
    'admin_cli_backfill',
    default_args=default_args,
    description='[관리자] 입력한 날짜의 데이터를 안전하게 보정하는 백필 도구 (KST 기준)',
    schedule_interval=None,
    params={
        "target_dag_id": Param(
            active_dags[0] if active_dags else "", 
            type="string", 
            enum=active_dags,
            description="목록에서 대상 DAG를 선택하세요. (활성화된 정기 배치만 표시)"
        ),
        "manual_target_dag_id": Param(
            "", 
            type="string", 
            description="목록에 없는 DAG는 직접 입력하세요. (입력 시 우선 적용)"
        ),
        "start_date": Param(
            "", 
            type="string", 
            description="데이터 처리 시작일 (YYYY-MM-DD)"
        ),
        "end_date": Param(
            "", 
            type="string", 
            description="데이터 처리 종료일 (YYYY-MM-DD)"
        ),
        "reset_dagruns": Param(
            False, 
            type="boolean", 
            description="기존 성공 이력을 삭제하고 전체 다시 실행할까요? (--reset-dagruns)"
        ),
        "rerun_failed_tasks": Param(
            False, 
            type="boolean", 
            description="성공한 태스크는 건너뛰고 실패한 것만 다시 시도할까요? (--rerun-failed-tasks)"
        ),
    },
    user_defined_macros={
        'pendulum': pendulum
    },
    catchup=False,
    tags=['admin', 'safety', 'backfill', 'cli', 'dynamic', 'hybrid', 'smart_filter'],
) as dag:

    run_backfill = BashOperator(
        task_id='execute_airflow_backfill',
        bash_command="""
            FINAL_ID="{{ params.manual_target_dag_id or params.target_dag_id }}"
            
            if [ -z "$FINAL_ID" ] || [ -z "{{ params.start_date }}" ] || [ -z "{{ params.end_date }}" ]; then
                echo "❌ [Error] 필수 입력값이 누락되었습니다."
                exit 1
            fi

            TODAY=$(date +%Y-%m-%d)
            if [[ "{{ params.end_date }}" > "$TODAY" ]]; then
                echo "❌ [Error] 미래 날짜({{ params.end_date }})는 백필할 수 없습니다. (오늘: $TODAY)"
                exit 1
            fi

            # Jinja와 Python으로 날짜 보정 (+1 day)
            B_START="{{ (pendulum.parse(params.start_date).add(days=1)).strftime('%Y-%m-%d') }}"
            B_END="{{ (pendulum.parse(params.end_date).add(days=1)).strftime('%Y-%m-%d') }}"

            RESET_FLAG=""
            if [ "{{ params.reset_dagruns }}" = "True" ]; then
                RESET_FLAG="--reset-dagruns"
            fi

            RERUN_FLAG=""
            if [ "{{ params.rerun_failed_tasks }}" = "True" ]; then
                RERUN_FLAG="--rerun-failed-tasks"
            fi

            echo "🚀 백필 시작 대상: $FINAL_ID"
            echo "📊 보정된 날짜 (Logical Date): $B_START ~ $B_END"
            
            airflow dags backfill \
                --start-date "$B_START" \
                --end-date "$B_END" \
                $RESET_FLAG \
                $RERUN_FLAG \
                --yes \
                "$FINAL_ID"
        """
    )
