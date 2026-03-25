# Airflow DAG 개발 및 설정 지침

이 문서는 Airflow를 통한 dbt 워크플로우 자동화 시 준수해야 할 표준 규칙을 설명합니다.

## 1. DAG 기본 설정 규칙

모든 DAG 생성 시 예기치 않은 자동 실행 및 과거 데이터 오적재를 방지하기 위해 다음 설정을 반드시 포함해야 합니다.

*   **is_paused_upon_creation = True**
    *   **목적**: DAG 파일 생성/수정 후 스케줄러가 처음 인지할 때 '일시정지(Paused)' 상태로 등록되도록 함.
    - **이유**: 개발자가 UI에서 직접 로직을 최종 확인하고 활성화(Toggle ON)하기 전에는 절대 실행되지 않아야 함.
*   **catchup = False**
    *   **목적**: `start_date`와 현재 시점 사이의 누락된 실행(Backfill)을 방지.
    - **이유**: 스케줄 등록 즉시 과거 데이터 수천 건이 한꺼번에 도는 현상을 방지함.
*   **depends_on_past = False**
    *   **목적**: 이전 날짜 배치의 성공 여부와 관계없이 현재 배치를 실행.
    - **이유**: 특정 일자의 일시적 장애가 전체 자동화 파이프라인의 중단으로 이어지는 것을 방지. (특수한 데이터 의존성이 있는 경우에만 개별 설정)

## 2. dbt 연동 표준 프로세스

### 2.1. 공통 모듈 사용
직접적인 Operator 코딩 대신 `plugins` 디렉토리의 공통 유틸리티를 사용하여 일관성을 유지합니다.
*   **Cosmos 방식**: `dbt_cosmos_utils.get_dbt_tag_task_group()` 사용. (모델 단위 시각화 필요 시)
*   **Bash 방식**: `dbt_bash_utils.get_dbt_bash_operator()` 사용. (복합 명령 제어 필요 시)

### 2.2. 컨텍스트 변수 전달
dbt 실행 시 반드시 Airflow의 실행 컨텍스트 정보를 dbt `--vars`로 전달하여 로그 및 데이터 범위를 일치시킵니다.
*   **전달 필수 항목**: `data_interval_start`, `data_interval_end`, `run_id` (dbt 로그의 `airflow_run_id` 컬럼에 기록됨)

## 3. 표준 코드 예시

```python
from airflow import DAG
from datetime import datetime, timedelta
from dbt_bash_utils import get_dbt_bash_operator # 공통 모듈 활용

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,    # [지침] 이전 실행 의존성 제거
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'standard_dbt_workflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,               # [지침] 과거 데이터 백필 방지
    is_paused_upon_creation=True, # [지침] 활성화 전 자동 실행 방지
    tags=['standard', 'dbt'],
) as dag:

    dbt_run = get_dbt_bash_operator(
        task_id='dbt_run_marts',
        command='run',
        select='marts'
    )
```
