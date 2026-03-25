## 7. 데이터 모델 상세 검증 지침

dbt 모델(stg 및 marts)의 변환 및 데이터 통합 정확성을 검증하기 위해 다음의 상세 검증 절차를 따릅니다. 이 방법은 단순히 샘플 데이터를 시각적으로 확인하는 것을 넘어, 소스와 타겟 간의 특정 레코드 일치 여부를 정밀하게 비교합니다.

### 7.1. 검증 절차

1.  **키 기반 샘플 레코드 추출:**
    *   검증 대상 모델(타겟)의 주 키 또는 조인 키가 되는 컬럼(예: `order_id`, `customer_id`, `product_id`)을 식별합니다.
    *   해당 모델의 주요 소스 테이블(예: `stg.stg_order` 또는 `stg.stg_customer`)에서 필터링 조건(날짜 범위 등)을 만족하는 대표적인 샘플 레코드 5개(또는 필요에 따라 조정)를 추출합니다. 이 샘플의 키 컬럼 값(예: `(1, 2, 3, 4, 5)`와 같은 `order_id` 튜플)을 확보합니다.
    *   **dbt 변수 사용 시:** 모델에 `start_date`, `end_date` 등의 dbt 변수가 사용될 경우, 모델을 실행할 때 `--vars '{"data_interval_start": "YYYY-MM-DD HH:MM:SS", "data_interval_end": "YYYY-MM-DD HH:MM:SS"}'` 형태로 변수 값을 명시적으로 전달하여 전체 데이터 범위가 처리되도록 합니다.
    *   **키가 없는 테이블:** 테이블에 명시적인 키(주 키, 고유 키 등)가 없는 경우, 모든 컬럼을 조합하여 복합 키로 사용하고 해당 컬럼들을 `WHERE` 조건에 활용하여 샘플 레코드를 추출합니다.

2.  **소스 데이터 추출 (모델 로직 재현):**
    *   샘플링된 키 컬럼 값들에 해당하는 레코드만을 대상으로, 검증 대상 모델의 SQL 로직(FROM 절, JOIN 조건, WHERE 절, 계산 로직 등)을 그대로 재현한 쿼리를 작성하여 소스 테이블(stg 또는 raw 테이블)에서 데이터를 추출합니다.
    *   이 쿼리는 타겟 모델이 생성되기 직전의 중간 결과물과 최대한 유사해야 합니다.
    *   추출된 데이터는 `source_data`로 명명합니다.

3.  **타겟 데이터 추출:**
    *   샘플링된 키 컬럼 값들을 `WHERE` 절에 사용하여, 빌드된 타겟 모델(예: `marts.orders_mart`)에서 해당하는 레코드들을 추출합니다.
    *   추출된 데이터는 `target_data`로 명명합니다.

4.  **데이터 비교 및 검증:**
    *   `source_data`와 `target_data`를 나란히 비교하여 모든 컬럼의 값, 데이터 타입, 순서 등이 일치하는지 시각적으로 검증합니다.
    *   특히, 타겟 모델에서 계산되거나 변환된 컬럼(예: `item_total`, `churn_risk_score`)의 값이 소스 데이터에서 예상되는 계산 결과와 일치하는지 주의 깊게 확인합니다.
    *   모든 데이터가 예상대로 일치하면 검증을 성공으로 간주합니다.

### 7.2. 활용 도구

*   `refs/edu/tools/verify_dbt_model.py`: dbt 모델의 소스(compiled SQL)와 타겟(실제 데이터베이스 테이블) 간의 카운트, 합계, 샘플 데이터를 비교하여 데이터 일관성을 검증하는 자동화된 스크립트입니다.

    **사용법:**

    ```bash
    /Users/macbook/projects/edu_project/venv_dbt/bin/python refs/edu/tools/verify_dbt_model.py \
      --project_dir /Users/macbook/projects/edu_project/dbt_projects/edu001 \
      --model_name stg.stg_customers \
      --date_column registration_date \
      --start_date YYYYMMDD 또는 YYYY-MM-DD \
      --end_date YYYYMMDD 또는 YYYY-MM-DD \
      --target dev
    ```

    **주요 파라미터:**
    *   `--project_dir`: DBT 프로젝트의 루트 디렉토리 경로.
    *   `--model_name`: 검증할 DBT 모델 이름 (예: `schema.model_name` 형식).
    *   `--date_column`: 데이터 필터링에 사용할 날짜 컬럼 이름.
    *   `--start_date`: 검증 시작 날짜 (YYYYMMDD 또는 YYYY-MM-DD 형식).
    *   `--end_date`: 검증 종료 날짜 (YYYYMMDD 또는 YYYY-MM-DD 형식).
    *   `--target`: 사용할 DBT 프로필 타겟 (예: `dev`, `container`).

    **참고:**
    *   스크립트 실행 전, 해당 DBT 모델이 `run_mode: manual` 변수를 사용하여 지정된 날짜 범위로 실행되었는지 확인해야 합니다.
