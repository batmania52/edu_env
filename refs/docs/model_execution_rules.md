## 4. 모델 실행 규칙

*   **날짜 변수 형식1**: `data_interval_start`, `data_interval_end` 변수의 값은 `Timestamp` 형태로 합니다.
*   **날짜 변수 형식2**: 날짜 변수 형식1의 규칙대로 사용자가 범위를 지정하지 않았을 경우 자의적으로 `Timestamp` 형식으로 변환합니다.
*   **개별 모델 실행**: 개별 모델 실행 시(`dbt run`) 옵션에 `-s` 또는 `--select [모델명]`을 사용합니다.
*   **run_mode 설정**: DBT 모델 실행 시, `run_mode` 변수를 `manual`로 설정하여 날짜 필터링 로직이 자동으로 변경되지 않도록 한다.
*   **full-refresh** : dbt run 시 `--full-refresh`는 어떠한 경우에도 사용하지 않는다, 사용이 필요하다고 판단되더라도 절대 사용 금지