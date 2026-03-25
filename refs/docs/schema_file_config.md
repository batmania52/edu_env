## 5. Schema 파일 설정 지침

*   **파일 위치**: 각 `schema.yml` 파일은 `models/스키마명` 디렉토리에 둡니다.
*   **테스트 생략**: `yml` 파일 내의 모델 정보에 `tests` 부분은 생략합니다.
*   **Config Block 관리**: `config` 블록은 `schema` 파일에서 관리합니다. 각 모델에 존재 시 `schema`로 옮긴 후 모델에서는 삭제합니다.
*   **Materialization 설정**: 모델 생성 시 `materialized='incremental'`, `incremental_strategy='append'`로만 설정합니다. (`view`는 사용 예정 없음)
*   **컬럼 정보 포함**: 컬럼명, `comment`, 타입, 데이터 길이를 테이블 기준으로 포함합니다. 정보를 모를 경우 DB에 접근해서 직접 정보를 가져와 넣습니다.
    ```컬럼부분 예시
    - name: suplcd
        description: suplcd
        data_type: varchar(18)
    ```
*   **Contract 설정 금지**: `contract` 관련 설정은 `schema.yml` 파일에 직접 넣지 않습니다.
