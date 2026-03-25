# DBT 실행 결과 검증 프로그램 작성 가이드
## Program 작성 규칙
** fstring은 되도록이면 개행하지 않고 개행이 필요할 경우 삼중따옴표를 사용할것
## 파라미터
** start_date ** : 타임스탬프를 기본으로 하며 YYYYMMDD나 YYYY-MM-DD의 경우 타임스탬프로 치환
** end_date ** : 타임스탬프를 기본으로 하며 YYYYMMDD나 YYYY-MM-DD의 경우 타임스탬프로 치환
** model name ** : 스키마.모델명으로 받음
** project-dir ** : dbt project 위치
** 날짜 조건 컬럼 ** : 날짜 조건 컬럼을 명시하고 해당 파라미터가 없을 경우 검증 sql에도 날짜를 포함하지 않음

## 검증 데이터
count, sum sample(5건)

## 구현
### Source 검증
** 인자로 받은 파라미터들을 통해 dbt compile을 수행, 출력된 sql을 변수에 저장
** compile은 아래의 문법을 참조한다
``` compile
source venv_dbt/bin/activate && cd dbt_projects/edu001 && dbt compile --profiles-dir .dbt --project-dir /Users/macbook/projects/edu/dbt_projects/edu001 -s stg_orders -q
```
** 출력된 sql을 cte로 래핑하여 count, sum, sample 데이터를 추출

### Target 검증
** Target 테이블의 PK를 DB를 조회해서 추출
** sample검증은 Source 검증에서 추출한 sample검증에서 Target 테이블의 PK에 해당되는 column을 where 조건으로 하여 추출, PK가 없을 경우 모든 열의 값을 where 조건으로 사용함
** 인자로 받은 파라미터들을 통해 target 테이블에 count, sum, sample 데이터를 추출