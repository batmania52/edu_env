#!/bin/bash
cd "$(dirname "$0")"

# 가상환경이 안 켜져 있을 때 deactivate를 하면 에러가 날 수 있으므로 에러 메시지 숨김
deactivate 2>/dev/null

# 가상환경이 없으면 자동 설치
if [[ ! -d "./venv_dbt" ]]; then
    echo "가상환경이 없습니다. 자동 설치를 시작합니다..."
    python3.11 -m venv venv_dbt
    if [[ $? -ne 0 ]]; then
        echo "가상환경 생성 실패. Python 3.11이 설치되어 있는지 확인해주세요."
        return 1
    fi
    . ./venv_dbt/bin/activate
    echo "패키지를 설치합니다. 잠시 기다려주세요..."
    pip install -r refs/edu/requirements.txt
    if [[ $? -ne 0 ]]; then
        echo "패키지 설치 실패"
        return 1
    fi
    echo "가상환경 설치 완료!"
else
    # 점(.) 뒤에 공백 필수, 대괄호 안팎에 공백 필수
    . ./venv_dbt/bin/activate
    if [[ $? -ne 0 ]]; then
        echo "가상환경 활성화에 실패했습니다."
        return 1
    fi
fi

python refs/edu/tools/manage_schemas_for_test.py
if [[ $? -ne 0 ]]; then
    echo "Generate Schemas Failed"
    return 1
fi

python refs/edu/tools/execute_all_ddls.py
if [[ $? -ne 0 ]]; then
    echo "Table Create Failed"
    return 1
fi

python refs/edu/tools/load_data_from_csv.py
if [[ $? -ne 0 ]]; then
    echo "Load Data Failed"
    return 1
fi

python refs/edu/tools/initialize_log_infrastructure.py
if [[ $? -ne 0 ]]; then
    echo "Create Log Table Failed"
    return 1
fi
