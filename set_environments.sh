# 가상환경이 안 켜져 있을 때 deactivate를 하면 에러가 날 수 있으므로 에러 메시지 숨김
deactivate 2>/dev/null

# 점(.) 뒤에 공백 필수, 대괄호 안팎에 공백 필수
. ./venv_dbt/bin/activate

if [[ $? -ne 0 ]]; then
    echo "Please set Virtual Env(Python 3.11)"
    exit 1
fi

python refs/edu/tools/manage_schemas_for_test.py
if [[ $? -ne 0 ]]; then
    echo "Generate Schemas Failed"
    exit 1
fi

python refs/edu/tools/execute_all_ddls.py
if [[ $? -ne 0 ]]; then
    echo "Table Create Failed"
    exit 1
fi

python refs/edu/tools/load_data_from_csv.py
if [[ $? -ne 0 ]]; then
    echo "Load Data Failed"
    exit 1
fi

python refs/edu/tools/initialize_log_infrastructure.py
if [[ $? -ne 0 ]]; then
    echo "Create Log Table Failed"
    exit 1
fi
