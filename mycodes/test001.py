# pip install InquirerPy
from InquirerPy import inquirer

choices = ["stg_users", "stg_payments", "fct_orders", "dim_products", "mart_finance_daily"]

# 리스트가 뜬 상태에서 글자를 치면 해당 글자가 포함된 항목만 남음
selected = inquirer.fuzzy(
    message="실행할 모델을 선택하세요 (타이핑하여 필터링 가능):",
    choices=choices,
    match_exact=False, # 부분 일치 허용
).execute()

print(f"최종 선택: {selected}")