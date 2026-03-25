import json
import psycopg2
import os
import random
from datetime import datetime, timedelta
import pandas as pd

def get_db_config(db_config_path, db_key='postgres_local'):
    """DB 설정 파일을 읽어 DB 접속 정보를 반환합니다."""
    try:
        with open(db_config_path, 'r') as f:
            db_configs = json.load(f)
        db_config = db_configs.get(db_key)
        if not db_config:
            raise ValueError(f"오류: {db_key}에 대한 DB 설정이 {db_config_path}에 없습니다.")
        return db_config
    except FileNotFoundError:
        raise FileNotFoundError(f"오류: DB 설정 파일 {db_config_path}을 찾을 수 없습니다.")
    except json.JSONDecodeError:
        raise ValueError(f"오류: DB 설정 파일 {db_config_path}의 형식이 올바르지 않습니다.")

def generate_dummy_data(num_records=1000, min_customer_id=1, max_customer_id=1999):
    """
    edu.purchase_orders 테이블에 삽입할 더미 데이터를 생성합니다.
    """
    data = []
    statuses = ['pending', 'completed', 'canceled']
    start_date = datetime(2025, 1, 1)
    end_date = datetime.now()

    for i in range(num_records):
        customer_id = random.randint(min_customer_id, max_customer_id)
        
        # 랜덤 날짜 생성
        random_days = random.randint(0, (end_date - start_date).days)
        order_date = start_date + timedelta(days=random_days)
        order_date = order_date.replace(hour=random.randint(0, 23),
                                        minute=random.randint(0, 59),
                                        second=random.randint(0, 59))

        total_amount = round(random.uniform(100.00, 2000.00), 2)
        status = random.choice(statuses)

        data.append({
            'customer_id': customer_id,
            'order_date': order_date,
            'total_amount': total_amount,
            'status': status
        })
    return pd.DataFrame(data)

def insert_data_to_db(df, db_config, table_name='edu.purchase_orders'):
    """
    DataFrame의 데이터를 PostgreSQL 테이블에 삽입합니다.
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # 데이터 삽입
        for _, row in df.iterrows():
            # purchase_order_id는 SERIAL이므로 INSERT 시 제외
            insert_query = f"""
            INSERT INTO {table_name} (customer_id, order_date, total_amount, status)
            VALUES (%s, %s, %s, %s)
            """
            cur.execute(insert_query, (row['customer_id'], row['order_date'], row['total_amount'], row['status']))
        conn.commit()
        print(f"성공: {len(df)}개의 더미 데이터가 {table_name} 테이블에 삽입되었습니다.")
        return True
    except Exception as e:
        print(f"오류: 데이터 삽입 중 오류 발생: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    db_config_path = os.path.normpath(os.path.join(current_dir, "../../../airflow/dbconf.json"))
    csv_output_path = os.path.normpath(os.path.join(current_dir, "../../datas/edu_purchase_orders.csv"))
    
    print(f"Resolved DB config path: {db_config_path}")
    print(f"Resolved CSV output path: {csv_output_path}")

    try:
        db_config = get_db_config(db_config_path)
    except (FileNotFoundError, ValueError) as e:
        print(e)
        exit(1)

    # 더미 데이터 생성 (customer_id 범위는 1-1999로 설정)
    dummy_df = generate_dummy_data(num_records=1000, min_customer_id=1, max_customer_id=1999)
    print(f"""생성된 더미 데이터 미리보기:
{dummy_df.head()}""")

    # 데이터베이스에 삽입
    if insert_data_to_db(dummy_df, db_config):
        # CSV로 저장
        os.makedirs(os.path.dirname(csv_output_path), exist_ok=True)
        dummy_df.to_csv(csv_output_path, index=False)
        print(f"성공: 더미 데이터가 {csv_output_path}에 CSV 파일로 저장되었습니다.")
    else:
        print("데이터베이스 삽입에 실패하여 CSV 파일 저장을 건너뜜.")
