import csv
import os
from datetime import datetime, timedelta

def rescale_csv_dates():
    data_dir = "/Users/macbook/projects/edu_project/refs/edu/datas"
    target_start = datetime(2026, 3, 1)
    target_end = datetime(2026, 4, 30, 23, 59, 59)
    
    files_to_process = {
        "edu_order.csv": "order_date",
        "edu_purchase_orders.csv": "order_date",
        "edu_raw_customers.csv": "registration_date",
        "edu_raw_products.csv": "created_date"
    }

    # 1. 기준이 되는 주문 데이터의 시간 범위 파악 (Scale Ratio 계산용)
    order_file = os.path.join(data_dir, "edu_order.csv")
    if not os.path.exists(order_file):
        print(f"Error: {order_file} not found.")
        return
        
    dates = []
    with open(order_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dates.append(datetime.fromisoformat(row["order_date"].split('.')[0]))
            except: continue
            
    if not dates:
        print("Error: No valid dates found in edu_order.csv")
        return
        
    old_min = min(dates)
    old_max = max(dates)
    old_range = (old_max - old_min).total_seconds()
    new_range = (target_end - target_start).total_seconds()
    ratio = new_range / old_range if old_range > 0 else 1
    
    print(f"Rescaling ratio: {ratio:.4f} (Range: {old_range:.0f}s -> {new_range:.0f}s)")

    # 2. 각 파일별 날짜 치환 실행
    for filename, date_col in files_to_process.items():
        file_path = os.path.join(data_dir, filename)
        if not os.path.exists(file_path):
            print(f"Skipping {filename} (not found)")
            continue
            
        temp_data = []
        fieldnames = []
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                try:
                    # 기존 날짜 파싱 (ISO 또는 기타 형식 대응)
                    dt_str = row[date_col].split('.')[0].replace(' ', 'T')
                    old_dt = datetime.fromisoformat(dt_str)
                    
                    # 선형 변환: (old_val - old_min) * ratio + target_start
                    new_dt = target_start + timedelta(seconds=(old_dt - old_min).total_seconds() * ratio)
                    row[date_col] = new_dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    # 날짜 형식이 아니거나 오류 발생 시 원본 유지
                    pass
                temp_data.append(row)
        
        # 파일 업데이트
        with open(file_path, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(temp_data)
            
        print(f"Updated {filename} successfully.")

    print("\n[Success] All CSV dates rescaled to 2026-03-01 ~ 2026-04-30.")

if __name__ == "__main__":
    rescale_csv_dates()
