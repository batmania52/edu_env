import pandas as pd
import os

def generate_receipts_data():
    data_dir = "/Users/macbook/projects/edu_project/refs/edu/datas"
    
    # 기초 데이터 로드
    orders = pd.read_csv(os.path.join(data_dir, "edu_order.csv"))
    customers = pd.read_csv(os.path.join(data_dir, "edu_raw_customers.csv"))
    products = pd.read_csv(os.path.join(data_dir, "edu_raw_products.csv"))
    order_items = pd.read_csv(os.path.join(data_dir, "edu_order_items.csv"))

    # 데이터 매핑 및 조인 (영수증 상세 내역 구성)
    # 1. 주문 + 고객 정보
    receipts = orders.merge(customers, on='customer_id', how='inner')
    
    # 2. 주문 정보 + 주문 항목 (1:N)
    # 실제 운영 데이터라면 order_items에 order_id가 있어야 함
    receipts = receipts.merge(order_items, on='order_id', how='inner')
    
    # 3. 항목 + 상품 정보
    receipts = receipts.merge(products, on='product_id', how='inner')

    # 컬럼명 정리 (stg_receipts.sql 구조에 맞춤)
    receipts['receipt_id'] = range(1, len(receipts) + 1)
    receipts['total_order_amount'] = receipts['total_amount']
    receipts['item_quantity'] = receipts['quantity']
    receipts['item_price'] = receipts['price_y'] # 상품 가격
    receipts['item_total'] = receipts['item_quantity'] * receipts['item_price']

    final_cols = [
        'receipt_id', 'order_id', 'customer_id', 'customer_name', 'customer_email',
        'order_date', 'total_order_amount', 'product_id', 'product_name',
        'product_category', 'item_quantity', 'item_price', 'item_total'
    ]
    
    receipts_final = receipts[final_cols]
    
    # CSV 저장
    target_path = os.path.join(data_dir, "edu_receipts.csv")
    receipts_final.to_csv(target_path, index=False)
    print(f"Successfully generated {len(receipts_final)} rows for {target_path}")

if __name__ == "__main__":
    generate_receipts_data()
