import pandas as pd
import numpy as np
import os

def generate_order_items_data(seed=42):
    np.random.seed(seed)
    data_dir = "/Users/macbook/projects/edu_project/refs/edu/datas"

    orders   = pd.read_csv(os.path.join(data_dir, "edu_order.csv"))
    products = pd.read_csv(os.path.join(data_dir, "edu_raw_products.csv"))

    order_ids   = orders["order_id"].values
    product_ids = products["product_id"].values
    price_map   = products.set_index("product_id")["price"].to_dict()

    rows = []
    item_id = 1
    for oid in order_ids:
        n_items = np.random.randint(1, 4)                    # 주문당 1~3개 항목
        chosen_products = np.random.choice(product_ids, size=n_items, replace=False)
        for pid in chosen_products:
            quantity = int(np.random.randint(1, 11))         # 수량 1~10
            price    = round(float(price_map[pid]), 2)
            rows.append({
                "id":         item_id,
                "order_id":   int(oid),
                "product_id": int(pid),
                "quantity":   quantity,
                "price":      price,
            })
            item_id += 1

    df = pd.DataFrame(rows, columns=["id", "order_id", "product_id", "quantity", "price"])
    out_path = os.path.join(data_dir, "edu_order_items.csv")
    df.to_csv(out_path, index=False)
    print(f"생성 완료: {len(df):,}행 → {out_path}")
    print(f"  order_id  유일: {df['order_id'].nunique():,}개 / {len(order_ids):,}개 주문")
    print(f"  product_id 유일: {df['product_id'].nunique():,}개")
    return df

if __name__ == "__main__":
    generate_order_items_data()
