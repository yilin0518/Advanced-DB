import pandas as pd
from pymongo import MongoClient, ASCENDING, TEXT
import time
import os
import random
from datetime import datetime

# ================= 配置区域 =================
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:password@localhost:27018/')
DB_NAME = os.getenv('DB_NAME', 'olist_db')
DATASET_DIR = os.getenv('DATASET_DIR', './dataset')
AUTO_LOAD_DATA = os.getenv('AUTO_LOAD_DATA', 'false').lower() == 'true'
# ===========================================

def get_db():
    """获取 MongoDB 数据库连接"""
    try:
        client = MongoClient(MONGO_URI)
        # 测试连接
        client.admin.command('ping')
        return client[DB_NAME]
    except Exception as e:
        print(f"MongoDB 连接失败: {e}")
        return None

def load_data(db):
    """加载 CSV 数据到 MongoDB (使用反规范化/嵌入式设计)"""
    print("\n=== 开始数据加载 (MongoDB) ===")
    start_total = time.time()

    # 1. 加载基础表 (Customers, Products)
    # ------------------------------------------------
    print("正在加载 Customers 和 Products...")
    
    # Customers
    df_cust = pd.read_csv(os.path.join(DATASET_DIR, 'olist_customers_dataset.csv'))
    db.customers.drop()
    db.customers.insert_many(df_cust.to_dict('records'))
    print(f"  - Customers: {len(df_cust)} docs")

    # Products
    df_prod = pd.read_csv(os.path.join(DATASET_DIR, 'olist_products_dataset.csv'))
    # 简单的列名清洗，MongoDB 对 key 中的点号敏感
    df_prod.columns = [c.replace('.', '_') for c in df_prod.columns]
    db.products.drop()
    db.products.insert_many(df_prod.to_dict('records'))
    print(f"  - Products: {len(df_prod)} docs")

    # 2. 构建核心 Orders 集合 (反规范化：嵌入 Items 和 Reviews)
    # ------------------------------------------------
    print("正在构建 Orders 聚合文档 (这可能需要一点时间)...")
    
    # 读取 Orders
    df_orders = pd.read_csv(os.path.join(DATASET_DIR, 'olist_orders_dataset.csv'))
    # 转换日期
    date_cols = [c for c in df_orders.columns if 'date' in c or 'timestamp' in c]
    for c in date_cols:
        df_orders[c] = pd.to_datetime(df_orders[c], errors='coerce')
    
    # 读取 Items
    df_items = pd.read_csv(os.path.join(DATASET_DIR, 'olist_order_items_dataset.csv'))
    
    # 读取 Reviews
    df_reviews = pd.read_csv(os.path.join(DATASET_DIR, 'olist_order_reviews_dataset.csv'))
    
    # 为了快速查找，将 Items 和 Reviews 按 order_id 分组
    print("  - Grouping items and reviews...")
    items_grp = df_items.groupby('order_id')
    reviews_grp = df_reviews.groupby('order_id')
    
    # 准备批量插入
    orders_buffer = []
    batch_size = 5000
    total_orders = 0
    
    db.orders.drop()
    
    start_build = time.time()
    
    # 遍历每个订单，构建嵌套文档
    # 结构: { ...order_info, items: [...], reviews: [...] }
    for order in df_orders.to_dict('records'):
        oid = order['order_id']
        
        # 嵌入 Items
        if oid in items_grp.groups:
            order['items'] = items_grp.get_group(oid).to_dict('records')
        else:
            order['items'] = []
            
        # 嵌入 Reviews
        if oid in reviews_grp.groups:
            order['reviews'] = reviews_grp.get_group(oid).to_dict('records')
        else:
            order['reviews'] = []
            
        orders_buffer.append(order)
        
        if len(orders_buffer) >= batch_size:
            db.orders.insert_many(orders_buffer)
            total_orders += len(orders_buffer)
            orders_buffer = []
            print(f"  - 已插入 {total_orders} 个聚合订单...")
            
    if orders_buffer:
        db.orders.insert_many(orders_buffer)
        total_orders += len(orders_buffer)
        
    print(f"  - Orders 聚合完成: {total_orders} docs, 耗时: {time.time() - start_build:.2f}s")
    print(f"=== 数据加载完成，总耗时: {time.time() - start_total:.2f} 秒 ===\n")

def create_indexes(db):
    """创建 MongoDB 索引"""
    print("\n=== 正在创建索引 (Indexing) ===")
    start_time = time.time()
    
    # 1. 简单索引
    print("  - Creating index on orders.customer_id...")
    db.orders.create_index([("customer_id", ASCENDING)])
    
    print("  - Creating index on products.product_id...")
    db.products.create_index([("product_id", ASCENDING)])
    
    # 2. 范围查询索引
    print("  - Creating index on orders.order_purchase_timestamp...")
    db.orders.create_index([("order_purchase_timestamp", ASCENDING)])
    
    # 3. 嵌套字段索引 (针对 items 数组中的 price)
    print("  - Creating index on orders.items.price...")
    db.orders.create_index([("items.price", ASCENDING)])
    
    # 4. 文本索引 (针对 reviews 数组中的 comment)
    print("  - Creating TEXT index on orders.reviews.review_comment_message...")
    db.orders.create_index([("reviews.review_comment_message", TEXT)])
    
    # 5. 聚合优化索引
    print("  - Creating index on customers.customer_city...")
    db.customers.create_index([("customer_city", ASCENDING)])
    
    print(f"=== 索引创建完成，耗时: {time.time() - start_time:.2f} 秒 ===\n")

def get_random_samples(db, collection, field, limit=50):
    """获取随机样本 ID"""
    pipeline = [{"$sample": {"size": limit}}, {"$project": {field: 1, "_id": 0}}]
    result = list(db[collection].aggregate(pipeline))
    return [doc[field] for doc in result if field in doc]

def run_benchmark(db, label="No Index"):
    """执行查询性能测试"""
    print(f"=== 开始查询性能测试 [{label}] ===\n")
    
    # 采样
    sample_customer_ids = get_random_samples(db, 'customers', 'customer_id')
    sample_product_ids = get_random_samples(db, 'products', 'product_id')
    
    queries = [
        # ------------------- 1. 简单查询 -------------------
        {
            "type": "简单查询 (Point Query)",
            "name": "查询用户订单 (By Customer ID)",
            "func": lambda: list(db.orders.find({"customer_id": random.choice(sample_customer_ids)}))
        },
        {
            "type": "简单查询 (Point Query)",
            "name": "查询商品详情 (By Product ID)",
            "func": lambda: db.products.find_one({"product_id": random.choice(sample_product_ids)})
        },

        # ------------------- 2. 范围查询 -------------------
        {
            "type": "范围查询 (Range Query)",
            "name": "时间范围查询 (Orders by Date)",
            "func": lambda: list(db.orders.find({
                "order_purchase_timestamp": {
                    "$gte": datetime(2018, 1, 1),
                    "$lte": datetime(2018, 1, 31, 23, 59, 59)
                }
            }))
        },
        {
            "type": "范围查询 (Range Query)",
            "name": "价格范围查询 (Items by Price)",
            "func": lambda: list(db.orders.find({
                "items.price": {"$gte": 500, "$lte": 1000}
            }).limit(1000))
        },

        # ------------------- 3. 文本搜索 (MongoDB 强项) -------------------
        {
            "type": "文本搜索 (Text Search)",
            "name": "评论关键词搜索 ($text)",
            "func": lambda: list(db.orders.find(
                {"$text": {"$search": "estão"}},
                {"score": {"$meta": "textScore"}}
            ).limit(100))
        },

        # ------------------- 4. 聚合查询 -------------------
        {
            "type": "聚合查询 (Aggregation)",
            "name": "热门城市统计 (Top 10 Cities)",
            "func": lambda: list(db.customers.aggregate([
                {"$group": {"_id": "$customer_city", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]))
        },
        
        # ------------------- 5. 复杂关联 (MongoDB 优势场景) -------------------
        # 在 MongoDB 中，"用户完整购买记录" 不需要 Join，因为 items 已经嵌入在 order 里了
        # 这就是 NoSQL 的核心优势：Read Local
        {
            "type": "复杂关联 (Join -> Embedded)",
            "name": "用户完整购买记录 (Full History)",
            "func": lambda: list(db.orders.find(
                {"customer_id": random.choice(sample_customer_ids)},
                {"items": 1, "order_purchase_timestamp": 1}
            ))
        }
    ]

    results = []
    for q in queries:
        print(f"测试: [{q['type']}] {q['name']}")
        times = []
        # 运行 10 次
        for _ in range(10):
            start = time.time()
            try:
                q['func']()
            except Exception as e:
                # 如果没有文本索引，Text Search 会报错，这里捕获一下
                if "text index required" in str(e):
                    pass 
            end = time.time()
            times.append(end - start)
        
        avg_time = sum(times) / len(times) if times else 0
        print(f"  -> 平均: {avg_time:.4f}s")
        results.append({
            "Type": q['type'],
            "Name": q['name'],
            "Time": avg_time
        })
        
    return results

def main():
    db = get_db()
    if db is None:
        return

    # 1. 加载数据
    if AUTO_LOAD_DATA:
        load_data(db)
    else:
        try:
            if input("是否重新加载数据? (y/n): ").lower() == 'y':
                load_data(db)
        except:
            pass

    # 2. 无索引测试
    # 注意：MongoDB 的 Text Search 必须要有索引才能运行，所以无索引阶段 Text Search 会跳过或报错
    print("注意: 无索引阶段 Text Search 将被跳过")
    results_no_index = run_benchmark(db, label="No Index")

    # 3. 创建索引
    create_indexes(db)

    # 4. 有索引测试
    results_with_index = run_benchmark(db, label="With Index")

    # 5. 对比报告
    print("\n=== MongoDB 性能对比报告 ===")
    comparison = []
    for r1, r2 in zip(results_no_index, results_with_index):
        speedup = r1['Time'] / r2['Time'] if r2['Time'] > 0 else 0
        comparison.append({
            "Query Name": r1['Name'],
            "No Index (s)": f"{r1['Time']:.4f}",
            "With Index (s)": f"{r2['Time']:.4f}",
            "Speedup (x)": f"{speedup:.2f}x"
        })
    
    print(pd.DataFrame(comparison).to_string(index=False))

if __name__ == "__main__":
    main()
