import pandas as pd
from pymongo import MongoClient, ASCENDING, TEXT
import time
import os
import random
from datetime import datetime
import hashlib  # ✅ 新增：稳定 cache key
from cache_helper import CacheHelper  # ✅ 新增：Redis 缓存

# ================= 配置区域 =================
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:password@localhost:27018/')
DB_NAME = os.getenv('DB_NAME', 'olist_db')
DATASET_DIR = os.getenv('DATASET_DIR', './dataset')
AUTO_LOAD_DATA = os.getenv('AUTO_LOAD_DATA', 'false').lower() == 'true'

MONGO_PORT = os.getenv('MONGO_PORT', '27017')
MONGO_URI = os.getenv('MONGO_URI', f'mongodb://{DB_HOST}:{MONGO_PORT}/')
# ============================================================


def get_db():
    """获取 MongoDB 数据库连接"""
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        return client[DB_NAME]
    except Exception as e:
        print(f"MongoDB 连接失败: {e}")
        return None


def load_data(db):
    """加载 CSV 数据到 MongoDB"""
    print("\n=== 开始数据加载 (MongoDB) ===")
    start_total = time.time()

    print("正在加载 Customers 和 Products...")

    df_cust = pd.read_csv(os.path.join(DATASET_DIR, 'olist_customers_dataset.csv'))
    db.customers.drop()
    db.customers.insert_many(df_cust.to_dict('records'))
    print(f"  - Customers: {len(df_cust)} docs")

    df_prod = pd.read_csv(os.path.join(DATASET_DIR, 'olist_products_dataset.csv'))
    df_prod.columns = [c.replace('.', '_') for c in df_prod.columns]
    db.products.drop()
    db.products.insert_many(df_prod.to_dict('records'))
    print(f"  - Products: {len(df_prod)} docs")

    print("正在构建 Orders 聚合文档...")

    df_orders = pd.read_csv(os.path.join(DATASET_DIR, 'olist_orders_dataset.csv'))
    date_cols = [c for c in df_orders.columns if 'date' in c or 'timestamp' in c]
    for c in date_cols:
        df_orders[c] = pd.to_datetime(df_orders[c], errors='coerce')

    df_items = pd.read_csv(os.path.join(DATASET_DIR, 'olist_order_items_dataset.csv'))
    df_reviews = pd.read_csv(os.path.join(DATASET_DIR, 'olist_order_reviews_dataset.csv'))

    items_grp = df_items.groupby('order_id')
    reviews_grp = df_reviews.groupby('order_id')

    orders_buffer = []
    batch_size = 5000
    total_orders = 0

    db.orders.drop()

    for order in df_orders.to_dict('records'):
        oid = order['order_id']

        if oid in items_grp.groups:
            order['items'] = items_grp.get_group(oid).to_dict('records')
        else:
            order['items'] = []

        if oid in reviews_grp.groups:
            order['reviews'] = reviews_grp.get_group(oid).to_dict('records')
        else:
            order['reviews'] = []

        orders_buffer.append(order)

        if len(orders_buffer) >= batch_size:
            db.orders.insert_many(orders_buffer)
            total_orders += len(orders_buffer)
            orders_buffer = []

    if orders_buffer:
        db.orders.insert_many(orders_buffer)
        total_orders += len(orders_buffer)

    print(f"  - Orders 聚合完成: {total_orders} docs")
    print(f"=== 数据加载完成，总耗时: {time.time() - start_total:.2f} 秒 ===\n")


def create_indexes(db):
    """创建 MongoDB 索引"""
    print("\n=== 正在创建索引 (Indexing) ===")
    start_time = time.time()

    db.orders.create_index([("customer_id", ASCENDING)])
    db.products.create_index([("product_id", ASCENDING)])
    db.orders.create_index([("order_purchase_timestamp", ASCENDING)])
    db.orders.create_index([("items.price", ASCENDING)])
    db.orders.create_index([("reviews.review_comment_message", TEXT)])
    db.customers.create_index([("customer_city", ASCENDING)])

    print(f"=== 索引创建完成，耗时: {time.time() - start_time:.2f} 秒 ===\n")


def get_random_samples(db, collection, field, limit=50):
    """获取随机样本 ID"""
    pipeline = [{"$sample": {"size": limit}}, {"$project": {field: 1, "_id": 0}}]
    result = list(db[collection].aggregate(pipeline))
    return [doc[field] for doc in result if field in doc]


def run_benchmark(db, label="No Index", cache=None):
    """执行查询性能测试"""
    print(f"=== 开始查询性能测试 [{label}] ===\n")

    # ✅ 新增：Redis 可用性检测 + 清空（不可用就自动降级）
    if cache is not None:
        try:
            cache.clear_all()
            print("已启用 Redis 缓存并清空\n")
        except Exception as e:
            print(f"警告: Redis 不可用，将禁用缓存。原因: {e}")
            cache = None

    sample_customer_ids = get_random_samples(db, 'customers', 'customer_id')
    sample_product_ids = get_random_samples(db, 'products', 'product_id')

    queries = [
        {
            "type": "简单查询 (Point Query)",
            "name": "查询用户订单 (By Customer ID)",
            "func": lambda cid: list(db.orders.find({"customer_id": cid})),
            "params": sample_customer_ids
        },
        {
            "type": "简单查询 (Point Query)",
            "name": "查询商品详情 (By Product ID)",
            "func": lambda pid: db.products.find_one({"product_id": pid}),
            "params": sample_product_ids
        },
        {
            "type": "范围查询 (Range Query)",
            "name": "时间范围查询 (Orders by Date)",
            "func": lambda: list(db.orders.find({
                "order_purchase_timestamp": {
                    "$gte": datetime(2018, 1, 1),
                    "$lte": datetime(2018, 1, 31, 23, 59, 59)
                }
            })),
            "params": None
        },
        {
            "type": "范围查询 (Range Query)",
            "name": "价格范围查询 (Items by Price)",
            "func": lambda: list(db.orders.find({
                "items.price": {"$gte": 500, "$lte": 1000}
            }).limit(1000)),
            "params": None
        },
        {
            "type": "文本搜索 (Text Search)",
            "name": "评论关键词搜索 ($text)",
            "func": lambda: list(db.orders.find(
                {"$text": {"$search": "estão"}},
                {"score": {"$meta": "textScore"}}
            ).limit(100)),
            "params": None
        },
        {
            "type": "聚合查询 (Aggregation)",
            "name": "热门城市统计 (Top 10 Cities)",
            "func": lambda: list(db.customers.aggregate([
                {"$group": {"_id": "$customer_city", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ])),
            "params": None
        },
        {
            "type": "复杂关联 (Join -> Embedded)",
            "name": "用户完整购买记录 (Full History)",
            "func": lambda cid: list(db.orders.find(
                {"customer_id": cid},
                {"items": 1, "order_purchase_timestamp": 1}
            )),
            "params": sample_customer_ids
        }
    ]

    # ✅ 新增：只缓存点查（与 MySQL 对齐）
    CACHEABLE = {
        "查询用户订单 (By Customer ID)",
        "查询商品详情 (By Product ID)"
    }

    results = []
    for q in queries:
        print(f"测试: [{q['type']}] {q['name']}")
        times = []

        for _ in range(10):
            param = random.choice(q['params']) if q['params'] else None

            start = time.time()
            try:
                if cache is not None and q["name"] in CACHEABLE:
                    raw = f"{q['name']}|{param if param else 'static'}"
                    cache_key = "mongo:" + hashlib.md5(raw.encode("utf-8")).hexdigest()

                    if param is not None:
                        _ = cache.cache_aside(cache_key, lambda p=param: q['func'](p))
                    else:
                        _ = cache.cache_aside(cache_key, q['func'])
                else:
                    if param is not None:
                        _ = q['func'](param)
                    else:
                        _ = q['func']()

            except Exception as e:
                # 保持你原来的容错逻辑：text index 没有就不测这一轮
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

    # 加载数据
    if AUTO_LOAD_DATA:
        load_data(db)
    else:
        try:
            if input("是否重新加载数据? (y/n): ").lower() == 'y':
                load_data(db)
        except:
            pass

    # ✅ 新增：初始化 Redis 缓存（不可用则降级）
    cache = None
    try:
        cache = CacheHelper()
    except Exception as e:
        print(f"警告: Redis 连接失败（将禁用缓存阶段）：{e}")

    # 阶段 1: 无索引
    results_no_index = run_benchmark(db, label="No Index")

    # 阶段 2: 有索引
    create_indexes(db)
    results_with_index = run_benchmark(db, label="With Index")

    # 阶段 3: 有索引 + Redis 缓存
    results_with_cache = run_benchmark(db, label="With Index + Redis Cache", cache=cache)

    # 对比报告
    print("\n=== MongoDB 性能对比报告 (3 阶段) ===")
    comparison = []
    for r1, r2, r3 in zip(results_no_index, results_with_index, results_with_cache):
        speedup_index = r1['Time'] / r2['Time'] if r2['Time'] > 0 else 0
        speedup_cache = r2['Time'] / r3['Time'] if r3['Time'] > 0 else 0
        comparison.append({
            "Query Name": r1['Name'],
            "No Index (s)": f"{r1['Time']:.4f}",
            "With Index (s)": f"{r2['Time']:.4f}",
            "With Cache (s)": f"{r3['Time']:.4f}",
            "Index Speedup": f"{speedup_index:.2f}x",
            "Cache Speedup": f"{speedup_cache:.2f}x"
        })

    print(pd.DataFrame(comparison).to_string(index=False))


if __name__ == "__main__":
    main()
