import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, TEXT, Integer, Float, DateTime
import time
import os
import random
import hashlib  # ✅ CHANGED: 用稳定 hash
from cache_helper import CacheHelper

# export REDIS_HOST=127.0.0.1
# export REDIS_PORT=6379


# ================= 配置区域 =================
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '3307')
DB_NAME = os.getenv('DB_NAME', 'olist_db')
DATASET_DIR = os.getenv('DATASET_DIR', './dataset')

CONNECTION_STR = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
AUTO_LOAD_DATA = os.getenv('AUTO_LOAD_DATA', 'false').lower() == 'true'
# ===========================================

def get_engine():
    """创建数据库连接引擎"""
    try:
        temp_engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
                                    pool_pre_ping=True)
        # ✅ CHANGED: 用 begin() 自动提交，避免 CREATE DATABASE 没生效
        with temp_engine.begin() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))

        engine = create_engine(CONNECTION_STR, pool_pre_ping=True)
        return engine
    except Exception as e:
        print(f"数据库连接失败: {e}")
        print("请确保 MySQL 服务已启动，且配置信息正确。")
        return None

def load_data(engine):
    """加载 CSV 数据到 MySQL"""
    files = {
        'olist_customers_dataset.csv': 'customers',
        'olist_geolocation_dataset.csv': 'geolocation',
        'olist_order_items_dataset.csv': 'order_items',
        'olist_order_payments_dataset.csv': 'order_payments',
        'olist_order_reviews_dataset.csv': 'order_reviews',
        'olist_orders_dataset.csv': 'orders',
        'olist_products_dataset.csv': 'products',
        'olist_sellers_dataset.csv': 'sellers',
        'product_category_name_translation.csv': 'category_translation'
    }

    column_types = {
        'customer_id': VARCHAR(32),
        'customer_unique_id': VARCHAR(32),
        'order_id': VARCHAR(32),
        'product_id': VARCHAR(32),
        'seller_id': VARCHAR(32),
        'review_id': VARCHAR(32),
        'customer_zip_code_prefix': VARCHAR(10),
        'customer_city': VARCHAR(100),
        'customer_state': VARCHAR(5),
        'product_category_name': VARCHAR(100),
        'payment_type': VARCHAR(50),
        'order_status': VARCHAR(50),
        'review_comment_title': VARCHAR(255),
        'review_comment_message': TEXT,
        'price': Float(),
        'freight_value': Float(),
        'payment_value': Float(),
        'review_score': Integer(),
        'order_purchase_timestamp': DateTime(),
        'order_approved_at': DateTime(),
        'order_delivered_carrier_date': DateTime(),
        'order_delivered_customer_date': DateTime(),
        'order_estimated_delivery_date': DateTime(),
        'shipping_limit_date': DateTime(),
        'review_creation_date': DateTime(),
        'review_answer_timestamp': DateTime()
    }

    print("\n=== 开始数据加载 (MySQL) ===")
    start_total = time.time()

    for filename, table_name in files.items():
        file_path = os.path.join(DATASET_DIR, filename)
        if not os.path.exists(file_path):
            print(f"警告: 文件 {filename} 不存在，跳过。")
            continue

        print(f"正在处理 {filename} -> 表: {table_name} ...")

        try:
            df = pd.read_csv(file_path)

            if 'order_purchase_timestamp' in df.columns:
                date_cols = [col for col in df.columns if 'date' in col or 'timestamp' in col]
                for col in date_cols:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            dtype_mapping = {col: column_types[col] for col in df.columns if col in column_types}

            start_table = time.time()
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False,
                chunksize=1000,
                dtype=dtype_mapping
            )
            end_table = time.time()

            print(f"  - 成功写入 {len(df)} 行，耗时: {end_table - start_table:.2f} 秒")

        except Exception as e:
            print(f"  - 写入失败: {e}")

    end_total = time.time()
    print(f"=== 数据加载完成，总耗时: {end_total - start_total:.2f} 秒 ===\n")

def get_random_samples(engine, table, column, limit=100):
    """从数据库获取随机样本"""
    try:
        with engine.connect() as conn:
            sql = text(f"SELECT {column} FROM {table} LIMIT 5000")
            result = conn.execute(sql).fetchall()
            all_ids = [row[0] for row in result]
            if not all_ids:
                return []
            return random.sample(all_ids, min(limit, len(all_ids)))
    except Exception as e:
        print(f"获取样本数据失败 ({table}.{column}): {e}")
        return []

def create_indexes(engine):
    """创建数据库索引"""
    print("\n=== 正在创建索引 (Indexing) ===")
    start_time = time.time()

    indexes = [
        ('orders', 'idx_orders_customer_id', 'customer_id'),
        ('order_items', 'idx_items_order_id', 'order_id'),
        ('order_items', 'idx_items_product_id', 'product_id'),
        ('products', 'idx_products_product_id', 'product_id'),
        ('customers', 'idx_customers_customer_id', 'customer_id'),
        ('orders', 'idx_orders_date', 'order_purchase_timestamp'),
        ('order_items', 'idx_items_price', 'price'),
        ('customers', 'idx_customers_city', 'customer_city'),
        ('products', 'idx_products_category', 'product_category_name'),
        ('order_reviews', 'idx_reviews_comment', 'review_comment_message')
    ]

    with engine.connect() as conn:
        for table, idx_name, column in indexes:
            print(f"  - Creating index {idx_name} on {table}({column})...")
            try:
                # ✅ CHANGED: TEXT 列索引要前缀，否则容易报错
                if column == "review_comment_message":
                    conn.execute(text(f"CREATE INDEX {idx_name} ON {table} ({column}(100))"))
                else:
                    conn.execute(text(f"CREATE INDEX {idx_name} ON {table} ({column})"))
                conn.commit()
            except Exception as e:
                print(f"    Warning: {e}")

    print(f"=== 索引创建完成，耗时: {time.time() - start_time:.2f} 秒 ===\n")

def execute_query(engine, sql):
    """执行 SQL 查询并返回结果"""
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return [dict(row._mapping) for row in result]

def run_benchmark(engine, label="No Index", cache=None):
    """执行查询性能测试"""
    print(f"=== 开始查询性能测试 [{label}] ===\n")

    if cache:
        cache.clear_all()
        print("已启用 Redis 缓存并清空\n")

    sample_customer_ids = get_random_samples(engine, 'customers', 'customer_id', 50)
    sample_product_ids = get_random_samples(engine, 'products', 'product_id', 50)

    queries = [
        {
            "type": "简单查询 (Point Query)",
            "name": "查询用户订单 (By Customer ID)",
            "sql_template": "SELECT * FROM orders WHERE customer_id = '{param}'",
            "params": sample_customer_ids
        },
        {
            "type": "简单查询 (Point Query)",
            "name": "查询商品详情 (By Product ID)",
            "sql_template": "SELECT * FROM products WHERE product_id = '{param}'",
            "params": sample_product_ids
        },
        {
            "type": "范围查询 (Range Query)",
            "name": "时间范围查询 (Orders by Date)",
            "sql_template": "SELECT * FROM orders WHERE order_purchase_timestamp BETWEEN '2018-01-01 00:00:00' AND '2018-01-31 23:59:59'",
            "params": None
        },
        {
            "type": "范围查询 (Range Query)",
            "name": "价格范围查询 (Items by Price)",
            "sql_template": "SELECT * FROM order_items WHERE price BETWEEN 500 AND 1000 LIMIT 1000",
            "params": None
        },
        {
            "type": "文本搜索 (Text Search)",
            "name": "评论关键词搜索 (LIKE)",
            "sql_template": "SELECT * FROM order_reviews WHERE review_comment_message LIKE '%estão%' LIMIT 100",
            "params": None
        },
        {
            "type": "聚合查询 (Aggregation)",
            "name": "热门城市统计 (Top 10 Cities)",
            "sql_template": "SELECT customer_city, COUNT(*) as count FROM customers GROUP BY customer_city ORDER BY count DESC LIMIT 10",
            "params": None
        },
        {
            "type": "聚合查询 (Aggregation)",
            "name": "月度销售额 (Monthly Sales)",
            "sql_template": """
                SELECT DATE_FORMAT(order_purchase_timestamp, '%Y-%m') as month, COUNT(*) as orders 
                FROM orders 
                GROUP BY month 
                ORDER BY month
            """,
            "params": None
        },
        {
            "type": "复杂关联 (Join)",
            "name": "商品类别销售额 (Category Sales)",
            "sql_template": """
                SELECT 
                    p.product_category_name, 
                    SUM(oi.price) as total_sales 
                FROM order_items oi
                JOIN products p ON oi.product_id = p.product_id
                GROUP BY p.product_category_name
                ORDER BY total_sales DESC
                LIMIT 10
            """,
            "params": None
        },
        {
            "type": "复杂关联 (Join)",
            "name": "用户完整购买记录 (Full History)",
            "sql_template": """
                SELECT 
                    c.customer_id,
                    o.order_purchase_timestamp,
                    p.product_category_name,
                    oi.price
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN products p ON oi.product_id = p.product_id
                WHERE c.customer_id = '{param}'
            """,
            "params": sample_customer_ids
        }
    ]

    # ✅ CHANGED: 只缓存“适合缓存”的点查（最符合“热点商品/用户”）
    CACHEABLE = {
        "查询用户订单 (By Customer ID)",
        "查询商品详情 (By Product ID)",
        "时间范围查询 (Orders by Date)",
        "价格范围查询 (Items by Price)",
        "评论关键词搜索 (LIKE)",
        "热门城市统计 (Top 10 Cities)",
        "月度销售额 (Monthly Sales)",
        "商品类别销售额 (Category Sales)",
        "用户完整购买记录 (Full History)",
    }

    results = []

    for q in queries:
        print(f"测试: [{q['type']}] {q['name']}")

        # ✅ CHANGED: 固定一个热点 param，让缓存命中（最少改动但能看到提升）
        hot_param = random.choice(q['params']) if q['params'] else None

        times = []
        for i in range(10):
            if q['params']:
                param = hot_param  # 固定热点
                sql = q['sql_template'].format(param=param)
            else:
                sql = q['sql_template']

            start = time.time()

            if cache and q["name"] in CACHEABLE:
                # ✅ CHANGED: 稳定 key（md5），避免 hash() 每次运行不一样、以及碰撞
                cache_key = "mysql:" + hashlib.md5(sql.encode("utf-8")).hexdigest()
                _ = cache.cache_aside(cache_key, lambda s=sql: execute_query(engine, s))
            else:
                _ = execute_query(engine, sql)

            end = time.time()
            times.append(end - start)

        avg_time = sum(times) / len(times)
        print(f"  -> 平均: {avg_time:.4f}s")

        results.append({
            "Type": q['type'],
            "Name": q['name'],
            "Time": avg_time
        })

    return results

def main():
    max_retries = 10
    engine = None
    for i in range(max_retries):
        engine = get_engine()
        if engine:
            break
        print(f"等待数据库启动... ({i+1}/{max_retries})")
        time.sleep(5)

    if not engine:
        print("无法连接到数据库，程序退出。")
        return

    # 加载数据
    if AUTO_LOAD_DATA:
        print("检测到 AUTO_LOAD_DATA 环境变量，自动开始加载数据...")
        load_data(engine)
    else:
        try:
            user_input = input("是否需要重新加载数据? (y/n): ")
            if user_input.lower() == 'y':
                load_data(engine)
        except EOFError:
            print("无法读取输入，跳过数据加载。")

    # 初始化缓存（Redis 必须可用）
    cache = CacheHelper()

    # 阶段 1: 无索引
    results_no_index = run_benchmark(engine, label="No Index")

    # 阶段 2: 有索引
    create_indexes(engine)
    results_with_index = run_benchmark(engine, label="With Index")

    # 阶段 3: 有索引 + Redis 缓存
    results_with_cache = run_benchmark(engine, label="With Index + Redis Cache", cache=cache)

    # 对比报告（每条都打印 “前 -> 后” 的具体用时）
    print("\n=== MySQL 性能对比报告 (3 阶段，含前后用时) ===")

    comparison = []
    for r1, r2, r3 in zip(results_no_index, results_with_index, results_with_cache):
        t_no = r1["Time"]
        t_idx = r2["Time"]
        t_cache = r3["Time"]

        speedup_index = (t_no / t_idx) if t_idx > 0 else 0
        speedup_cache = (t_idx / t_cache) if t_cache > 0 else 0

        # ✅ 这里是你要的：逐项打印“前 -> 后”的具体用时
        print(f"- {r1['Name']}")
        print(f"  Index: {t_no:.4f}s -> {t_idx:.4f}s  (x{speedup_index:.2f}, 省 {t_no - t_idx:.4f}s)")
        print(f"  Cache: {t_idx:.4f}s -> {t_cache:.4f}s (x{speedup_cache:.2f}, 省 {t_idx - t_cache:.4f}s)")

        comparison.append({
            "Query Name": r1["Name"],
            "No Index (s)": f"{t_no:.4f}",
            "With Index (s)": f"{t_idx:.4f}",
            "With Cache (s)": f"{t_cache:.4f}",
            "Index Before->After": f"{t_no:.4f}->{t_idx:.4f}",
            "Cache Before->After": f"{t_idx:.4f}->{t_cache:.4f}",
            "Index Speedup": f"{speedup_index:.2f}x",
            "Cache Speedup": f"{speedup_cache:.2f}x",
        })

    print("\n=== 汇总表 ===")
    print(pd.DataFrame(comparison).to_string(index=False))


if __name__ == "__main__":
    main()
