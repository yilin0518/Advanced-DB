import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, TEXT, Integer, Float, DateTime
import time
import os
import random

# ================= 配置区域 =================
# 优先从环境变量获取配置，方便 Docker 运行
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'olist_db')

# 数据集路径
DATASET_DIR = os.getenv('DATASET_DIR', './dataset')

# 数据库连接字符串
CONNECTION_STR = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 是否自动加载数据 (用于 Docker 非交互模式)
AUTO_LOAD_DATA = os.getenv('AUTO_LOAD_DATA', 'false').lower() == 'true'

# ===========================================

def get_engine():
    """创建数据库连接引擎"""
    try:
        # 先连接到不指定 DB 的 server，创建数据库
        temp_engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}")
        with temp_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
        
        # 返回指定 DB 的 engine
        engine = create_engine(CONNECTION_STR)
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

    # 定义列类型映射，解决索引长度问题
    # 这里的长度是根据数据集特征估算的
    column_types = {
        # ID 类 (UUID 通常是 32 位)
        'customer_id': VARCHAR(32),
        'customer_unique_id': VARCHAR(32),
        'order_id': VARCHAR(32),
        'product_id': VARCHAR(32),
        'seller_id': VARCHAR(32),
        'review_id': VARCHAR(32),
        
        # 短文本类
        'customer_zip_code_prefix': VARCHAR(10),
        'customer_city': VARCHAR(100),
        'customer_state': VARCHAR(5),
        'product_category_name': VARCHAR(100),
        'payment_type': VARCHAR(50),
        'order_status': VARCHAR(50),
        
        # 长文本类 (保留 TEXT)
        'review_comment_title': VARCHAR(255),
        'review_comment_message': TEXT,
        
        # 数值类 (Pandas 通常能自动识别，但显式指定更安全)
        'price': Float(),
        'freight_value': Float(),
        'payment_value': Float(),
        'review_score': Integer(),
        
        # 时间类
        'order_purchase_timestamp': DateTime(),
        'order_approved_at': DateTime(),
        'order_delivered_carrier_date': DateTime(),
        'order_delivered_customer_date': DateTime(),
        'order_estimated_delivery_date': DateTime(),
        'shipping_limit_date': DateTime(),
        'review_creation_date': DateTime(),
        'review_answer_timestamp': DateTime()
    }

    print("\n=== 开始数据加载 ===")
    start_total = time.time()

    for filename, table_name in files.items():
        file_path = os.path.join(DATASET_DIR, filename)
        if not os.path.exists(file_path):
            print(f"警告: 文件 {filename} 不存在，跳过。")
            continue

        print(f"正在处理 {filename} -> 表: {table_name} ...")
        
        try:
            # 读取 CSV
            df = pd.read_csv(file_path)
            
            # 简单的预处理：转换日期列
            if 'order_purchase_timestamp' in df.columns:
                date_cols = [col for col in df.columns if 'date' in col or 'timestamp' in col]
                for col in date_cols:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # 筛选出当前 DataFrame 中存在的列的类型映射
            dtype_mapping = {col: column_types[col] for col in df.columns if col in column_types}

            # 写入数据库
            start_table = time.time()
            df.to_sql(
                name=table_name, 
                con=engine, 
                if_exists='replace', 
                index=False, 
                chunksize=1000,
                dtype=dtype_mapping  # 关键修改：传入类型映射
            )
            end_table = time.time()
            
            print(f"  - 成功写入 {len(df)} 行，耗时: {end_table - start_table:.2f} 秒")
            
        except Exception as e:
            print(f"  - 写入失败: {e}")

    end_total = time.time()
    print(f"=== 数据加载完成，总耗时: {end_total - start_total:.2f} 秒 ===\n")

def get_random_samples(engine, table, column, limit=100):
    """从数据库获取一批 ID 用于随机查询测试"""
    try:
        with engine.connect() as conn:
            # 获取前 5000 条数据，然后从中随机采样，避免 ORDER BY RAND() 的全表扫描性能问题
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
    """创建数据库索引以优化查询性能"""
    print("\n=== 正在创建索引 (Indexing) ===")
    start_time = time.time()
    
    # 定义需要创建的索引
    # 格式: (表名, 索引名, 列名)
    indexes = [
        # 外键和主键查找优化
        ('orders', 'idx_orders_customer_id', 'customer_id'),
        ('order_items', 'idx_items_order_id', 'order_id'),
        ('order_items', 'idx_items_product_id', 'product_id'),
        ('products', 'idx_products_product_id', 'product_id'),
        ('customers', 'idx_customers_customer_id', 'customer_id'),
        
        # 范围查询和排序优化
        ('orders', 'idx_orders_date', 'order_purchase_timestamp'),
        ('order_items', 'idx_items_price', 'price'),
        
        # 聚合查询优化
        ('customers', 'idx_customers_city', 'customer_city'),
        ('products', 'idx_products_category', 'product_category_name'),
        
        # 文本搜索优化 (注意：普通 B-Tree 索引对 LIKE '%...%' 无效，这里仅作演示对比)
        ('order_reviews', 'idx_reviews_comment', 'review_comment_message')
    ]

    with engine.connect() as conn:
        for table, idx_name, column in indexes:
            print(f"  - Creating index {idx_name} on {table}({column})...")
            try:
                # MySQL 创建索引语法
                conn.execute(text(f"CREATE INDEX {idx_name} ON {table} ({column})"))
            except Exception as e:
                print(f"    Warning: {e}")
    
    print(f"=== 索引创建完成，耗时: {time.time() - start_time:.2f} 秒 ===\n")

def run_benchmark(engine, label="No Index"):
    """执行增强版查询性能测试"""
    print(f"=== 开始查询性能测试 [{label}] ===\n")
    
    # 预先获取随机参数
    # 注意：为了公平对比，有无索引应该使用相同的随机种子或重新采样
    # 这里我们重新采样，模拟真实负载
    sample_customer_ids = get_random_samples(engine, 'customers', 'customer_id', 50)
    sample_product_ids = get_random_samples(engine, 'products', 'product_id', 50)
    
    # 定义测试查询集
    queries = [
        # ------------------- 1. 简单查询 (Point Queries) -------------------
        {
            "type": "简单查询 (Point Query)",
            "name": "查询用户订单 (By Customer ID)",
            "desc": "根据随机 Customer ID 查询其所有订单",
            "sql_template": "SELECT * FROM orders WHERE customer_id = '{param}'",
            "params": sample_customer_ids
        },
        {
            "type": "简单查询 (Point Query)",
            "name": "查询商品详情 (By Product ID)",
            "desc": "根据随机 Product ID 查询商品属性",
            "sql_template": "SELECT * FROM products WHERE product_id = '{param}'",
            "params": sample_product_ids
        },

        # ------------------- 2. 范围查询 (Range Queries) -------------------
        {
            "type": "范围查询 (Range Query)",
            "name": "时间范围查询 (Orders by Date)",
            "desc": "查询 2018 年 1 月份的所有订单",
            "sql_template": "SELECT * FROM orders WHERE order_purchase_timestamp BETWEEN '2018-01-01 00:00:00' AND '2018-01-31 23:59:59'",
            "params": None
        },
        {
            "type": "范围查询 (Range Query)",
            "name": "价格范围查询 (Items by Price)",
            "desc": "查询价格在 500 到 1000 之间的订单项",
            "sql_template": "SELECT * FROM order_items WHERE price BETWEEN 500 AND 1000 LIMIT 1000",
            "params": None
        },

        # ------------------- 3. 文本搜索 (Text Search) -------------------
        {
            "type": "文本搜索 (Text Search)",
            "name": "评论关键词搜索 (LIKE)",
            "desc": "查找包含 'lannister' (低频词) 的评论",
            "sql_template": "SELECT * FROM order_reviews WHERE review_comment_message LIKE '%estão%' LIMIT 100",
            "params": None
        },

        # ------------------- 4. 聚合查询 (Aggregations) -------------------
        {
            "type": "聚合查询 (Aggregation)",
            "name": "热门城市统计 (Top 10 Cities)",
            "desc": "统计各城市的客户数量 Top 10",
            "sql_template": "SELECT customer_city, COUNT(*) as count FROM customers GROUP BY customer_city ORDER BY count DESC LIMIT 10",
            "params": None
        },
        {
            "type": "聚合查询 (Aggregation)",
            "name": "月度销售额 (Monthly Sales)",
            "desc": "按月份统计总销售额 (Time Series Aggregation)",
            "sql_template": """
                SELECT DATE_FORMAT(order_purchase_timestamp, '%Y-%m') as month, COUNT(*) as orders 
                FROM orders 
                GROUP BY month 
                ORDER BY month
            """,
            "params": None
        },

        # ------------------- 5. 复杂关联 (Joins) -------------------
        {
            "type": "复杂关联 (Join)",
            "name": "商品类别销售额 (Category Sales)",
            "desc": "关联 3 表：统计各商品类别的总销售额",
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
            "desc": "关联 4 表：查询某用户购买的所有商品名称和价格",
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

    # 执行测试循环
    results = []
    
    for q in queries:
        print(f"测试: [{q['type']}] {q['name']}")
        
        times = []
        # 执行 10 次取平均值
        for i in range(10):
            # 准备 SQL
            if q['params']:
                param = random.choice(q['params'])
                sql = q['sql_template'].format(param=param)
            else:
                sql = q['sql_template']

            start = time.time()
            with engine.connect() as conn:
                result = conn.execute(text(sql))
                _ = result.fetchall()
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
    # 1. 获取数据库连接
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

    # 2. 加载数据
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
    
    # 3. 运行基准测试 (无索引)
    results_no_index = run_benchmark(engine, label="No Index")
    
    # 4. 创建索引
    create_indexes(engine)
    
    # 5. 运行基准测试 (有索引)
    results_with_index = run_benchmark(engine, label="With Index")
    
    # 6. 生成对比报告
    print("\n=== 最终性能对比报告 (Performance Comparison) ===")
    comparison = []
    for r1, r2 in zip(results_no_index, results_with_index):
        speedup = r1['Time'] / r2['Time'] if r2['Time'] > 0 else 0
        comparison.append({
            "Query Type": r1['Type'],
            "Query Name": r1['Name'],
            "No Index (s)": f"{r1['Time']:.4f}",
            "With Index (s)": f"{r2['Time']:.4f}",
            "Speedup (x)": f"{speedup:.2f}x"
        })
    
    df_compare = pd.DataFrame(comparison)
    print(df_compare.to_string(index=False))

if __name__ == "__main__":
    main()
