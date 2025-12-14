import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, TEXT, Integer, Float, DateTime
from pymongo import MongoClient
import time
import os


class DataLoader:
    def __init__(self, mysql_uri, mongo_uri, dataset_dir="./dataset"):
        print(mysql_uri)
        self.mysql_engine = create_engine(mysql_uri)
        self.mongo_client = MongoClient(mongo_uri)
        self.dataset_dir = dataset_dir

        db_name = mysql_uri.split('/')[-1].split('?')[0]
        self.mongo_db = self.mongo_client[db_name]

    def clean_datetime_for_mongo(self, value):
        """清理日期时间值，将 NaT 转换为 None"""
        if pd.isna(value):
            return None
        return value

    def load_mysql_data(self):
        """加载数据到MySQL"""
        print("\n=== 加载数据到 MySQL ===")

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
            file_path = os.path.join(self.dataset_dir, filename)
            if not os.path.exists(file_path):
                print(f"警告: 文件 {filename} 不存在，跳过。")
                continue

            print(f"正在处理 {filename} -> 表: {table_name} ...")
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
                con=self.mysql_engine, 
                if_exists='replace', 
                index=False, 
                chunksize=1000,
                dtype=dtype_mapping  # 关键修改：传入类型映射
            )
            end_table = time.time()
            
            print(f"  - 成功写入 {len(df)} 行，耗时: {end_table - start_table:.2f} 秒")
                

        end_total = time.time()
        print(f"=== 数据加载完成，总耗时: {end_total - start_total:.2f} 秒 ===\n")

    def load_mongo_data(self):
        """加载数据到MongoDB（使用反规范化设计）"""
        print("\n=== 加载数据到 MongoDB ===")

        try:
            # 读取CSV文件
            df_orders = pd.read_csv(os.path.join(self.dataset_dir, 'olist_orders_dataset.csv'))
            df_items = pd.read_csv(os.path.join(self.dataset_dir, 'olist_order_items_dataset.csv'))
            df_reviews = pd.read_csv(os.path.join(self.dataset_dir, 'olist_order_reviews_dataset.csv'))

            # 转换日期并清理 NaT 值
            date_cols = [c for c in df_orders.columns if 'date' in c or 'timestamp' in c]
            for c in date_cols:
                df_orders[c] = pd.to_datetime(df_orders[c], errors='coerce')

            # 处理其他数据集的日期列
            if 'shipping_limit_date' in df_items.columns:
                df_items['shipping_limit_date'] = pd.to_datetime(df_items['shipping_limit_date'], errors='coerce')

            if 'review_creation_date' in df_reviews.columns:
                df_reviews['review_creation_date'] = pd.to_datetime(df_reviews['review_creation_date'], errors='coerce')
            if 'review_answer_timestamp' in df_reviews.columns:
                df_reviews['review_answer_timestamp'] = pd.to_datetime(df_reviews['review_answer_timestamp'],
                                                                       errors='coerce')

            # 分组数据
            print("正在聚合订单数据...")
            items_grp = df_items.groupby('order_id')
            reviews_grp = df_reviews.groupby('order_id')

            # 构建嵌套文档
            orders_buffer = []
            batch_size = 5000
            total_orders = 0

            start_time = time.time()

            for order in df_orders.to_dict('records'):
                oid = order['order_id']

                # 清理订单中的日期字段
                for key in list(order.keys()):
                    if isinstance(order[key], pd.Timestamp) and pd.isna(order[key]):
                        order[key] = None
                    elif pd.isna(order[key]):
                        order[key] = None

                # 嵌入Items
                if oid in items_grp.groups:
                    items_list = []
                    items_group = items_grp.get_group(oid)
                    for item in items_group.to_dict('records'):
                        # 清理items中的日期字段
                        if 'shipping_limit_date' in item and pd.isna(item['shipping_limit_date']):
                            item['shipping_limit_date'] = None
                        items_list.append(item)
                    order['items'] = items_list
                else:
                    order['items'] = []

                # 嵌入Reviews
                if oid in reviews_grp.groups:
                    reviews_list = []
                    reviews_group = reviews_grp.get_group(oid)
                    for review in reviews_group.to_dict('records'):
                        # 清理reviews中的日期字段
                        for date_key in ['review_creation_date', 'review_answer_timestamp']:
                            if date_key in review and pd.isna(review[date_key]):
                                review[date_key] = None
                        reviews_list.append(review)
                    order['reviews'] = reviews_list
                else:
                    order['reviews'] = []

                orders_buffer.append(order)

                if len(orders_buffer) >= batch_size:
                    self.mongo_db.orders.insert_many(orders_buffer)
                    total_orders += len(orders_buffer)
                    orders_buffer = []
                    print(f"  - 已插入 {total_orders} 个聚合订单...")

            if orders_buffer:
                self.mongo_db.orders.insert_many(orders_buffer)
                total_orders += len(orders_buffer)

            print(f"✅ MongoDB 数据加载完成: {total_orders} 个订单，耗时: {time.time() - start_time:.2f}s")

        except Exception as e:
            print(f"❌ MongoDB 数据加载失败: {e}")
            import traceback
            traceback.print_exc()

    def create_indexes(self):
        """创建数据库索引"""
        print("\n=== 创建数据库索引 ===")

        # MySQL索引
        mysql_indexes = [
            ('orders', 'customer_id'),
            ('order_items', 'order_id'),
            ('order_items', 'product_id'),
            ('products', 'product_id'),
            ('customers', 'customer_id'),
            ('orders', 'order_purchase_timestamp'),
            ('order_items', 'price'),
        ]

        with self.mysql_engine.connect() as conn:
            for table, column in mysql_indexes:
                try:
                    conn.execute(text(f"CREATE INDEX idx_{table}_{column} ON {table} ({column})"))
                    print(f"  - MySQL: 创建索引 idx_{table}_{column}")
                except Exception as e:
                    print(f"  - MySQL索引创建失败: {e}")

        # MongoDB索引
        mongo_indexes = [
            ("orders", "customer_id"),
            ("orders", "order_purchase_timestamp"),
            ("orders", "items.price"),
        ]

        for collection, field in mongo_indexes:
            try:
                self.mongo_db[collection].create_index([(field, 1)])
                print(f"  - MongoDB: 创建索引 {collection}.{field}")
            except Exception as e:
                print(f"  - MongoDB索引创建失败: {e}")

        print("✅ 索引创建完成")

    def check_data_exists(self):
        """检查数据是否已存在"""
        try:
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM customers"))
                mysql_count = result.scalar()

            mongo_count = self.mongo_db.orders.count_documents({})

            return mysql_count > 0 and mongo_count > 0
        except:
            return False


if __name__ == "__main__":
    loader = DataLoader(
        mysql_uri="mysql+mysqlconnector://root:password@localhost:3306/olist_relations",
        mongo_uri="mongodb://root:password@localhost:27017/olist_documents",
        dataset_dir="../dataset"
    )

    if not loader.check_data_exists():
        print("数据库为空，开始加载数据...")
        loader.load_mysql_data()
        loader.load_mongo_data()
        loader.create_indexes()
    else:
        print("数据库已有数据，跳过加载")