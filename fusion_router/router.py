# fusion_router/router.py
"""
核心路由逻辑，根据分析结果分发查询
"""
import json
import time
import re
from typing import Any, Dict, List
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from analyzer import QueryAnalyzer


class FusionQueryRouter:
    def __init__(self, mysql_uri: str, mongo_uri: str):
        """
        初始化数据库连接和查询分析器

        Args:
            mysql_uri: MySQL连接字符串，如 "mysql://user:pass@localhost:3306/olist_relations"
            mongo_uri: MongoDB连接字符串，如 "mongodb://user:pass@localhost:27017/olist_documents"
        """
        # 初始化数据库连接
        self.mysql_engine = create_engine(mysql_uri)
        self.mongo_client = MongoClient(mongo_uri)

        # 从连接字符串提取数据库名
        # MySQL: mysql://root:password@localhost:3306/olist_relations
        self.mysql_db_name = mysql_uri.split('/')[-1].split('?')[0]

        # MongoDB: mongodb://root:password@localhost:27017/olist_documents
        # 从MongoDB URI提取数据库名，如果没有指定，使用默认
        if 'mongodb://' in mongo_uri:
            # 移除协议部分
            path = mongo_uri.split('//')[1].split('@')[-1]
            if '/' in path:
                db_name = path.split('/')[-1].split('?')[0]
                if db_name:
                    self.mongo_db = self.mongo_client[db_name]
                else:
                    self.mongo_db = self.mongo_client[self.mysql_db_name]
            else:
                self.mongo_db = self.mongo_client[self.mysql_db_name]
        else:
            self.mongo_db = self.mongo_client.get_database()

        # 初始化查询分析器
        self.analyzer = QueryAnalyzer()

        # 查询统计
        self.stats = {
            'total_queries': 0,
            'mysql_queries': 0,
            'mongo_queries': 0,
            'cross_queries': 0,
            'avg_response_time': 0
        }

        print("✅ Fusion Query Router initialized")
        print(f"   - MySQL: {mysql_uri}")
        print(f"   - MongoDB: {mongo_uri}")
        print(f"   - MongoDB Database: {self.mongo_db.name}")

    def execute(self, sql: str) -> Dict[str, Any]:
        """
        执行SQL查询，自动路由到合适的数据库

        Args:
            sql: SQL查询语句

        Returns:
            Dict包含结果和元数据
        """
        self.stats['total_queries'] += 1

        # 1. 分析查询
        analysis = self.analyzer.analyze(sql)
        print(f"\n🔍 Query Analysis:")
        print(f"   SQL: {sql[:100]}...")
        print(f"   Type: {analysis['query_type']}")
        print(f"   DB: {analysis['db_type'].upper()}")
        print(f"   Reason: {analysis['reason']}")

        # 2. 根据分析结果路由
        start_time = time.time()

        if analysis['db_type'] == 'mysql':
            self.stats['mysql_queries'] += 1
            result = self._execute_mysql(sql)

        elif analysis['db_type'] == 'mongo':
            self.stats['mongo_queries'] += 1
            result = self._execute_mongo(sql, analysis)

        elif analysis['db_type'] == 'both':
            self.stats['cross_queries'] += 1
            result = self._execute_cross_db(sql, analysis)

        else:
            # 默认到MySQL
            result = self._execute_mysql(sql)

        # 3. 计算响应时间
        response_time = result.get('actual_time', time.time() - start_time)
        self.stats['avg_response_time'] = (
                                                  self.stats['avg_response_time'] * (
                                                      self.stats['total_queries'] - 1) + response_time
                                          ) / self.stats['total_queries']

        # 4. 构建返回结果
        response = {
            'success': True if 'error' not in result else False,
            'analysis': analysis,
            'stats': {
                'response_time': response_time,
                'row_count': result.get('row_count', 0),
                'db_type': analysis['db_type']
            }
        }

        # 添加数据或错误信息
        if 'error' in result:
            response['error'] = result['error']
        else:
            response['data'] = result.get('data', [])

        return response

    def _execute_mysql(self, sql: str) -> Dict[str, Any]:
        """执行MySQL查询"""
        try:
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text(sql))

                # 获取所有结果
                rows = result.fetchall()

                # 转换为字典列表
                columns = result.keys()
                data = [dict(zip(columns, row)) for row in rows]

                return {
                    'data': data,
                    'row_count': len(data),
                    'source': 'mysql'
                }
        except Exception as e:
            print(f"MySQL查询错误: {e}")
            return {
                'error': str(e),
                'source': 'mysql'
            }

    def _execute_mongo(self, sql: str, analysis: Dict) -> Dict[str, Any]:
        """
        将SQL转换为MongoDB查询并执行
        """
        try:
            # 解析SQL获取表名和条件
            table = analysis.get('table', '')

            # 如果没有指定表，尝试从SQL中提取
            if not table:
                # 尝试提取表名
                table_match = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
                if table_match:
                    table = table_match.group(1)
                else:
                    table = 'orders'  # 默认

            # 获取对应的MongoDB集合
            start_time = time.time()
            collection_name = table
            if table == 'order_items' or table == 'order_reviews':
                # 这些表的数据已经嵌入到orders中
                collection_name = 'orders'

            collection = self.mongo_db[collection_name]

            # 解析WHERE条件
            mongo_query = {}
            where_match = re.search(r'WHERE\s+(.+?)(?:\s+ORDER BY|\s+LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)

            if where_match:
                where_clause = where_match.group(1).strip()

                # 解析简单的等值条件
                # WHERE customer_id = 'xxx'
                eq_match = re.search(r'(\w+)\s*=\s*[\'"]?([^\'"]+)[\'"]?', where_clause)
                if eq_match:
                    field, value = eq_match.groups()
                    mongo_query[field] = value

                # WHERE key LIKE '%value%'
                like_match = re.search(r'(\w+)\s+LIKE\s+[\'"]?%([^%]+)%[\'"]?', where_clause, re.IGNORECASE)
                if like_match:
                    field, pattern = like_match.groups()
                    mongo_query[field] = {'$regex': pattern, '$options': 'i'}

            # 解析LIMIT
            limit = 1000  # 默认限制
            limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
            if limit_match:
                limit = int(limit_match.group(1))

            # 执行查询
            cursor = collection.find(mongo_query).limit(limit)

            # 转换为列表
            data = list(cursor)
            end_time = time.time()

            # 移除MongoDB的_id字段
            for doc in data:
                doc.pop('_id', None)

            # 如果查询的是嵌入字段（如items, reviews），需要提取
            if table in ['order_items', 'order_reviews']:
                extracted_data = []
                for order in data:
                    if table == 'order_items' and 'items' in order:
                        for item in order['items']:
                            item['order_id'] = order.get('order_id')
                            extracted_data.append(item)
                    elif table == 'order_reviews' and 'reviews' in order:
                        for review in order['reviews']:
                            review['order_id'] = order.get('order_id')
                            extracted_data.append(review)
                data = extracted_data

            return {
                'data': data,
                'row_count': len(data),
                'source': 'mongo',
                'actual_time': end_time - start_time
            }
        except Exception as e:
            print(f"MongoDB查询错误: {e}")
            return {
                'error': str(e),
                'source': 'mongo'
            }

    def _execute_cross_db(self, sql: str, analysis: Dict) -> Dict[str, Any]:
        """
        执行跨数据库查询
        """
        try:
            tables = analysis.get('tables', [])
            print(f"跨数据库查询涉及的表: {tables}")

            # 这里实现一个具体的跨数据库查询示例
            # 查找JOIN条件
            join_pattern = r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
            join_matches = re.findall(join_pattern, sql)

            if join_matches:
                # 简化处理：如果查询包含 customers 和 orders
                if 'customers' in tables and 'orders' in tables:
                    # 提取条件
                    for t1, c1, t2, c2 in join_matches:
                        if 'customer' in c1.lower() or 'customer' in c2.lower():
                            # 提取customer_id
                            customer_id_match = re.search(r'customers\.customer_id\s*=\s*[\'"]?(\w+)[\'"]?', sql)
                            if customer_id_match:
                                customer_id = customer_id_match.group(1)

                                # 1. 从MySQL获取用户信息
                                user_sql = f"SELECT * FROM customers WHERE customer_id = '{customer_id}'"
                                user_result = self._execute_mysql(user_sql)

                                # 2. 从MongoDB获取订单信息
                                orders_sql = f"SELECT * FROM orders WHERE customer_id = '{customer_id}'"
                                orders_result = self._execute_mongo(orders_sql, {'table': 'orders', 'params': {
                                    'customer_id': customer_id}})

                                # 3. 合并结果
                                combined_data = {
                                    'customer': user_result.get('data', [{}])[0] if user_result.get('data') else {},
                                    'orders': orders_result.get('data', [])
                                }

                                return {
                                    'data': combined_data,
                                    'row_count': len(orders_result.get('data', [])),
                                    'source': 'cross_db'
                                }

            # 默认回退到MySQL
            print("跨数据库查询无法处理，回退到MySQL")
            return self._execute_mysql(sql)

        except Exception as e:
            print(f"跨数据库查询错误: {e}")
            return {
                'error': f"Cross-database query failed: {str(e)}",
                'source': 'cross_db'
            }

    def get_stats(self) -> Dict[str, Any]:
        """获取路由统计信息"""
        total = self.stats['total_queries']
        return {
            **self.stats,
            'mysql_percentage': (
                self.stats['mysql_queries'] / total * 100 if total > 0 else 0
            ),
            'mongo_percentage': (
                self.stats['mongo_queries'] / total * 100 if total > 0 else 0
            ),
            'cross_percentage': (
                self.stats['cross_queries'] / total * 100 if total > 0 else 0
            )
        }