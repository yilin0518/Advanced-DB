# fusion_router/analyzer.py
"""
分析SQL查询，决定路由到哪个数据库
"""
import re
from typing import Dict, Any


class QueryAnalyzer:
    def __init__(self):
        # 预编译的正则表达式模式
        self.patterns = {
            # 点查询（主键/外键查询）-> 优先MongoDB
            'point_query': re.compile(
                r"SELECT\s+\*\s+FROM\s+(\w+)\s+WHERE\s+(\w+_id)\s*=\s*['\"]?(\w+)['\"]?",
                re.IGNORECASE
            ),

            # 关联查询 -> MySQL
            'join_query': re.compile(
                r"\b(?:JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|OUTER JOIN)\b",
                re.IGNORECASE
            ),

            # 全文搜索 -> MongoDB
            'fulltext_query': re.compile(
                r"LIKE\s*['\"]?%.*%['\"]?",
                re.IGNORECASE
            ),

            # 复杂聚合 -> 根据数据量决定
            'complex_aggregation': re.compile(
                r"\b(?:GROUP BY|HAVING|ROLLUP|CUBE)\b",
                re.IGNORECASE
            ),

            # 简单查询 -> 根据表类型决定
            'simple_select': re.compile(
                r"SELECT\s+.*?\s+FROM\s+(\w+)",
                re.IGNORECASE
            )
        }

        # 表到数据库的映射
        self.table_mapping = {
            # 关系型表 -> MySQL
            'customers': 'mysql',
            'products': 'mysql',
            'sellers': 'mysql',
            'geolocation': 'mysql',
            'category_translation': 'mysql',

            # 文档型表 -> MongoDB
            'orders': 'mongo',
            'order_items': 'mongo',  # 实际会嵌入到orders
            'order_payments': 'mongo',
            'order_reviews': 'mongo'
        }

    def analyze(self, sql: str) -> Dict[str, Any]:
        """
        分析SQL查询，返回路由决策
        """
        sql = sql.strip()

        # 默认分析结果
        analysis = {
            'sql': sql,
            'db_type': 'unknown',
            'table': None,
            'query_type': None,
            'reason': '',
            'params': {}
        }

        # 1. 检查是否是点查询（主键查询）
        match = self.patterns['point_query'].search(sql)
        if match:
            table, column, value = match.groups()
            analysis.update({
                'table': table,
                'query_type': 'point_query',
                'params': {column: value}
            })

            # 点查询优先使用MongoDB（如果表在MongoDB中）
            if table in ['orders', 'order_items']:
                analysis.update({
                    'db_type': 'mongo',
                    'reason': f'Point query on {table}.{column}, MongoDB更擅长'
                })
            else:
                analysis.update({
                    'db_type': 'mysql',
                    'reason': f'Point query on {table}.{column}, MySQL已优化'
                })
            return analysis

        # 2. 检查是否是关联查询
        if self.patterns['join_query'].search(sql):
            # 找出涉及的所有表
            tables = re.findall(r'\bFROM\s+(\w+)|\bJOIN\s+(\w+)', sql, re.IGNORECASE)
            tables = [t for group in tables for t in group if t]

            # 检查是否涉及多个数据库
            db_types = set(self.table_mapping.get(t, 'unknown') for t in tables)

            if len(db_types) > 1:
                # 跨数据库查询
                analysis.update({
                    'query_type': 'cross_db_join',
                    'db_type': 'both',
                    'tables': tables,
                    'reason': f'Cross-database join involving {tables}'
                })
            else:
                # 单数据库关联查询
                db_type = db_types.pop() if db_types else 'mysql'
                analysis.update({
                    'query_type': 'join_query',
                    'db_type': db_type,
                    'reason': f'Join query, {db_type.upper()}擅长处理关联'
                })
            return analysis

        # 3. 检查是否是全文搜索
        if self.patterns['fulltext_query'].search(sql):
            analysis.update({
                'query_type': 'fulltext_search',
                'db_type': 'mongo',
                'reason': 'Full-text search, MongoDB文本索引更高效'
            })
            return analysis

        # 4. 检查表映射
        match = self.patterns['simple_select'].search(sql)
        if match:
            table = match.group(1)
            db_type = self.table_mapping.get(table, 'mysql')
            analysis.update({
                'table': table,
                'query_type': 'simple_select',
                'db_type': db_type,
                'reason': f'Table {table} mapped to {db_type}'
            })
            return analysis

        # 5. 默认到MySQL
        analysis.update({
            'query_type': 'unknown',
            'db_type': 'mysql',
            'reason': 'Default routing to MySQL'
        })

        return analysis