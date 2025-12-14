import sys
import os
from flask import Flask, request, jsonify
from sqlalchemy import text
import time

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 先检查是否需要初始化数据
from data_loader import DataLoader

# 配置
MYSQL_URI = "mysql+mysqlconnector://root:password@localhost:3307/olist_db?charset=utf8mb4"
MONGO_URI = "mongodb://root:password@localhost:27018"
DATASET_DIR = "../dataset"  # 容器内的数据集路径

print("=== 检查数据库状态 ===")
loader = DataLoader(MYSQL_URI, MONGO_URI, DATASET_DIR)
if not loader.check_data_exists():
    print("⚠️  数据库为空，自动加载数据...")
    loader.load_mysql_data()
    loader.load_mongo_data()
    loader.create_indexes()
    print("✅ 数据加载完成")
else:
    print("✅ 数据库已有数据")

from flask import Flask, request, jsonify
from router import FusionQueryRouter

app = Flask(__name__)

# 初始化路由器
router = FusionQueryRouter(MYSQL_URI, MONGO_URI)


@app.route('/api/query', methods=['POST'])
def execute_query():
    """
    执行SQL查询
    """
    data = request.get_json()

    if not data or 'sql' not in data:
        return jsonify({
            'success': False,
            'error': 'Missing SQL query'
        }), 400

    sql = data['sql']

    try:
        result = router.execute(sql)
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/analyze', methods=['POST'])
def analyze_query():
    """
    仅分析查询，不执行
    """
    data = request.get_json()

    if not data or 'sql' not in data:
        return jsonify({
            'success': False,
            'error': 'Missing SQL query'
        }), 400

    sql = data['sql']
    analysis = router.analyzer.analyze(sql)

    return jsonify({
        'success': True,
        'analysis': analysis
    })


@app.route('/api/stats', methods=['GET'])
def get_statistics():
    """
    获取路由统计信息
    """
    stats = router.get_stats()

    return jsonify({
        'success': True,
        'stats': stats,
        'timestamp': time.time()
    })


@app.route('/api/health', methods=['GET'])
def health_check():
    """
    健康检查
    """
    # 测试数据库连接
    mysql_ok = False
    mongo_ok = False

    try:
        with router.mysql_engine.connect() as conn:
            conn.execute(text("SELECT 1"))  # 现在text已导入
        mysql_ok = True
    except Exception as e:
        print(f"MySQL connection error: {e}")

    try:
        router.mongo_client.admin.command('ping')
        mongo_ok = True
    except Exception as e:
        print(f"MongoDB connection error: {e}")

    return jsonify({
        'status': 'healthy' if mysql_ok and mongo_ok else 'degraded',
        'mysql': 'connected' if mysql_ok else 'disconnected',
        'mongodb': 'connected' if mongo_ok else 'disconnected',
        'router': 'running'
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8002, debug=True)