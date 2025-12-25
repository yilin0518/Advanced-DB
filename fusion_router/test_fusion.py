# test_fusion.py
import requests
import json
import time
import pandas as pd

BASE_URL = "http://localhost:8002/api"


def test_queries():
    """测试各种查询的路由"""

    test_cases = [
        # 1. 点查询 -> 应该路由到MongoDB
        {
            "name": "点查询订单",
            "sql": "SELECT * FROM orders WHERE order_id = '00010242fe8c5a6d1ba2dd792cb16214'",
            "expected_db": "mongo"
        },

        # 2. 点查询用户 -> 应该路由到MySQL
        {
            "name": "点查询用户",
            "sql": "SELECT * FROM customers WHERE customer_id = '4e7b3e00288586ebd08712fdd0374a03'",
            "expected_db": "mysql"
        },

        # 3. 关联查询 -> 应该路由到MySQL
        {
            "name": "关联查询订单详情",
            "sql": """
                   SELECT o.order_id, c.customer_city, p.product_category_name
                   FROM orders o
                            JOIN customers c ON o.customer_id = c.customer_id
                            JOIN order_items oi ON o.order_id = oi.order_id
                            JOIN products p ON oi.product_id = p.product_id
                   WHERE o.order_id = '00010242fe8c5a6d1ba2dd792cb16214'
                   """,
            "expected_db": "mysql"
        },

        # 4. 全文搜索 -> 应该路由到MongoDB
        {
            "name": "搜索评论",
            "sql": "SELECT * FROM order_reviews WHERE review_comment_message LIKE 'marca'",
            "expected_db": "mongo"
        },

        # 5. 跨数据库查询示例
        {
            "name": "用户订单历史",
            "sql": """
                   SELECT c.*, o.*
                   FROM customers c
                            LEFT JOIN orders o ON c.customer_id = o.customer_id
                   WHERE c.customer_id = '4e7b3e00288586ebd08712fdd0374a03'
                   """,
            "expected_db": "both"
        }
    ]

    results = []

    for test in test_cases:
        print(f"\n🧪 Testing: {test['name']}")
        print(f"   SQL: {test['sql'][:80]}...")

        # 发送请求
        response = requests.post(
            f"{BASE_URL}/query",
            json={"sql": test['sql']}
        )

        if response.status_code == 200:
            result = response.json()
            actual_db = result['analysis']['db_type']

            # 检查路由是否正确
            passed = (actual_db == test['expected_db'])

            print(f"   Expected: {test['expected_db']}")
            print(f"   Actual: {actual_db}")
            print(f"   ✓ PASS" if passed else f"   ✗ FAIL")
            print(f"   Time: {result['stats']['response_time']:.3f}s")
            print(f"   Reason: {result['analysis']['reason']}")

            results.append({
                "test": test['name'],
                "passed": passed,
                "expected": test['expected_db'],
                "actual": actual_db,
                "time": result['stats']['response_time']
            })
        else:
            print(f"   ✗ Request failed: {response.status_code}")
            results.append({
                "test": test['name'],
                "passed": False,
                "error": f"HTTP {response.status_code}"
            })

        time.sleep(0.5)  # 避免请求过快

    # 打印汇总
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed_count = sum(1 for r in results if r['passed'])
    total_count = len(results)

    print(f"Total Tests: {total_count}")
    print(f"Passed: {passed_count}")
    print(f"Failed: {total_count - passed_count}")
    print(f"Success Rate: {passed_count / total_count * 100:.1f}%")

    # 获取统计信息
    stats_response = requests.get(f"{BASE_URL}/stats")
    if stats_response.status_code == 200:
        stats = stats_response.json()['stats']
        print(f"\n📈 ROUTER STATISTICS")
        print(f"Total Queries: {stats['total_queries']}")
        print(f"MySQL Queries: {stats['mysql_queries']} ({stats.get('mysql_percentage', 0):.1f}%)")
        print(f"MongoDB Queries: {stats['mongo_queries']} ({stats.get('mongo_percentage', 0):.1f}%)")
        print(f"Cross-database Queries: {stats['cross_queries']}")
        print(f"Avg Response Time: {stats['avg_response_time']:.3f}s")


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


def run_hybrid_routing_benchmark(runs=20, label="Hybrid Routing"):
    """混合式数据库路由测试：按类别跑多次取平均耗时，并评估路由正确率"""
    print(f"=== 开始混合式数据库路由测试 [{label}] ===\n")

    sample_customer_ids = [
        '053577e4537e6b6b68b9f0e929f27d32', '00c042af846ab3125854b4abc3bf25a6', 
        '03dffa43f4eb19d5bb46183e6be9c03f', '0570eff03cfce2f794d818d3f6bec651', 
        '09ab2c2de0ecac3c7cca42e6a9dea0c1', '03de10b74e57b7aa7c13361497c5c51a', 
        '0950fe2a7eda69cb626c908ef124902d', '08185246997b160bfdc98c4804abae71', 
        '0c9fdc5b4e2b1d8d52de981dd05d7222', '043563b734e56fc7efa52c47e58d339f', 
        '068e1b93e18127f5d03e95308211c438', '0bd51eff36516c236f8eebce83579c43', 
        '080ea580ebf890d1b95afdb8d7a5297e', '06497bec842a481bd823944e52528944', 
        '042bcdea41b22aa79516d40e4f374ed8', '07721eb08dd1b2ad67e03e8aeeb1a83b', 
        '078285da1785eeb5bb83d3e4d1096fc2', '0445283ab2d69419d518a3b1d82092c7', 
        '00c36dc4ec485e2500e9a669d6ea63d6', '01de8fdab9ccb256665253dab73c1509', 
        '09b38464eb15dc38f7388212f156d894', '0abfaf23dee18e9e25cac561ed01e909', 
        '01fb955db98cde5399bd9d21e88caa9b', '0403515b897169ea1b001e4ea602bf84', 
        '02afc864fbc233378368b0c0980f576c', '0cf44a827f1bda40bb968f6c0393aec8', 
        '014693f265c4f52b8c1aa9eb8cd197d5', '023cf83ac35d703a6f3c1b31bcf844d5', 
        '093c43383e2807557c81ff090b107c27', '0a99769a832fecc64920faf892ed1be5', 
        '06343064a4554dfdfff580095169869b', '08f39bb318284f8eca5a32c9e3a15273', 
        '028e29b651eae6076b5b70613201c493', '0095d34ddb16f0d0776fc9f2a341dfcd', 
        '080f252f36da3da6eb5b04407af199f7', '081f93ad5ce5604b882889f5bb44359f', 
        '01c77b0f81a1e64cc8de4f736226f7a1', '001a57041f56400917a187dd74e6cbc1', 
        '0aaf8035b64dd152699c97ca990a7e78', '0092770b2a1471643d88b60d6b804464', 
        '033fddf04734fc23644c3d6298562372', '07290721bf998f2a8230469bc3862a5f', 
        '04cf6b7a84aeea29e7146b2f5ca5ebf9', '01122215dd21ac872ae567ec4e351e01', 
        '0c73d3ffa5eda2d98e6f4474a8e610b0', '00e0a2be19aca747d3051ed15e18b77a', 
        '02e5c40ec97aefc28340fa9702651e21', '0a60413541d00d686cf4a7cd994cc1f2', 
        '01980baf1c23e7437caf43ce20455d0d', '01c843a2c0600def0b7693dba47af460'
    ]
    sample_product_ids = [
        '2001ae82f374f0f76db97ad0b37231da', '12c567b916f6f3bb02b2557456a30212', 
        '137d0bf5250717a478bd7f9c74410bda', '0661920a7a5f19746f501d1190888270', 
        '20d6d5469584eb71125803455ccf914a', '002959d7a0b0990fe2d69988affcbc80', 
        '08418a3bc628e92c012281010957e259', '0cf2faf9749f53924cea652a09d8e327', 
        '181f62ad3d1d8b78fa2695f30613b444', '04d1e516df784ab0cc7cb3b74a0933d2', 
        '1716ea399ed8ee62ba811e6f55180f45', '1a1d458cb32036ef1bfdc7896e3a63ce', 
        '188025a9e821ac2a983b63c5d7512df0', '20e04dcc7b37710b6fe52237cbe2a274', 
        '03c94210a8223f2f0c811f6783fe3d22', '0bb9709934061bd3316175ad24d90409', 
        '1a079ba5d672d64c3bdae859c45d8e67', '232a5adb0fc1881bbfeb03560c639c31', 
        '080a388eb2bd1051b01f6fc4ca659450', '10aa0f6833300990dff6f6763cc7ff8a', 
        '0eda670810c42c5fcf92ca8be2a615a9', '1da4de0fa473f8506723c82af7d4cb3b', 
        '108e21a48f54c559e2186db8a4bec9f9', '00fd6afd95fe066db8433832180a5369', 
        '00e62bcf9337ca4c5d5b4c5c8188f8d2', '24297ae137968498e50c2f283a4f3d9c', 
        '1264d5ede085c34d455f62cceca87791', '121314119f09ee1e994373873c7ab11d', 
        '0e0dec1c30232f86f5076622e9f623b6', '14a174908a08ad7cfb56d3814d8e0ea5', 
        '1e60f484bfeb1c1eadf273f6318738c0', '1a405418406359cc2b8815f93bf359c2', 
        '111e2f43245b193147270aba6d558129', '0f8016710ce920034150241f0b8b5def', 
        '1aafc94a341ff202817c81cd5e66e522', '0c46bfb6210825d07804a6d4f81a5a92', 
        '024553ca83fc6e9ec93f5a9c823d1834', '0a96387463e89f518bec31ed12378aea', 
        '0aa186c65b07e61f5ac114182ab92a2b', '25bd6053b3b5425cc4277b3c51641504', 
        '23429572ca0a6901f36825829cec09b1', '083b8b5ce2d02b7d5d4e1ceab8b7ed04', 
        '0352f26fa462bd615275f3e91857926d', '0d954479e7991c06d35202c130844b57', 
        '151259fe8ced305ca05dc771fc72d711', '17d4764518c5a017b128060439d1559f', 
        '0c9a1721e65cea1561c531b0e166cb1e', '130cdbc1715f7e1b5d9be728ce04398e', 
        '15873878161dd60a2de39df25443080e', '166b38c4ecd6765dad14586d0a8f7086'
    ]
    # 定义测试查询集（按类别组织）
    # expected_db 的含义：为了发挥“混合式数据库”的优势，应该路由到哪里
    # - mongo: 更适合文档/订单明细聚合、全文/模糊搜索、按主键读取大对象等（视你的架构而定）
    # - mysql: 更适合强一致事务、关系型 join、维表/主数据查询等
    # - both : 跨库查询/需要 federation（或由网关做两段查询再合并）
    queries = [
        # ------------------- 1. 点查询 (Point Query) -------------------
        {
            "type": "点查询 (Point Query)",
            "name": "点查询订单 (By Order ID)",
            "desc": "按订单主键查询订单；若订单在文档库/宽表中更快，期望路由到 MongoDB",
            "sql_template": "SELECT * FROM orders WHERE order_id = '{param}'",
            "params": sample_customer_ids,
            "expected": "mongo",
        },
        {
            "type": "点查询 (Point Query)",
            "name": "点查询用户 (By Customer ID)",
            "desc": "按客户主键查询客户信息；若 customers 作为关系型主数据，期望路由到 MySQL",
            "sql_template": "SELECT * FROM customers WHERE customer_id = '{param}'",
            "params": sample_product_ids,
            "expected": "mysql",
        },

        {
            "type": "范围查询 (Range Query)",
            "name": "时间范围查询 (Orders by Date)",
            "desc": "查询 2018 年 1 月份的所有订单",
            "sql_template": "SELECT * FROM orders WHERE order_purchase_timestamp BETWEEN '2018-01-01 00:00:00' AND '2018-01-31 23:59:59'",
            "params": None,
            "expected": "mongo"
        },
        {
            "type": "范围查询 (Range Query)",
            "name": "价格范围查询 (Items by Price)",
            "desc": "查询价格在 500 到 1000 之间的订单项",
            "sql_template": "SELECT * FROM order_items WHERE price BETWEEN 500 AND 1000 LIMIT 1000",
            "params": None,
            "expected": "mongo"
        },

        # ------------------- 2. 关联查询 (Join) -------------------
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
            "params": None,
            "expected": "mongo"
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
            "params": sample_customer_ids,
            "expected": "both"
        },

        # ------------------- 3. 文本搜索 (Text Search) -------------------
        {
            "type": "文本搜索 (Text Search)",
            "name": "评论关键词搜索 (LIKE)",
            "desc": "文本搜索/模糊匹配更偏文档/搜索型能力，期望路由到 MongoDB（或搜索引擎）",
            # 注意：你原例子里 LIKE 'marca' 不含通配符，严格来说是等值风格
            # 这里改成更典型的模糊匹配：LIKE '%marca%'
            "sql_template": "SELECT * FROM order_reviews WHERE review_comment_message LIKE '%estão%' LIMIT 100",
            "params": None,
            "expected": "mongo",
        },

        # ------------------- 4. 跨库查询 (Cross-DB / Federation) -------------------
        {
            "type": "聚合查询 (Aggregation)",
            "name": "热门城市统计 (Top 10 Cities)",
            "desc": "统计各城市的客户数量 Top 10",
            "sql_template": "SELECT customer_city, COUNT(*) as count FROM customers GROUP BY customer_city ORDER BY count DESC LIMIT 10",
            "params": None,
            "expected": "mysql",
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
            "expected": "both",
        },
    ]

    results = []
    total_cases = 0
    total_passed = 0

    for q in queries:
        print(f"测试: [{q['type']}] {q['name']}")
        times = []
        actual_dbs = []
        passed_count = 0

        for i in range(runs):
            # 选择参数并渲染 SQL
            if q.get("params"):
                import random
                param = random.choice(q["params"])
                sql = q["sql_template"].format(param=param)
            else:
                sql = q["sql_template"]

            # 请求执行
            t0 = time.time()
            resp = requests.post(f"{BASE_URL}/query", json={"sql": sql})
            t1 = time.time()

            if resp.status_code != 200:
                # 记录失败：这次耗时仍记录为客户端观测耗时
                times.append(t1 - t0)
                actual_dbs.append("HTTP_ERROR")
                continue

            data = resp.json()

            # actual_db：以服务端返回为准
            actual_db = data.get("analysis", {}).get("db_type", "UNKNOWN")
            reason = data.get("analysis", {}).get("reason", "")
            # time：优先用服务端统计；若没有，则用客户端观测
            server_time = data.get("stats", {}).get("response_time", None)
            observed_time = (server_time if isinstance(server_time, (int, float)) else (t1 - t0))

            times.append(observed_time)
            actual_dbs.append(actual_db)

            if actual_db == q["expected"]:
                passed_count += 1

            # 你如果想看每次的理由，可取消注释
            # print(f"  run {i+1:02d}: actual={actual_db}, time={observed_time:.3f}s, reason={reason}")

        avg_time = sum(times) / len(times) if times else float("inf")

        # 以“多数投票”作为这类查询的最终 actual（更符合“类别路由策略”评估）
        from collections import Counter
        actual_majority = Counter(actual_dbs).most_common(1)[0][0] if actual_dbs else "UNKNOWN"

        # 类别级 pass：用多数投票 vs expected（也可以改成“20次里通过次数占比”）
        category_passed = (actual_majority == q["expected"])

        total_cases += 1
        if category_passed:
            total_passed += 1

        print(f"  -> 平均耗时: {avg_time:.4f}s (runs={runs})")
        print(f"  -> Expected: {q['expected']} | Actual(majority): {actual_majority} | PASS: {category_passed}")
        print()

        results.append({
            "Type": q["type"],
            "Name": q["name"],
            "Time": avg_time,
            "Expected": q["expected"],
            "Actual": actual_majority,
            "Pass": category_passed,
            # 额外信息：该类别 20 次的通过率 & 实际分布
            "PassRate": passed_count / runs if runs else 0.0,
            "ActualDistribution": dict(Counter(actual_dbs)),
        })

    accuracy = total_passed / total_cases if total_cases else 0.0
    print(f"=== 测试完成 [{label}] ===")
    print(f"类别级正确率(majority vote): {total_passed}/{total_cases} = {accuracy:.2%}\n")

    print("\n=== 最终路由与性能报告 (Hybrid Routing Performance Report) ===")
    report = []
    for r in results:  # results = run_hybrid_routing_benchmark(...) 返回的 results
        report.append({
            "Query Type": r["Type"],
            "Query Name": r["Name"],
            "Avg Time (s)": f"{r['Time']:.4f}",
            "Expected": r["Expected"],
            "Actual": r["Actual"],
            "PASS": "✓" if r["Pass"] else "✗",
            "PassRate(20x)": f"{r['PassRate']:.2%}",
            # 如不想显示分布可删掉这一列
            "ActualDist": str(r.get("ActualDistribution", {}))
        })

    df_report = pd.DataFrame(report)
    print(df_report.to_string(index=False))

    # accuracy = run_hybrid_routing_benchmark(...) 返回的 accuracy
    print(f"\n=== 总体正确率 (Category-level Accuracy, Majority Vote) ===")
    print(f"Accuracy: {accuracy:.2%}  ({sum(1 for r in results if r['Pass'])}/{len(results)})")

    return results, accuracy


if __name__ == "__main__":
    # 先检查服务是否健康
    try:
        health = requests.get(f"{BASE_URL}/health", timeout=5)
        print(health)
        if health.status_code == 200:
            print("✅ Fusion Router is healthy")
            run_hybrid_routing_benchmark()
        else:
            print("❌ Fusion Router is not responding")
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to Fusion Router. Is it running?")
        print("   Start it with: docker-compose up -d fusion-router")