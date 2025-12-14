import requests
import json


def test_endpoints():
    base_url = "http://localhost:8002"
    resp = requests.get(base_url + "/api/health")

    print("status:", resp.status_code)
    print("content-type:", resp.headers.get("Content-Type"))

    print("\n3. 测试简单查询...")
    queries = [
        "SELECT * FROM customers LIMIT 3",
        "SELECT COUNT(*) FROM orders",
        "SELECT * FROM orders WHERE customer_id = '4e7b3e00288586ebd08712fdd0374a03'",
        "SELECT * FROM orders WHERE order_id = '00010242fe8c5a6d1ba2dd792cb16214'"
    ]

    for sql in queries:
        print(f"\n查询: {sql}")
        response = requests.post(f"{base_url}/api/query", json={"sql": sql})
        if response.status_code == 200:
            result = response.json()
            print(result.keys())
            print(f"状态: 成功")
            print(f"用时: {result.get('execution_time')}s")
            print(f"使用数据库: {result.get('database_used')}")
            if 'data' in result:
                data = result['data']
                if isinstance(data, list):
                    print(f"返回 {len(data)} 条记录")
                    if data:
                        print("第一条记录:", json.dumps(data[0], default=str, indent=2)[:200] + "...")
                else:
                    print("结果:", json.dumps(data, default=str, indent=2)[:200])
        else:
            print(f"状态: 失败 ({response.status_code})")
            print("错误:", response.text)


if __name__ == "__main__":
    test_endpoints()