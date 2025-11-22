# 高级数据库小组作业：Olist 电商数据集性能评测实验方案

## 1. 项目背景与目标
本项目旨在通过对真实电商数据集（Brazilian E-Commerce Public Dataset by Olist）的实际操作，深入理解不同数据库系统在处理复杂关系数据时的性能差异。我们将以 MySQL 为基准（Baseline），测试其在数据导入、复杂查询、聚合分析等场景下的性能表现，并为后续引入其他数据库（如 MongoDB, Neo4j）做对比准备。

## 2. 数据集概述
数据集包含 2016-2018 年间巴西 Olist 商店的 10 万条订单信息。主要包含以下关系表：
- **核心交易流**：`orders` (订单), `order_items` (订单明细), `order_payments` (支付), `order_reviews` (评价)
- **实体信息**：`customers` (客户), `products` (商品), `sellers` (卖家)
- **辅助信息**：`geolocation` (地理位置), `product_category_name_translation` (类别翻译)

## 3. 实验环境
- **操作系统**: Windows (当前环境)
- **数据库版本**: MySQL 8.0 (推荐)
- **编程语言**: Python 3.x (使用 Pandas 进行数据处理，SQLAlchemy 进行数据库交互)

## 4. 实验设计方案

### 4.1 数据库建模 (Schema Design)
在关系型数据库中，我们将遵循第三范式 (3NF) 进行建模，建立如下外键关系：
- `orders.customer_id` -> `customers.customer_id`
- `order_items.order_id` -> `orders.order_id`
- `order_items.product_id` -> `products.product_id`
- `order_items.seller_id` -> `sellers.seller_id`
*(注：为了测试性能，实验初期可先通过 Pandas 自动建表，随后手动添加索引以对比优化效果)*

### 4.2 测试场景 (Test Scenarios)

#### A. 数据加载性能 (Load Performance)
- **测试内容**: 记录将所有 CSV 文件加载到数据库所需的总时间。
- **指标**: 写入速度 (Rows/sec), 总耗时 (Seconds)。

#### B. 查询性能测试 (Query Performance)
我们将设计三类查询来模拟真实业务场景：

1.  **简单点查询 (Point Query)**
    - *场景*: 用户查看自己的订单状态。
    - *SQL*: `SELECT * FROM olist_orders WHERE customer_id = '...'`
    - *目的*: 测试索引查找性能。

2.  **多表关联查询 (Complex Join)**
    - *场景*: 生成订单详情页（包含商品名、卖家地、支付方式）。
    - *SQL*: 联结 `orders`, `order_items`, `products`, `sellers` 四张表。
    - *目的*: 测试数据库处理 Join 的效率，这是关系型数据库的核心能力。

3.  **聚合分析查询 (OLAP Aggregation)**
    - *场景*: 销售仪表盘。
    - *SQL*: 统计每个产品类别的平均客单价和总销量，并按销量排序。
    - *目的*: 测试全表扫描和聚合计算 (Group By) 的性能。

### 4.3 实验步骤
1.  **环境准备**: 启动 MySQL 服务，创建空数据库 `olist_db`。
2.  **数据清洗**: 使用 Python 处理 CSV 中的日期格式和空值。
3.  **数据导入**: 批量写入数据，记录时间。
4.  **基准测试**: 运行预定义的 SQL 查询集，每组查询重复运行 10 次取平均值。
5.  **索引优化**: 为常用查询字段（如 `customer_id`, `product_category`）添加索引，重复步骤 4，对比性能提升。

## 5. 预期成果
- 一份完整的性能测试报告（图表展示）。
- 包含数据加载和自动化测试的 Python 脚本。
- 对比分析：有无索引对查询性能的影响。

---
*小组分工建议：*
- 成员 A: 负责环境搭建和数据清洗脚本。
- 成员 B: 负责设计 SQL 查询语句和索引策略。
- 成员 C: 负责撰写报告和分析实验结果。
