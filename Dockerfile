# 使用官方 Python 基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制脚本
COPY mysql_benchmark.py .

# 注意：数据集将通过 Volume 挂载到 /app/dataset
# 这样就不需要把大数据集构建到镜像里了

# 设置环境变量默认值
ENV PYTHONUNBUFFERED=1

# 运行命令
CMD ["python", "mysql_benchmark.py"]
