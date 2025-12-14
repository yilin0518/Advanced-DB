# fusion_router/Dockerfile
FROM python:3.9-slim

WORKDIR /app

# 只复制和安装Python包，不安装系统包
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python", "api/app.py"]