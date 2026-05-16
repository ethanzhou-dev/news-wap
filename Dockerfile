# 使用官方轻量级 Python 镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /code

# 复制并安装依赖
COPY requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目所有文件到工作目录
COPY . .

# 暴露 Hugging Face 默认的 7860 端口
EXPOSE 7860

# [修复] 设置容器时区为上海，解决日期不对的问题
ENV TZ=Asia/Shanghai

# 启动 FastAPI 服务
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860", "--timeout-keep-alive", "15"]