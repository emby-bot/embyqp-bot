ARG BASE_IMAGE=m.daocloud.io/docker.io/library/python:3.11-slim
FROM ${BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir \
    -i https://mirrors.ustc.edu.cn/pypi/simple \
    -r requirements.txt

COPY app.py .
COPY app_private_menu.py .

EXPOSE 8787

CMD ["python", "app_private_menu.py"]