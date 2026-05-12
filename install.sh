#!/usr/bin/env bash
set -e

APP_DIR="/home/embyqp-bot"
IMAGE="zyjx/embyqp-bot:v1.0.0"
CONTAINER_NAME="embyqp-bot"

echo "======================================"
echo " Emby 求片 Bot 一键安装脚本"
echo " 镜像：${IMAGE}"
echo " 安装目录：${APP_DIR}"
echo "======================================"
echo

# 检查 Docker
if ! command -v docker >/dev/null 2>&1; then
  echo "❌ 未检测到 Docker，请先安装 Docker 后再运行。"
  exit 1
fi

# 检查 docker compose
if ! docker compose version >/dev/null 2>&1; then
  echo "❌ 未检测到 Docker Compose 插件，请先安装 docker compose。"
  exit 1
fi

echo "请按提示填写配置。"
echo "带 * 的为必填项。"
echo

read -rp "* Telegram BOT_TOKEN: " BOT_TOKEN
read -rp "* TMDB_API_KEY: " TMDB_API_KEY
read -rp "* OWNER_CHAT_ID，（Bot 主人 TG ID）: " OWNER_CHAT_ID
read -rp "* ADMIN_CHAT_IDS，（管理员TG ID，多个管理员用英文逗号分隔）: " ADMIN_CHAT_IDS
read -rp "* EMBY_URL: " EMBY_URL
read -rp "* EMBY_API_KEY: " EMBY_API_KEY
read -rp "TG_CHANNEL_ID（入库通知频道） ID: " TG_CHANNEL_ID
read -rp "EMBY_WEBHOOK_SECRET（32位随机字符串）: " EMBY_WEBHOOK_SECRET
read -rp "宿主机映射端口，默认 8787: " HOST_PORT

HOST_PORT="${HOST_PORT:-8787}"

# 简单校验
if [ -z "$BOT_TOKEN" ]; then
  echo "❌ BOT_TOKEN 不能为空。"
  exit 1
fi

if [ -z "$TMDB_API_KEY" ]; then
  echo "❌ TMDB_API_KEY 不能为空。"
  exit 1
fi

if [ -z "$OWNER_CHAT_ID" ]; then
  echo "❌ OWNER_CHAT_ID 不能为空。"
  exit 1
fi

if [ -z "$ADMIN_CHAT_IDS" ]; then
  ADMIN_CHAT_IDS="$OWNER_CHAT_ID"
fi

if [ -z "$EMBY_URL" ]; then
  echo "❌ EMBY_URL 不能为空。"
  exit 1
fi

if [ -z "$EMBY_API_KEY" ]; then
  echo "❌ EMBY_API_KEY 不能为空。"
  exit 1
fi

echo
echo "==> 创建目录：${APP_DIR}"
mkdir -p "${APP_DIR}/data"
cd "${APP_DIR}"

echo "==> 生成 docker-compose.yml"
cat > docker-compose.yml <<EOF
services:
  embyqp-bot:
    image: ${IMAGE}
    container_name: ${CONTAINER_NAME}
    restart: always
    env_file:
      - .env
    environment:
      TZ: Asia/Shanghai
    ports:
      - "${HOST_PORT}:8787"
    volumes:
      - ./data:/app/data
EOF

echo "==> 生成 .env"
cat > .env <<EOF
# =========================
# Telegram Bot 配置
# =========================
BOT_TOKEN=${BOT_TOKEN}
OWNER_CHAT_ID=${OWNER_CHAT_ID}
ADMIN_CHAT_IDS=${ADMIN_CHAT_IDS}

# =========================
# TMDB 配置
# =========================
TMDB_API_KEY=${TMDB_API_KEY}
TMDB_LANGUAGE=zh-CN
RESULT_LIMIT=5

# =========================
# Emby 配置
# =========================
EMBY_URL=${EMBY_URL}
EMBY_API_KEY=${EMBY_API_KEY}
EMBY_USER_ID=

# =========================
# Emby Webhook / 入库通知
# =========================
EMBY_WEBHOOK_HOST=0.0.0.0
EMBY_WEBHOOK_PORT=8787
EMBY_WEBHOOK_SECRET=${EMBY_WEBHOOK_SECRET}
TG_CHANNEL_ID=${TG_CHANNEL_ID}

# =========================
# 请求超时 / 轮询
# =========================
POLL_TIMEOUT=30
REQUEST_TIMEOUT=65

# =========================
# 用户额度
# =========================
DEFAULT_QUOTA_LIMIT=3

# =========================
# 待审请求清理
# =========================
PENDING_EXPIRE_SECONDS=86400
PENDING_CLEANUP_INTERVAL=600
PENDING_QP_INPUT_EXPIRE_SECONDS=300
EOF

echo
echo "==> 拉取镜像"
docker compose pull

echo
echo "==> 启动容器"
docker compose up -d

echo
echo "======================================"
echo "✅ 安装完成"
echo "======================================"
