# emby-tmdb-bot

一个基于 Docker Compose 部署的 Emby / TMDB / Telegram Bot。

## 快速开始

```bash
git clone https://github.com/emby-bot/emby-tmdb-bot.git
cd emby-tmdb-bot
cp .env.example .env
nano .env
docker compose up -d --build
```

## 需要填写的配置

在 `.env` 里至少填写这些：

- `BOT_TOKEN`
- `TMDB_API_KEY`
- `OWNER_CHAT_ID`
- `ADMIN_CHAT_IDS`
- `EMBY_URL`
- `EMBY_API_KEY`
- `EMBY_WEBHOOK_SECRET`

## 常用命令

启动：

```bash
docker compose up -d
```

停止：

```bash
docker compose down
```

重启：

```bash
docker compose restart
```

重新构建并启动：

```bash
docker compose up -d --build
```

## 说明

- 数据目录是 `./data`

## 更新项目

如果你已经部署过项目，后续更新代码可执行：

```bash
git pull
docker compose up -d --build