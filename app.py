import os
import re
import time
import json
import logging
import sqlite3
from threading import Thread, Lock
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import requests
from flask import Flask, request, jsonify


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()
OWNER_CHAT_ID = os.getenv("OWNER_CHAT_ID", "").strip()  # 兼容旧配置
ADMIN_CHAT_IDS = os.getenv("ADMIN_CHAT_IDS", "").strip()  # 多管理员，逗号分隔
TMDB_LANGUAGE = os.getenv("TMDB_LANGUAGE", "zh-CN").strip()
RESULT_LIMIT = int(os.getenv("RESULT_LIMIT", "5"))
POLL_TIMEOUT = int(os.getenv("POLL_TIMEOUT", "30"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "65"))
EMBY_URL = os.getenv("EMBY_URL", "").strip()
EMBY_API_KEY = os.getenv("EMBY_API_KEY", "").strip()
EMBY_USER_ID = os.getenv("EMBY_USER_ID", "").strip()
EMBY_WEBHOOK_HOST = os.getenv("EMBY_WEBHOOK_HOST", "0.0.0.0").strip()
EMBY_WEBHOOK_PORT = int(os.getenv("EMBY_WEBHOOK_PORT", "8787"))
EMBY_WEBHOOK_SECRET = os.getenv("EMBY_WEBHOOK_SECRET", "").strip()
TG_CHANNEL_ID = os.getenv("TG_CHANNEL_ID", "").strip()

app = Flask(__name__)

if not BOT_TOKEN:
    raise RuntimeError("缺少 BOT_TOKEN")
if not TMDB_API_KEY:
    raise RuntimeError("缺少 TMDB_API_KEY")

TELEGRAM_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
TMDB_BASE = "https://api.themoviedb.org/3"

session = requests.Session()
RECENT_NOTIFY_CACHE: Dict[str, float] = {}
RECENT_NOTIFY_TTL = 1200  # 秒

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
USERS_FILE = os.path.join(DATA_DIR, "users.json")
REQUESTS_FILE = os.path.join(DATA_DIR, "pending_requests.json")
DEFAULT_QUOTA_LIMIT = int(os.getenv("DEFAULT_QUOTA_LIMIT", "3"))
USER_DATA_LOCK = Lock()
REQUESTS_LOCK = Lock()

PENDING_EXPIRE_SECONDS = int(os.getenv("PENDING_EXPIRE_SECONDS", str(24 * 60 * 60)))  # 默认24小时
PENDING_CLEANUP_INTERVAL = int(os.getenv("PENDING_CLEANUP_INTERVAL", "600"))  # 默认每10分钟清理一次


def ensure_user_storage() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

def ensure_request_storage() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

def _read_request_store_unlocked() -> Dict[str, Any]:
    os.makedirs(DATA_DIR, exist_ok=True)
    with DB_LOCK:
        conn = get_db_conn()
        try:
            rows = conn.execute('SELECT * FROM pending_requests').fetchall()
            return {
                str(row['request_id']): _row_to_dict(row)
                for row in rows
            }
        finally:
            conn.close()

def _write_request_store_unlocked(store: Dict[str, Any]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute('DELETE FROM pending_requests')
            for request_id, value in (store or {}).items():
                if not isinstance(value, dict):
                    continue
                conn.execute(
                    """
                    INSERT OR REPLACE INTO pending_requests (
                        request_id, status, requester_user_id, requester_chat_id, media_type, tmdb_id,
                        requester_name, admin_message_refs, created_at, created_at_ts,
                        handled_by, handled_at, action
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request_id,
                        str(value.get('status') or 'pending'),
                        _safe_int(value.get('requester_user_id'), 0),
                        _safe_int(value.get('requester_chat_id'), 0),
                        str(value.get('media_type') or ''),
                        _safe_int(value.get('tmdb_id'), 0),
                        value.get('requester_name') or '',
                        _json_dumps(value.get('admin_message_refs') or []),
                        value.get('created_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        _safe_int(value.get('created_at_ts'), int(time.time())),
                        _safe_int(value.get('handled_by'), 0) or None,
                        value.get('handled_at'),
                        value.get('action'),
                    )
                )
            conn.commit()
        finally:
            conn.close()

def parse_admin_chat_ids() -> List[int]:
    ids: List[int] = []

    # 新配置：ADMIN_CHAT_IDS=111,222,333
    if ADMIN_CHAT_IDS:
        for raw in ADMIN_CHAT_IDS.split(","):
            raw = raw.strip()
            if not raw:
                continue
            try:
                ids.append(int(raw))
            except ValueError:
                logging.warning("ADMIN_CHAT_IDS 中存在非法 chat_id：%s", raw)

    # 兼容旧配置 OWNER_CHAT_ID
    if OWNER_CHAT_ID:
        try:
            old_id = int(OWNER_CHAT_ID)
            if old_id not in ids:
                ids.append(old_id)
        except ValueError:
            logging.warning("OWNER_CHAT_ID 不是合法数字，已忽略")

    return ids


def is_admin_user(user_id: int) -> bool:
    return user_id in parse_admin_chat_ids()


def create_request_id(
    requester_chat_id: int,
    media_type: str,
    tmdb_id: int,
    season_number: Optional[int] = None,
) -> str:
    season_part = ""
    season_number = _safe_int(season_number, 0)
    if media_type == "tv" and season_number > 0:
        season_part = f"_s{season_number}"

    return f"{requester_chat_id}_{media_type}_{tmdb_id}{season_part}_{int(time.time() * 1000)}"


def parse_request_id_meta(request_id: str) -> Dict[str, Any]:
    m = re.match(
        r"^(?P<chat>-?\d+)_(?P<media>movie|tv)_(?P<tmdb>\d+)(?:_s(?P<season>\d+))?_(?P<ts>\d+)$",
        str(request_id or "").strip(),
    )
    if not m:
        return {}

    return {
        "requester_chat_id": _safe_int(m.group("chat"), 0),
        "media_type": m.group("media"),
        "tmdb_id": _safe_int(m.group("tmdb"), 0),
        "season_number": _safe_int(m.group("season"), 0),
        "created_ms": _safe_int(m.group("ts"), 0),
    }


def _get_request_created_ts(req: Dict[str, Any]) -> int:
    created_at_ts = req.get("created_at_ts")
    try:
        created_at_ts = int(created_at_ts)
        if created_at_ts > 0:
            return created_at_ts
    except (TypeError, ValueError):
        pass

    created_at = str(req.get("created_at") or "").strip()
    if created_at:
        try:
            return int(datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").timestamp())
        except ValueError:
            pass

    return 0


def _is_request_expired(req: Dict[str, Any], now_ts: Optional[int] = None) -> bool:
    now_ts = now_ts or int(time.time())
    created_ts = _get_request_created_ts(req)
    return created_ts > 0 and (now_ts - created_ts >= PENDING_EXPIRE_SECONDS)

def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def calc_remaining_quota(record: Dict[str, Any]) -> int:
    quota_limit = _safe_int(record.get("quota_limit"), DEFAULT_QUOTA_LIMIT)
    success_count = _safe_int(record.get("success_count"), 0)
    return max(quota_limit - success_count, 0)

def _read_user_store_unlocked() -> Dict[str, Any]:
    os.makedirs(DATA_DIR, exist_ok=True)
    with DB_LOCK:
        conn = get_db_conn()
        try:
            rows = conn.execute('SELECT * FROM users').fetchall()
            return {
                str(row['tg_id']): dict(row)
                for row in rows
            }
        finally:
            conn.close()

def _write_user_store_unlocked(store: Dict[str, Any]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute('DELETE FROM users')
            for key, value in (store or {}).items():
                if not isinstance(value, dict):
                    continue
                tg_id = _safe_int(value.get('tg_id') or key, 0)
                if not tg_id:
                    continue
                quota_limit = _safe_int(value.get('quota_limit'), DEFAULT_QUOTA_LIMIT)
                success_count = _safe_int(value.get('success_count'), 0)
                remaining_quota = max(quota_limit - success_count, 0)
                conn.execute(
                    """
                    INSERT INTO users (
                        tg_id, chat_id, username, first_name, last_name, display_name,
                        quota_limit, success_count, remaining_quota, updated_at, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tg_id) DO UPDATE SET
                        chat_id=excluded.chat_id,
                        username=excluded.username,
                        first_name=excluded.first_name,
                        last_name=excluded.last_name,
                        display_name=excluded.display_name,
                        quota_limit=excluded.quota_limit,
                        success_count=excluded.success_count,
                        remaining_quota=excluded.remaining_quota,
                        updated_at=excluded.updated_at,
                        last_seen_at=excluded.last_seen_at
                    """,
                    (
                        tg_id,
                        _safe_int(value.get('chat_id'), 0) or None,
                        (value.get('username') or '').strip(),
                        value.get('first_name') or '',
                        value.get('last_name') or '',
                        value.get('display_name') or '',
                        quota_limit,
                        success_count,
                        remaining_quota,
                        value.get('updated_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        value.get('last_seen_at') or value.get('updated_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    )
                )
            conn.commit()
        finally:
            conn.close()


def migrate_json_to_sqlite_if_needed() -> None:
    init_sqlite_db()
    if os.path.exists(USERS_FILE) or os.path.exists(REQUESTS_FILE):
        migrate_json_to_sqlite()

def normalize_emby_base(url: str) -> str:
    url = (url or "").rstrip("/")
    if not url:
        return ""
    return url if url.endswith("/emby") else url + "/emby"


EMBY_BASE = normalize_emby_base(EMBY_URL)


def emby_enabled() -> bool:
    return bool(EMBY_BASE and EMBY_API_KEY)


def emby_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not emby_enabled():
        raise RuntimeError("未配置 EMBY_URL / EMBY_API_KEY")

    params = params or {}
    params["api_key"] = EMBY_API_KEY

    if EMBY_USER_ID and "UserId" not in params:
        params["UserId"] = EMBY_USER_ID

    resp = session.get(
        f"{EMBY_BASE}{path}",
        params=params,
        timeout=REQUEST_TIMEOUT,
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def normalize_title(text: str) -> str:
    return re.sub(r"[\W_]+", "", (text or "").lower())

def get_notify_media_code(item: Dict[str, Any]) -> str:
    tmdb_media_type = item.get("_tmdb_media_type")
    if tmdb_media_type == "movie":
        return "movie"
    if tmdb_media_type == "tv":
        return "tv"

    normalized = item.get("_normalized") or {}
    item_type = str(normalized.get("item_type") or item.get("Type") or "").lower()

    if item_type == "movie":
        return "movie"
    if item_type in ("series", "episode", "tv"):
        return "tv"

    return "unknown"

def should_skip_recent_notify(item: Dict[str, Any]) -> bool:
    now = time.time()

    expired = [k for k, ts in RECENT_NOTIFY_CACHE.items() if now - ts > RECENT_NOTIFY_TTL]
    for k in expired:
        RECENT_NOTIFY_CACHE.pop(k, None)

    key = build_tmdb_notify_key(item)
    if not key:
        return False

    if key in RECENT_NOTIFY_CACHE:
        return True

    RECENT_NOTIFY_CACHE[key] = now
    return False

def build_tmdb_notify_key(item: Dict[str, Any]) -> Optional[str]:
    media_code = get_notify_media_code(item)

    tmdb_id = item.get("_tmdb_id")
    if tmdb_id:
        return f"{media_code}:tmdb:{tmdb_id}"

    normalized = item.get("_normalized") or {}
    title = (
        item.get("_tmdb_name")
        or normalized.get("title")
        or item.get("SeriesName")
        or item.get("Name")
        or "unknown"
    )
    year = (
        item.get("_tmdb_year")
        or normalized.get("year")
        or item.get("ProductionYear")
        or ""
    )
    return f"{media_code}:title:{normalize_title(title)}:{year}"


def clear_recent_notify(item: Dict[str, Any]) -> None:
    key = build_tmdb_notify_key(item)
    if not key:
        return

    if key in RECENT_NOTIFY_CACHE:
        RECENT_NOTIFY_CACHE.pop(key, None)
        logging.info("已清除通知去重缓存：%s", key)

def emby_item_type_label(item: Dict[str, Any]) -> str:
    item_type = (item.get("Type") or "").lower()
    if item_type == "movie":
        return "🎬电影"
    if item_type == "series":
        return "📺️剧集"
    return item.get("Type") or "媒体"


def format_emby_item_name(item: Dict[str, Any]) -> str:
    name = item.get("Name") or "未知标题"
    year = item.get("ProductionYear")
    return f"{name} ({year})" if year else name

def get_emby_notify_type(item: Dict[str, Any]) -> str:
    tmdb_media_type = item.get("_tmdb_media_type")
    if tmdb_media_type == "movie":
        return "电影"
    if tmdb_media_type == "tv":
        return "剧集"

    item_type = (item.get("Type") or "").lower()
    if item_type == "movie":
        return "电影"
    if item_type in ("series", "episode"):
        return "剧集"

    return "影视"


def format_emby_rating(item: Dict[str, Any]) -> str:
    if "_tmdb_rating" in item and item.get("_tmdb_rating") not in (None, ""):
        try:
            return f"{float(item['_tmdb_rating']):.1f}"
        except Exception:
            pass

    rating = item.get("CommunityRating")
    if rating is None:
        rating = item.get("CriticRating")

    if rating is None or rating == "":
        return "未知"

    try:
        return f"{float(rating):.1f}"
    except Exception:
        return str(rating)


def format_emby_notify_title(item: Dict[str, Any]) -> str:
    name = (
        item.get("_tmdb_name")
        or (item.get("_normalized") or {}).get("title")
        or item.get("SeriesName")
        or item.get("Name")
        or "未知标题"
    )
    year = (
        item.get("_tmdb_year")
        or (item.get("_normalized") or {}).get("year")
        or item.get("ProductionYear")
    )

    return f"{name} ({year})" if year else name


SEASON_CN_MAP = {
    1: "第一季",
    2: "第二季",
    3: "第三季",
    4: "第四季",
    5: "第五季",
    6: "第六季",
    7: "第七季",
    8: "第八季",
    9: "第九季",
    10: "第十季",
    11: "第十一季",
    12: "第十二季",
}


def format_season_cn(season_number: Any) -> str:
    n = _safe_int(season_number, 0)
    if n <= 0:
        return ""
    return SEASON_CN_MAP.get(n, f"第{n}季")


def build_emby_primary_image_url(item: Dict[str, Any]) -> Optional[str]:
    if not emby_enabled():
        return None

    item_type = (item.get("Type") or "").lower()

    # 单集优先用剧集主图
    image_item_id = item.get("SeriesId") if item_type == "episode" and item.get("SeriesId") else item.get("Id")
    if not image_item_id:
        return None

    return (
        f"{EMBY_BASE}/Items/{image_item_id}/Images/Primary"
        f"?maxWidth=1000&quality=90&api_key={EMBY_API_KEY}"
    )

def build_channel_notify_image_url(item: Dict[str, Any]) -> Optional[str]:
    tmdb_media_type = item.get("_tmdb_media_type")
    tmdb_id = item.get("_tmdb_id")

    if tmdb_media_type in ("movie", "tv") and tmdb_id:
        try:
            detail = get_detail_with_fallback(tmdb_media_type, int(tmdb_id))
            tmdb_cover = build_tmdb_cover_url(detail)
            if tmdb_cover:
                return tmdb_cover
        except Exception:
            logging.exception("获取 TMDB 海报失败，回退到 Emby 图片")

    return build_emby_primary_image_url(item)

def get_emby_tmdb_id(item: Dict[str, Any]) -> str:
    if item.get("_tmdb_id"):
        return str(item["_tmdb_id"])

    normalized = item.get("_normalized") or {}
    if normalized.get("tmdb_id"):
        return str(normalized["tmdb_id"])

    provider_ids = item.get("ProviderIds") or {}
    return (
        provider_ids.get("Tmdb")
        or provider_ids.get("tmdb")
        or "未知"
    )


def format_emby_added_time(item: Dict[str, Any]) -> str:
    raw = (
        item.get("DateCreated")
        or item.get("DateLastMediaAdded")
        or item.get("PremiereDate")
        or ""
    )

    if not raw:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return raw


def emby_search_by_query(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    if not emby_enabled():
        return []

    data = emby_get("/Items", {
        "SearchTerm": query,
        "Recursive": "true",
        "IncludeItemTypes": "Movie,Series",
        "Fields": "ProviderIds,Overview",
        "EnableImages": "true",
        "Limit": limit,
    })
    return data.get("Items", [])


def emby_find_exact_title(query: str) -> Optional[Dict[str, Any]]:
    needle = normalize_title(query)
    if not needle:
        return None

    for item in emby_search_by_query(query, limit=20):
        if normalize_title(item.get("Name") or "") == needle:
            return item
    return None


def emby_find_by_tmdb(media_type: str, tmdb_id: int) -> Optional[Dict[str, Any]]:
    if not emby_enabled():
        return None

    include_item_types = "Movie" if media_type == "movie" else "Series"

    data = emby_get("/Items", {
        "Recursive": "true",
        "IncludeItemTypes": include_item_types,
        "AnyProviderIdEquals": f"tmdb.{tmdb_id}",
        "Fields": "ProviderIds,Overview",
        "EnableImages": "true",
        "Limit": 1,
    })

    items = data.get("Items", [])
    return items[0] if items else None

def _extract_emby_items(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        items = data.get("Items")
        if isinstance(items, list):
            return items
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def _get_emby_season_number(item: Dict[str, Any]) -> int:
    for key in ("IndexNumber", "ParentIndexNumber", "Number"):
        n = _safe_int(item.get(key), 0)
        if n > 0:
            return n

    name = str(item.get("Name") or "").strip()
    m = re.search(r"(\d+)", name)
    if m:
        return _safe_int(m.group(1), 0)

    return 0


def emby_get_series_seasons(series_id: str) -> List[Dict[str, Any]]:
    if not series_id or not emby_enabled():
        return []

    # 官方季列表接口
    try:
        data = emby_get(
            f"/Shows/{series_id}/Seasons",
            {
                "Fields": "ProviderIds,ParentId,SeriesId,SeasonId,Overview"
            },
        )
        items = _extract_emby_items(data)
        if items:
            return items
    except Exception:
        logging.exception("读取 Emby 季列表失败：series_id=%s", series_id)

    # 兜底：直接从 Items 查 Season
    try:
        data = emby_get(
            "/Items",
            {
                "Recursive": "true",
                "IncludeItemTypes": "Season",
                "SeriesId": series_id,
                "Fields": "ProviderIds,ParentId,SeriesId,SeasonId,Overview",
                "Limit": 300,
            },
        )
        return _extract_emby_items(data)
    except Exception:
        logging.exception("通过 /Items 读取 Emby 季列表失败：series_id=%s", series_id)
        return []


def emby_find_any_episode_by_series_and_season(series_id: str, season_number: int) -> Optional[Dict[str, Any]]:
    if not series_id or not emby_enabled() or season_number <= 0:
        return None

    try:
        data = emby_get(
            "/Items",
            {
                "Recursive": "true",
                "IncludeItemTypes": "Episode",
                "SeriesId": series_id,
                "ParentIndexNumber": season_number,
                "Fields": "ProviderIds,ParentId,SeriesId,SeasonId,Overview",
                "Limit": 1,
            },
        )
        items = _extract_emby_items(data)
        return items[0] if items else None
    except Exception:
        logging.exception("按季读取剧集集数失败：series_id=%s season=%s", series_id, season_number)
        return None


def emby_find_tv_season_by_tmdb(tv_tmdb_id: int, season_number: int) -> Optional[Dict[str, Any]]:
    season_number = _safe_int(season_number, 0)
    if season_number <= 0:
        return None

    series_item = emby_find_by_tmdb("tv", tv_tmdb_id)
    if not series_item:
        return None

    series_id = str(series_item.get("Id") or "").strip()
    if not series_id:
        return None

    # 先从 Season 项判断
    seasons = emby_get_series_seasons(series_id)
    for season_item in seasons:
        if _get_emby_season_number(season_item) == season_number:
            if not season_item.get("SeriesName"):
                season_item["SeriesName"] = series_item.get("Name") or series_item.get("SeriesName") or ""
            if not season_item.get("Type"):
                season_item["Type"] = "Season"
            return season_item

    # 再兜底：如果有这个季的任意一集，也算这个季已存在
    episode_item = emby_find_any_episode_by_series_and_season(series_id, season_number)
    if episode_item:
        return {
            "Id": episode_item.get("SeasonId") or episode_item.get("Id") or series_id,
            "Name": format_season_cn(season_number),
            "Type": "Season",
            "IndexNumber": season_number,
            "SeriesName": episode_item.get("SeriesName") or series_item.get("Name") or "未知标题",
            "ProductionYear": series_item.get("ProductionYear"),
        }

    return None

def format_emby_exists_title(item: Dict[str, Any]) -> str:
    item_type = str(item.get("Type") or "").lower()

    if item_type == "season":
        series_name = item.get("SeriesName") or "未知标题"
        season_number = _get_emby_season_number(item)
        if season_number > 0:
            return f"{series_name} {format_season_cn(season_number)}"
        season_name = item.get("Name") or ""
        return f"{series_name} {season_name}".strip()

    return format_emby_item_name(item)


def notify_user_emby_exists(chat_id: int, item: Dict[str, Any]) -> None:
    title = format_emby_exists_title(item)
    item_type = str(item.get("Type") or "").lower()

    if item_type == "season":
        media_label = "📺️剧集"
    else:
        media_label = emby_item_type_label(item)

    text = (
        "✅ *Emby 库中已存在该资源*\n\n"
        f"🎬 *影片名：* {escape_markdown_v2(title)}\n"
        f"*类型：* {escape_markdown_v2(media_label)}"
    )

    send_message(chat_id, text, parse_mode="MarkdownV2")

def build_channel_new_item_text(
    item: Dict[str, Any],
    extra_lines: Optional[List[str]] = None,
    stats: Optional[Dict[str, Any]] = None,
) -> str:
    title = format_emby_notify_title(item)
    media_type = get_emby_notify_type(item)
    rating = format_emby_rating(item)
    tmdb_id = get_emby_tmdb_id(item)
    added_time = format_emby_added_time(item)

    if media_type == "剧集":
        tmdb_total_seasons = _safe_int(item.get("_tmdb_number_of_seasons"), 0)
        current_season = _safe_int(
            (stats or {}).get("season_number")
            or (item.get("_normalized") or {}).get("season_number")
            or item.get("ParentIndexNumber"),
            0,
        )
        if tmdb_total_seasons > 1 and current_season > 0:
            title = f"{title} {format_season_cn(current_season)}"

    lines = [
        f"📀 *新入库* {escape_markdown_v2(media_type)} {escape_markdown_v2(title)}",
        '',
        f"📚 *类型：* {escape_markdown_v2(media_type)}",
        f"⭐ *评分：* {escape_markdown_v2(rating)}",
        f"🎬 *TMDB ID：* `{escape_markdown_v2(tmdb_id)}`",
    ]
    if extra_lines:
        lines.extend(extra_lines)
    lines.append(f"🕐 *时间：* {escape_markdown_v2(added_time)}")
    return '\n'.join(lines)

def extract_emby_item_id(payload: Dict[str, Any]) -> Optional[str]:
    if isinstance(payload.get("Item"), dict) and payload["Item"].get("Id"):
        return str(payload["Item"]["Id"])

    if payload.get("ItemId"):
        return str(payload["ItemId"])

    if isinstance(payload.get("Items"), list) and payload["Items"]:
        first = payload["Items"][0]
        if isinstance(first, dict) and first.get("Id"):
            return str(first["Id"])

    return None


def extract_emby_item_fallback(payload: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(payload.get("Item"), dict):
        return payload["Item"]

    if isinstance(payload.get("Items"), list) and payload["Items"]:
        first = payload["Items"][0]
        if isinstance(first, dict):
            return first

    return {
        "Id": payload.get("Id") or payload.get("ItemId"),
        "Name": payload.get("Name") or payload.get("Title") or "新入库内容",
        "Type": payload.get("ItemType") or payload.get("Type") or "",
        "ProductionYear": payload.get("ProductionYear"),
        "DateCreated": payload.get("DateCreated") or payload.get("Date") or "",
        "ProviderIds": payload.get("ProviderIds") or {},
        "CommunityRating": payload.get("CommunityRating"),
        "CriticRating": payload.get("CriticRating"),
        "ParentIndexNumber": payload.get("ParentIndexNumber"),
        "IndexNumber": payload.get("IndexNumber"),
        "SeriesName": payload.get("SeriesName"),
        "SeriesId": payload.get("SeriesId"),
    }

def _verify_emby_webhook_secret():
    if not EMBY_WEBHOOK_SECRET:
        return True

    token = request.args.get("token", "").strip()
    secret = request.args.get("secret", "").strip()
    header_secret = request.headers.get("X-Emby-Secret", "").strip()

    if (
        token == EMBY_WEBHOOK_SECRET
        or secret == EMBY_WEBHOOK_SECRET
        or header_secret == EMBY_WEBHOOK_SECRET
    ):
        return True

    return jsonify({"ok": False, "error": "forbidden"}), 403


def _parse_emby_webhook_data() -> Dict[str, Any]:
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}

def format_season_episode_tag(season_number: Any, episode_number: Any) -> str:
    try:
        season = int(season_number) if season_number not in (None, "") else None
    except Exception:
        season = None

    try:
        episode = int(episode_number) if episode_number not in (None, "") else None
    except Exception:
        episode = None

    if season is not None and episode is not None:
        return f"S{season:02d}E{episode:02d}"
    if episode is not None:
        return f"E{episode:02d}"
    return ""


def normalize_emby_webhook_item(payload: Dict[str, Any]) -> Dict[str, Any]:
    item = payload.get("Item") or {}
    if not isinstance(item, dict):
        item = {}

    item_type = item.get("Type") or ""
    provider_ids = item.get("ProviderIds") or {}

    item_name = item.get("Name") or ""
    item_year = item.get("ProductionYear")
    tmdb_id = provider_ids.get("Tmdb") or provider_ids.get("tmdb")
    imdb_id = provider_ids.get("Imdb") or provider_ids.get("imdb")

    if item_type == "Episode":
        series_name = item.get("SeriesName") or item_name
        series_provider_ids = item.get("SeriesProviderIds") or {}
        series_tmdb_id = (
            series_provider_ids.get("Tmdb")
            or series_provider_ids.get("tmdb")
            or tmdb_id
        )

        if not item_year and item.get("PremiereDate"):
            item_year = item.get("PremiereDate", "")[:4]

        season_number = item.get("ParentIndexNumber") or item.get("SeasonNumber")
        episode_number = item.get("IndexNumber") or item.get("EpisodeNumber")
        episode_tag = format_season_episode_tag(season_number, episode_number)

        log_title = series_name
        if episode_tag:
            log_title = f"{series_name} {episode_tag}"

        return {
            "event_type": payload.get("Event", ""),
            "item_type": "Series",   # 保持原逻辑，别影响后面通知和去重
            "title": series_name,    # 保持原逻辑
            "year": item_year,
            "tmdb_id": series_tmdb_id,
            "imdb_id": imdb_id,
            "season_number": season_number,
            "episode_number": episode_number,
            "episode_tag": episode_tag,
            "episode_title": item_name,
            "log_title": log_title,
            "raw_item": item,
        }

    return {
        "event_type": payload.get("Event", ""),
        "item_type": item_type,
        "title": item_name,
        "year": item_year,
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "season_number": None,
        "episode_number": None,
        "episode_tag": "",
        "episode_title": item_name,
        "log_title": item_name,
        "raw_item": item,
    }


def enrich_emby_item_from_api(normalized: Dict[str, Any]) -> Dict[str, Any]:
    raw_item = normalized.get("raw_item") or {}
    item_id = raw_item.get("Id")

    api_item = emby_get_item_by_id(str(item_id)) if item_id else None
    if api_item:
        merged = dict(api_item)
        merged["_normalized"] = normalized
        return merged

    fallback = dict(raw_item)
    fallback["_normalized"] = normalized
    return fallback

def tg_api(method: str, payload: Optional[Dict[str, Any]] = None, timeout: Any = None) -> Any:
    url = f"{TELEGRAM_BASE}/{method}"

    if timeout is None:
        if method == "getUpdates":
            timeout = (10, POLL_TIMEOUT + 15)
        else:
            timeout = REQUEST_TIMEOUT

    resp = session.post(url, json=payload or {}, timeout=timeout)

    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        raise RuntimeError(f"Telegram API 返回非 JSON：HTTP {resp.status_code} {resp.text}")

    if resp.status_code >= 400:
        raise RuntimeError(f"Telegram API HTTP {resp.status_code}: {data}")

    if not data.get("ok"):
        raise RuntimeError(f"Telegram API 错误: {data}")

    return data["result"]


def tmdb_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{TMDB_BASE}{path}"
    params = params or {}
    params["api_key"] = TMDB_API_KEY

    headers = {
        "Accept": "application/json",
    }

    resp = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()

def search_tmdb_by_title_for_notify(title: str, item_type: str, year: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    title = (title or "").strip()
    if not title:
        return None

    if str(item_type).lower() in ("series", "tv", "episode"):
        path = "/search/tv"
        name_key = "name"
        date_key = "first_air_date"
        media_type = "tv"
    else:
        path = "/search/movie"
        name_key = "title"
        date_key = "release_date"
        media_type = "movie"

    params: Dict[str, Any] = {
        "query": title,
        "language": TMDB_LANGUAGE,
        "page": 1,
        "include_adult": "false",
    }

    data = tmdb_get(path, params=params)
    results = data.get("results") or []
    if not results:
        return None

    target_year = str(year)[:4] if year not in (None, "", "0", 0) else ""

    best = None
    for r in results:
        r_title = (r.get(name_key) or "").strip()
        r_year = (r.get(date_key) or "")[:4]

        if target_year and r_year == target_year:
            best = r
            break

        if r_title == title:
            best = r
            break

    if best is None:
        best = results[0]

    return {
        "id": best.get("id"),
        "media_type": media_type,
        "title": best.get(name_key) or title,
        "year": (best.get(date_key) or "")[:4],
        "rating": best.get("vote_average"),
    }


def enrich_item_with_tmdb(item: Dict[str, Any]) -> Dict[str, Any]:
    normalized = item.get("_normalized") or {}

    tmdb_id = (
        normalized.get("tmdb_id")
        or (item.get("ProviderIds") or {}).get("Tmdb")
        or (item.get("ProviderIds") or {}).get("tmdb")
    )

    item_type = normalized.get("item_type") or item.get("Type") or ""
    title = normalized.get("title") or item.get("SeriesName") or item.get("Name") or ""
    year = normalized.get("year") or item.get("ProductionYear")

    media_type = "tv" if str(item_type).lower() in ("series", "tv", "episode") else "movie"

    # 已有 TMDB ID：继续去 TMDB 拉详情，补 rating / title / year
    if tmdb_id:
        item["_tmdb_id"] = str(tmdb_id)
        item["_tmdb_media_type"] = media_type

        try:
            tmdb_detail = get_tmdb_detail_for_notify(media_type, tmdb_id)
            if tmdb_detail:
                item["_tmdb_name"] = tmdb_detail["title"]
                item["_tmdb_year"] = tmdb_detail["year"]
                item["_tmdb_rating"] = tmdb_detail["rating"]
                item["_tmdb_number_of_seasons"] = _safe_int(tmdb_detail.get("number_of_seasons"), 0)
        except Exception:
            logging.exception("TMDB 详情补全失败（已有 TMDB ID）")

        return item

    # 没有 TMDB ID：按标题搜索补全
    try:
        tmdb_result = search_tmdb_by_title_for_notify(title, str(item_type), year)
        if tmdb_result:
            item["_tmdb_id"] = str(tmdb_result["id"])
            item["_tmdb_media_type"] = tmdb_result["media_type"]
            item["_tmdb_name"] = tmdb_result["title"]
            item["_tmdb_year"] = tmdb_result["year"]
            item["_tmdb_rating"] = tmdb_result["rating"]

            try:
                tmdb_detail = get_tmdb_detail_for_notify(tmdb_result["media_type"], tmdb_result["id"])
                if tmdb_detail:
                    item["_tmdb_number_of_seasons"] = _safe_int(tmdb_detail.get("number_of_seasons"), 0)
            except Exception:
                logging.exception("TMDB 季数补全失败")
    except Exception:
        logging.exception("TMDB 补全失败")

    return item

def get_tmdb_detail_for_notify(media_type: str, tmdb_id: Any) -> Optional[Dict[str, Any]]:
    try:
        tmdb_id = int(tmdb_id)
    except (TypeError, ValueError):
        return None

    try:
        detail = get_detail_with_fallback(media_type, tmdb_id)
    except Exception:
        logging.exception("按 TMDB ID 获取详情失败")
        return None

    if media_type == "movie":
        title = detail.get("title") or detail.get("original_title") or "未知标题"
        year = (detail.get("release_date") or "")[:4]
        number_of_seasons = 0
    else:
        title = detail.get("name") or detail.get("original_name") or "未知标题"
        year = (detail.get("first_air_date") or "")[:4]
        number_of_seasons = _safe_int(detail.get("number_of_seasons"), 0)

    return {
        "id": tmdb_id,
        "media_type": media_type,
        "title": title,
        "year": year,
        "rating": detail.get("vote_average"),
        "number_of_seasons": number_of_seasons,
    }

def get_tmdb_season_detail_for_notify(tv_tmdb_id: Any, season_number: int) -> Optional[Dict[str, Any]]:
    try:
        tv_tmdb_id = int(tv_tmdb_id)
        season_number = int(season_number)
    except (TypeError, ValueError):
        return None

    try:
        detail = tmdb_get(
            f"/tv/{tv_tmdb_id}/season/{season_number}",
            {"language": TMDB_LANGUAGE},
        )
    except Exception:
        logging.exception("按 TMDB 获取季详情失败：tv=%s season=%s", tv_tmdb_id, season_number)
        return None

    episodes = detail.get("episodes") or []
    aired_numbers: List[int] = []
    unaired_numbers: List[int] = []

    today = datetime.now().date()

    for ep in episodes:
        ep_num = _safe_int(ep.get("episode_number"), 0)
        if ep_num <= 0:
            continue

        air_date_raw = (ep.get("air_date") or "").strip()
        air_date = None
        if air_date_raw:
            try:
                air_date = datetime.strptime(air_date_raw, "%Y-%m-%d").date()
            except Exception:
                air_date = None

        if air_date and air_date <= today:
            aired_numbers.append(ep_num)
        else:
            unaired_numbers.append(ep_num)

    return {
        "season_number": season_number,
        "total_count": len({x for x in aired_numbers + unaired_numbers if x > 0}),
        "aired_numbers": sorted(set(aired_numbers)),
        "unaired_numbers": sorted(set(unaired_numbers)),
    }

def send_message(
    chat_id: int,
    text: str,
    parse_mode: Optional[str] = None,
    reply_markup: Optional[Dict[str, Any]] = None,
    disable_web_page_preview: bool = True,
) -> None:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    tg_api("sendMessage", payload)


def answer_callback(callback_query_id: str, text: str, show_alert: bool = False) -> None:
    tg_api("answerCallbackQuery", {
        "callback_query_id": callback_query_id,
        "text": text,
        "show_alert": show_alert,
    })


def get_owner_chat_id_or_none() -> Optional[int]:
    if not OWNER_CHAT_ID:
        return None
    try:
        return int(OWNER_CHAT_ID)
    except ValueError:
        logging.warning("OWNER_CHAT_ID 不是合法数字，已忽略")
        return None

def is_owner_user(user_id: int) -> bool:
    owner_chat_id = get_owner_chat_id_or_none()
    return owner_chat_id is not None and user_id == owner_chat_id


def escape_markdown_v2(text: Any) -> str:
    s = str(text or "")
    s = s.replace("\\", "\\\\")
    return re.sub(r"([_*\[\]()~`>#+\-=|{}.!])", r"\\\1", s)


def shorten_text(text: str, max_len: int = 220) -> str:
    text = (text or "").replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return "暂无简介"
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def get_requester_name(message: Dict[str, Any]) -> str:
    user = message.get("from", {}) or {}
    username = user.get("username")

    if username:
        return f"@{username}"

    first_name = user.get("first_name") or ""
    last_name = user.get("last_name") or ""
    full_name = (first_name + " " + last_name).strip()

    if full_name:
        return full_name

    return str(user.get("id", "未知用户"))


def search_tmdb(query: str) -> List[Dict[str, Any]]:
    data = tmdb_get("/search/multi", {
        "query": query,
        "language": TMDB_LANGUAGE,
        "include_adult": "false",
        "page": 1,
    })

    results: List[Dict[str, Any]] = []
    for item in data.get("results", []):
        media_type = item.get("media_type")
        if media_type not in ("movie", "tv"):
            continue
        results.append(item)
        if len(results) >= RESULT_LIMIT:
            break

    return results


def get_detail(media_type: str, tmdb_id: int, language: Optional[str] = None) -> Dict[str, Any]:
    path = f"/movie/{tmdb_id}" if media_type == "movie" else f"/tv/{tmdb_id}"
    params: Dict[str, Any] = {}
    if language:
        params["language"] = language
    return tmdb_get(path, params=params)


def get_detail_with_fallback(media_type: str, tmdb_id: int) -> Dict[str, Any]:
    detail = get_detail(media_type, tmdb_id, TMDB_LANGUAGE)

    if detail.get("overview"):
        return detail

    try:
        fallback = get_detail(media_type, tmdb_id, "en-US")
        if fallback.get("overview"):
            merged = dict(fallback)
            merged.update({k: v for k, v in detail.items() if v not in (None, "", [], {})})
            return merged
    except Exception:
        pass

    return detail


def get_media_label(media_type: str) -> str:
    return "🎬电影" if media_type == "movie" else "📺️剧集"


def get_display_name(
    detail: Dict[str, Any],
    media_type: str,
    season_number: Optional[int] = None,
) -> str:
    if media_type == "movie":
        name = detail.get("title") or detail.get("original_title") or "未知标题"
        date_str = detail.get("release_date") or ""
    else:
        name = detail.get("name") or detail.get("original_name") or "未知标题"
        date_str = detail.get("first_air_date") or ""

    year = date_str[:4] if date_str else ""
    base = f"{name} ({year})" if year else name

    season_number = _safe_int(season_number, 0)
    if media_type == "tv" and season_number > 0 and _safe_int(detail.get("number_of_seasons"), 0) > 1:
        return f"{base} {format_season_cn(season_number)}"

    return base


def get_genres(detail: Dict[str, Any]) -> str:
    genres = detail.get("genres") or []
    names = [g.get("name") for g in genres if g.get("name")]
    return " / ".join(names) if names else "未知"


def get_scope_display(
    detail: Dict[str, Any],
    media_type: str,
    season_number: Optional[int] = None,
) -> str:
    if media_type == "movie":
        runtime = detail.get("runtime")
        if runtime:
            return f"全片（{runtime}分钟）"
        return "全片"

    season_number = _safe_int(season_number, 0)
    if season_number > 0:
        for s in (detail.get("seasons") or []):
            n = _safe_int(s.get("season_number"), 0)
            if n != season_number:
                continue
            episode_count = _safe_int(s.get("episode_count"), 0)
            if episode_count > 0:
                return f"{format_season_cn(season_number)}（共{episode_count}集）"
            return format_season_cn(season_number)

        return format_season_cn(season_number)

    seasons = detail.get("number_of_seasons")
    episodes = detail.get("number_of_episodes")

    if seasons:
        return f"全剧（{seasons}季）"
    if episodes:
        return f"全剧（共{episodes}集）"
    return "全剧"


def get_tmdb_page_url(media_type: str, tmdb_id: int) -> str:
    kind = "movie" if media_type == "movie" else "tv"
    return f"https://www.themoviedb.org/{kind}/{tmdb_id}"


def build_tmdb_cover_url(detail: Dict[str, Any]) -> Optional[str]:
    poster_path = detail.get("poster_path")
    if poster_path:
        return f"https://image.tmdb.org/t/p/w500{poster_path}"

    backdrop_path = detail.get("backdrop_path")
    if backdrop_path:
        return f"https://image.tmdb.org/t/p/w780{backdrop_path}"

    return None


def build_admin_caption(
    requester_name: str,
    media_type: str,
    detail: Dict[str, Any],
    season_number: Optional[int] = None,
) -> str:
    title = get_display_name(detail, media_type, season_number=season_number)
    media_label = get_media_label(media_type)
    genres = get_genres(detail)
    scope = get_scope_display(detail, media_type, season_number=season_number)
    tmdb_id = detail.get("id")
    overview = shorten_text(detail.get("overview") or "", 220)
    tmdb_url = get_tmdb_page_url(media_type, tmdb_id)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return (
        "📋 *新求片待审核*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 *用户：* {escape_markdown_v2(requester_name)}\n"
        f"🎬 *影片：* {escape_markdown_v2(title)}\n"
        f"📁 *类型：* {escape_markdown_v2(media_label)}\n"
        f"🏷 *分类：* {escape_markdown_v2(genres)}\n"
        f"📄 *范围：* {escape_markdown_v2(scope)}\n"
        f"🆔 *TMDB：* `{tmdb_id}`\n"
        f"📝 *简介：* {escape_markdown_v2(overview)}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔗 [查看 TMDB 详情]({tmdb_url})\n"
        f"⏰ *求片时间：* {escape_markdown_v2(now_str)}"
    )


def build_user_result_text(
    action: str,
    media_type: str,
    detail: Dict[str, Any],
    remaining_quota: Optional[int] = None,
    season_number: Optional[int] = None,
) -> str:
    title = get_display_name(detail, media_type, season_number=season_number)
    media_label = get_media_label(media_type)
    genres = get_genres(detail)
    scope = get_scope_display(detail, media_type, season_number=season_number)

    status_line = "✅ *你的求片已批准*" if action == "approve" else "❌ *你的求片未通过*"

    extra = ""
    if remaining_quota is not None:
        extra = f"\n💳 *剩余额度：* {remaining_quota}"

    return (
        f"{status_line}\n\n"
        f"🎬 *影片名：* {escape_markdown_v2(title)}\n"
        f"📁 *类型：* {escape_markdown_v2(media_label)}\n"
        f"🏷 *分类：* {escape_markdown_v2(genres)}\n"
        f"📄 *范围：* {escape_markdown_v2(scope)}"
        f"{extra}"
    )


def notify_requester_result(
    requester_chat_id: int,
    action: str,
    media_type: str,
    detail: Dict[str, Any],
    remaining_quota: Optional[int] = None,
    season_number: Optional[int] = None,
) -> None:
    text = build_user_result_text(
        action,
        media_type,
        detail,
        remaining_quota=remaining_quota,
        season_number=season_number,
    )
    send_message(
        requester_chat_id,
        text,
        parse_mode="MarkdownV2",
        disable_web_page_preview=False,
    )


def build_review_keyboard(request_id: str) -> Dict[str, Any]:
    approve_data = f"a|{request_id}"
    reject_data = f"r|{request_id}"

    return {
        "inline_keyboard": [[
            {"text": "✅ 批准", "callback_data": approve_data},
            {"text": "❌ 拒绝", "callback_data": reject_data},
        ]]
    }
    
def build_user_select_keyboard(
    results: List[Dict[str, Any]],
    requester_user_id: int,
    reply_chat_id: int,
) -> Dict[str, Any]:
    rows = []

    for item in results:
        media_type = item["media_type"]
        tmdb_id = int(item["id"])

        if media_type == "movie":
            title = item.get("title") or item.get("original_title") or "未知标题"
            date_str = item.get("release_date") or ""
        else:
            title = item.get("name") or item.get("original_name") or "未知标题"
            date_str = item.get("first_air_date") or ""

        year = date_str[:4] if date_str else ""
        media_label = "🎬电影" if media_type == "movie" else "📺️剧集"
        button_text = f"{title} {f'({year})' if year else ''} · {media_label}"

        callback_data = f"s|{media_type}|{tmdb_id}|{requester_user_id}|{reply_chat_id}"
        rows.append([{"text": button_text[:60], "callback_data": callback_data}])

    return {"inline_keyboard": rows}

def build_user_choice_text(query: str, results: List[Dict[str, Any]]) -> str:
    lines = [
        "🎬 *请先选择你要请求的版本*",
        "━━━━━━━━━━━━━━",
        "",
        f"*你的搜索：* {escape_markdown_v2(query)}",
        "",
        "请点击下方按钮选择正确的影片或剧集：",
    ]
    return "\n".join(lines)

def edit_message_text(
    chat_id: int,
    message_id: int,
    text: str,
    parse_mode: Optional[str] = None,
    reply_markup: Optional[Dict[str, Any]] = None,
    disable_web_page_preview: bool = True,
) -> None:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    tg_api("editMessageText", payload)


def build_tv_season_choice_text(detail: Dict[str, Any]) -> str:
    title = get_display_name(detail, "tv")
    total_seasons = _safe_int(detail.get("number_of_seasons"), 0)

    lines = [
        "📺 *该版本包含多季，请继续选择第几季*",
        "━━━━━━━━━━━━━━",
        "",
        f"*剧集：* {escape_markdown_v2(title)}",
    ]

    if total_seasons > 0:
        lines.append(f"*总季数：* {total_seasons}")

    lines.extend([
        "",
        "请点击下方按钮选择要请求第几季：",
    ])
    return "\n".join(lines)


def build_tv_season_select_keyboard(
    detail: Dict[str, Any],
    requester_user_id: int,
    reply_chat_id: int,
) -> Dict[str, Any]:
    tmdb_id = _safe_int(detail.get("id"), 0)
    rows: List[List[Dict[str, str]]] = []

    valid_seasons: List[Dict[str, int]] = []
    seen: Set[int] = set()

    for s in (detail.get("seasons") or []):
        season_number = _safe_int(s.get("season_number"), 0)
        if season_number <= 0 or season_number in seen:
            continue
        seen.add(season_number)
        valid_seasons.append({
            "season_number": season_number,
            "episode_count": _safe_int(s.get("episode_count"), 0),
        })

    if not valid_seasons:
        total_seasons = _safe_int(detail.get("number_of_seasons"), 0)
        for n in range(1, total_seasons + 1):
            valid_seasons.append({
                "season_number": n,
                "episode_count": 0,
            })

    valid_seasons.sort(key=lambda x: x["season_number"])

    for item in valid_seasons:
        season_number = item["season_number"]
        episode_count = item["episode_count"]

        text = format_season_cn(season_number)
        if episode_count > 0:
            text = f"{text}（{episode_count}集）"

        callback_data = f"ss|{tmdb_id}|{season_number}|{requester_user_id}|{reply_chat_id}"
        rows.append([{"text": text[:60], "callback_data": callback_data}])

    return {"inline_keyboard": rows}


def submit_user_request(
    callback_message: Dict[str, Any],
    requester_user_id: int,
    requester_chat_id: int,
    requester_name: str,
    media_type: str,
    detail: Dict[str, Any],
    season_number: Optional[int] = None,
) -> None:
    tmdb_id = _safe_int(detail.get("id"), 0)
    if not tmdb_id:
        send_message(requester_chat_id, "❌ TMDB 数据无效，无法提交。")
        return

    season_number = _safe_int(season_number, 0) or None

    if media_type == "tv":
        total_seasons = _safe_int(detail.get("number_of_seasons"), 0)
        if total_seasons > 1 and not season_number:
            send_message(
                requester_chat_id,
                "❌ 多季电视剧请先选择季数后再提交。"
            )
            return

    try:
        existing = None

        if media_type == "movie":
            existing = emby_find_by_tmdb("movie", tmdb_id)
        elif media_type == "tv":
            existing = emby_find_tv_season_by_tmdb(tmdb_id, season_number)

    except Exception:
        logging.exception("Emby 按季/按片查重失败")
        send_message(
            requester_chat_id,
            "❌ Emby 查库失败，请稍后重试。"
        )
        return

    if existing:
        remove_review_keyboard(callback_message)
        notify_user_emby_exists(requester_chat_id, existing)
        return

    admin_ids = parse_admin_chat_ids()
    if not admin_ids:
        send_message(
            requester_chat_id,
            "❌ 当前未配置管理员，无法提交审核。"
        )
        return

    request_id = create_request_id(
        requester_chat_id,
        media_type,
        tmdb_id,
        season_number=season_number,
    )

    admin_refs = send_request_cards_to_admins(
        request_id=request_id,
        requester_name=requester_name,
        media_type=media_type,
        detail=detail,
        season_number=season_number,
    )

    if not admin_refs:
        send_message(
            requester_chat_id,
            "❌ 审核消息发送失败，未成功发送给任何管理员。"
        )
        return

    save_pending_request(
        request_id=request_id,
        requester_user_id=requester_user_id,
        requester_chat_id=requester_chat_id,
        media_type=media_type,
        tmdb_id=tmdb_id,
        requester_name=requester_name,
        admin_message_refs=admin_refs,
    )

    remove_review_keyboard(callback_message)

    title = get_display_name(detail, media_type, season_number=season_number)
    send_message(
        requester_chat_id,
        f"✅ 你已选择：*{escape_markdown_v2(title)}*\n已发送给管理员审核。",
        parse_mode="MarkdownV2",
    )

def send_search_choices_to_user(
    chat_id: int,
    requester_user_id: int,
    query: str,
    results: List[Dict[str, Any]],
) -> None:
    text = build_user_choice_text(query, results)
    reply_markup = build_user_select_keyboard(results, requester_user_id, chat_id)

    send_message(
        chat_id,
        text,
        parse_mode="MarkdownV2",
        reply_markup=reply_markup,
        disable_web_page_preview=True,
    )

def send_request_card(
    target_chat_id: int,
    request_id: str,
    requester_name: str,
    media_type: str,
    detail: Dict[str, Any],
    season_number: Optional[int] = None,
) -> Optional[Dict[str, int]]:
    cover_url = build_tmdb_cover_url(detail)
    caption = build_admin_caption(
        requester_name,
        media_type,
        detail,
        season_number=season_number,
    )
    reply_markup = build_review_keyboard(request_id)

    try:
        if cover_url:
            try:
                result = tg_api("sendPhoto", {
                    "chat_id": target_chat_id,
                    "photo": cover_url,
                    "caption": caption,
                    "parse_mode": "MarkdownV2",
                    "reply_markup": reply_markup,
                })
            except Exception:
                logging.exception("发送审核图片失败，改为纯文本发送，chat_id=%s", target_chat_id)
                result = tg_api("sendMessage", {
                    "chat_id": target_chat_id,
                    "text": caption,
                    "parse_mode": "MarkdownV2",
                    "reply_markup": reply_markup,
                    "disable_web_page_preview": False,
                })
        else:
            result = tg_api("sendMessage", {
                "chat_id": target_chat_id,
                "text": caption,
                "parse_mode": "MarkdownV2",
                "reply_markup": reply_markup,
                "disable_web_page_preview": False,
            })

        return {
            "chat_id": int(result["chat"]["id"]),
            "message_id": int(result["message_id"]),
        }
    except Exception:
        logging.exception("发送审核卡片失败，chat_id=%s", target_chat_id)
        return None

def send_request_cards_to_admins(
    request_id: str,
    requester_name: str,
    media_type: str,
    detail: Dict[str, Any],
    season_number: Optional[int] = None,
) -> List[Dict[str, int]]:
    refs: List[Dict[str, int]] = []
    admin_ids = parse_admin_chat_ids()

    for admin_chat_id in admin_ids:
        ref = send_request_card(
            target_chat_id=admin_chat_id,
            request_id=request_id,
            requester_name=requester_name,
            media_type=media_type,
            detail=detail,
            season_number=season_number,
        )
        if ref:
            refs.append(ref)

    return refs

def remove_review_keyboard(callback_message: Dict[str, Any]) -> None:
    chat = callback_message.get("chat", {}) or {}
    message_id = callback_message.get("message_id")

    if not chat or not message_id:
        return

    try:
        tg_api("editMessageReplyMarkup", {
            "chat_id": chat["id"],
            "message_id": message_id,
            "reply_markup": {"inline_keyboard": []},
        })
    except Exception as e:
        logging.warning(
            "移除用户选择消息按钮失败，已忽略：chat_id=%s message_id=%s err=%s",
            chat.get("id"),
            message_id,
            e,
        )

def remove_review_keyboard_by_ref(chat_id: int, message_id: int) -> None:
    try:
        tg_api("editMessageReplyMarkup", {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": {"inline_keyboard": []},
        })
    except Exception as e:
        logging.warning(
            "移除管理员审核按钮失败，已忽略：chat_id=%s message_id=%s err=%s",
            chat_id,
            message_id,
            e,
        )

def remove_all_admin_review_keyboards(request_obj: Dict[str, Any]) -> None:
    refs = request_obj.get("admin_message_refs") or []
    for ref in refs:
        try:
            remove_review_keyboard_by_ref(
                chat_id=int(ref["chat_id"]),
                message_id=int(ref["message_id"]),
            )
        except Exception:
            logging.exception("移除管理员按钮失败：%s", ref)


def handle_search(message: Dict[str, Any], query: str) -> None:
    reply_chat_id = int(message["chat"]["id"])
    user = message.get("from", {}) or {}
    requester_user_id = _safe_int(user.get("id"), 0)

    record = upsert_user_profile(user, reply_chat_id)
    remaining_quota = calc_remaining_quota(record)

    if remaining_quota <= 0:
        send_message(
            reply_chat_id,
            "❌ 你的求片额度已用完，暂时不能继续求片。\n发送 /cx 查看详情。"
        )
        return

    results = search_tmdb(query)

    if not results:
        send_message(
            reply_chat_id,
            f"❌ 没有找到和 *{escape_markdown_v2(query)}* 相关的电影或剧集，请换个关键词试试。",
            parse_mode="MarkdownV2",
        )
        return

    send_search_choices_to_user(reply_chat_id, requester_user_id, query, results)

def handle_callback_query(callback: Dict[str, Any]) -> None:
    callback_id = callback.get("id")
    from_user = callback.get("from", {}) or {}
    from_user_id = int(from_user.get("id", 0))
    data = (callback.get("data") or "").strip()
    callback_message = callback.get("message", {}) or {}
    callback_chat_id = (callback_message.get("chat", {}) or {}).get("id")

    upsert_user_profile(from_user, callback_chat_id)

    action_code = data.split("|", 1)[0] if data else ""

    # 第一步：用户先选“版本”
    # s|media_type|tmdb_id|requester_user_id|reply_chat_id
    if action_code == "s":
        try:
            _, media_type, tmdb_id_str, requester_user_id_str, reply_chat_id_str = data.split("|", 4)
            tmdb_id = int(tmdb_id_str)
            requester_user_id = int(requester_user_id_str)
            requester_chat_id = int(reply_chat_id_str)
        except Exception:
            answer_callback(callback_id, "按钮数据无效", show_alert=True)
            return

        if from_user_id != requester_user_id:
            answer_callback(callback_id, "这不是你的按钮", show_alert=True)
            return

        quota_record = get_user_record(from_user_id)
        remaining_quota = calc_remaining_quota(quota_record)
        if remaining_quota <= 0:
            answer_callback(callback_id, "你的求片额度已用完", show_alert=True)
            send_message(
                requester_chat_id,
                "❌ 你的求片额度已用完，无法继续提交求片。\n发送 /cx 查看剩余额度。"
            )
            return

        try:
            detail = get_detail_with_fallback(media_type, tmdb_id)
        except Exception as e:
            logging.exception("获取 TMDB 详情失败")
            answer_callback(callback_id, "获取详情失败", show_alert=True)
            send_message(
                requester_chat_id,
                f"❌ 获取详情失败：{escape_markdown_v2(e)}",
                parse_mode="MarkdownV2",
            )
            return

        # 多季电视剧：继续让用户选季
        if media_type == "tv" and _safe_int(detail.get("number_of_seasons"), 0) > 1:
            try:
                edit_message_text(
                    chat_id=int(callback_message["chat"]["id"]),
                    message_id=int(callback_message["message_id"]),
                    text=build_tv_season_choice_text(detail),
                    parse_mode="MarkdownV2",
                    reply_markup=build_tv_season_select_keyboard(
                        detail,
                        requester_user_id=requester_user_id,
                        reply_chat_id=requester_chat_id,
                    ),
                    disable_web_page_preview=True,
                )
                answer_callback(callback_id, "请选择季数", show_alert=False)
            except Exception as e:
                logging.exception("切换到季选择界面失败")
                answer_callback(callback_id, "请选择季数失败", show_alert=True)
                send_message(
                    requester_chat_id,
                    f"❌ 选择季数失败：{escape_markdown_v2(e)}",
                    parse_mode="MarkdownV2",
                )
            return

        # 电影 / 单季剧：直接提交
        requester_name = (
            f"@{from_user.get('username')}"
            if from_user.get("username")
            else (((from_user.get("first_name") or "") + " " + (from_user.get("last_name") or "")).strip() or str(from_user_id))
        )

        answer_callback(callback_id, "正在提交审核...", show_alert=False)
        try:
            submit_user_request(
                callback_message=callback_message,
                requester_user_id=requester_user_id,
                requester_chat_id=requester_chat_id,
                requester_name=requester_name,
                media_type=media_type,
                detail=detail,
                season_number=None if media_type == "tv" else None,
            )
        except Exception as e:
            logging.exception("处理用户选片失败")
            send_message(
                requester_chat_id,
                f"❌ 提交失败：{escape_markdown_v2(e)}",
                parse_mode="MarkdownV2",
            )
        return

    # 第二步：用户选“季”
    # ss|tmdb_id|season_number|requester_user_id|reply_chat_id
    if action_code == "ss":
        try:
            _, tmdb_id_str, season_number_str, requester_user_id_str, reply_chat_id_str = data.split("|", 4)
            tmdb_id = int(tmdb_id_str)
            season_number = int(season_number_str)
            requester_user_id = int(requester_user_id_str)
            requester_chat_id = int(reply_chat_id_str)
        except Exception:
            answer_callback(callback_id, "按钮数据无效", show_alert=True)
            return

        if from_user_id != requester_user_id:
            answer_callback(callback_id, "这不是你的按钮", show_alert=True)
            return

        quota_record = get_user_record(from_user_id)
        remaining_quota = calc_remaining_quota(quota_record)
        if remaining_quota <= 0:
            answer_callback(callback_id, "你的求片额度已用完", show_alert=True)
            send_message(
                requester_chat_id,
                "❌ 你的求片额度已用完，无法继续提交求片。\n发送 /cx 查看剩余额度。"
            )
            return

        requester_name = (
            f"@{from_user.get('username')}"
            if from_user.get("username")
            else (((from_user.get("first_name") or "") + " " + (from_user.get("last_name") or "")).strip() or str(from_user_id))
        )

        answer_callback(callback_id, "正在提交审核...", show_alert=False)

        try:
            detail = get_detail_with_fallback("tv", tmdb_id)

            submit_user_request(
                callback_message=callback_message,
                requester_user_id=requester_user_id,
                requester_chat_id=requester_chat_id,
                requester_name=requester_name,
                media_type="tv",
                detail=detail,
                season_number=season_number,
            )
        except Exception as e:
            logging.exception("处理用户选季失败")
            send_message(
                requester_chat_id,
                f"❌ 提交失败：{escape_markdown_v2(e)}",
                parse_mode="MarkdownV2",
            )
        return

    if action_code not in ("a", "r"):
        answer_callback(callback_id, "未知操作", show_alert=True)
        return

    # 管理员审核按钮：a|request_id / r|request_id
    if not is_admin_user(from_user_id):
        answer_callback(callback_id, "只有管理员可以操作", show_alert=True)
        return

    try:
        _, request_id = data.split("|", 1)
    except Exception:
        answer_callback(callback_id, "按钮数据无效", show_alert=True)
        return

    action = "approve" if action_code == "a" else "reject"

    req = claim_pending_request(request_id, action, from_user_id)
    if not req:
        answer_callback(callback_id, "该求片请求不存在、已过期，或已被其他管理员处理", show_alert=True)
        try:
            remove_review_keyboard(callback_message)
        except Exception:
            pass
        return

    requester_user_id = _safe_int(req.get("requester_user_id"), 0)
    quota_consumed = False
    user_notified = False

    try:
        media_type = req["media_type"]
        tmdb_id = int(req["tmdb_id"])
        requester_chat_id = int(req["requester_chat_id"])

        request_meta = parse_request_id_meta(request_id)
        season_number = _safe_int(request_meta.get("season_number"), 0) or None

        detail = get_detail_with_fallback(media_type, tmdb_id)

        remaining_quota = None
        if action == "approve":
            quota_record = consume_user_quota(requester_user_id)
            quota_consumed = True
            remaining_quota = calc_remaining_quota(quota_record)

        notify_requester_result(
            requester_chat_id,
            action,
            media_type,
            detail,
            remaining_quota=remaining_quota,
            season_number=season_number,
        )
        user_notified = True

        try:
            remove_all_admin_review_keyboards(req)
        except Exception:
            logging.exception("移除管理员按钮失败：request_id=%s", request_id)

        try:
            delete_pending_request(request_id)
        except Exception:
            logging.exception("删除待审核记录失败：request_id=%s", request_id)

        answer_callback(
            callback_id,
            "已通知求片用户" if action == "approve" else "已将拒绝结果通知用户",
            show_alert=False,
        )
    except Exception as e:
        logging.exception("处理管理员回调失败：request_id=%s", request_id)

        if action == "approve" and quota_consumed and requester_user_id > 0 and not user_notified:
            try:
                rollback_user_quota(requester_user_id)
            except Exception:
                logging.exception("回滚用户额度失败：user_id=%s", requester_user_id)

        if not user_notified:
            try:
                restore_pending_request(request_id, req)
            except Exception:
                logging.exception("恢复待审核记录失败：request_id=%s", request_id)

        answer_callback(callback_id, f"操作失败：{e}", show_alert=True)

def parse_command(text: str):
    text = (text or "").strip()
    if not text:
        return None, ""

    if not text.startswith("/"):
        return None, ""

    parts = text.split(maxsplit=1)
    cmd = parts[0].split("@", 1)[0].lower()   # 兼容 /qp@你的bot
    args = parts[1].strip() if len(parts) > 1 else ""
    return cmd, args


def is_group_chat(message: Dict[str, Any]) -> bool:
    chat_type = ((message.get("chat") or {}).get("type") or "").lower()
    return chat_type in ("group", "supergroup")

def handle_setquota(message: Dict[str, Any], args: str) -> None:
    chat_id = int(message["chat"]["id"])
    from_user = message.get("from", {}) or {}
    from_user_id = _safe_int(from_user.get("id"), 0)

    if not is_owner_user(from_user_id):
        send_message(chat_id, "❌ 只有机器人创建者可以使用这个命令。")
        return

    parts = args.split()
    if len(parts) != 2:
        send_message(chat_id, "用法：/ed 用户TGID 总额度\n例如：/ed 111 20")
        return

    target_user_id = _safe_int(parts[0], 0)
    new_quota = _safe_int(parts[1], -1)

    if target_user_id <= 0 or new_quota < 0:
        send_message(chat_id, "❌ 参数无效。用法：/ed 用户TGID 总额度")
        return

    record = set_user_quota(target_user_id, new_quota)
    if not record:
        send_message(chat_id, "❌ 设置失败，请检查参数。")
        return

    success_count = _safe_int(record.get("success_count"), 0)
    remaining_quota = calc_remaining_quota(record)
    username = record.get("username") or ""
    display_name = f"@{username}" if username else str(target_user_id)

    send_message(
        chat_id,
        "✅ 已更新用户额度\n\n"
        f"用户：{display_name}\n"
        f"TG ID：{target_user_id}\n"
        f"总额度：{new_quota}\n"
        f"已成功求片：{success_count}\n"
        f"剩余额度：{remaining_quota}"
    )

def handle_quota(message: Dict[str, Any]) -> None:
    chat_id = int(message["chat"]["id"])
    user = message.get("from", {}) or {}

    record = upsert_user_profile(user, chat_id)
    quota_limit = _safe_int(record.get("quota_limit"), DEFAULT_QUOTA_LIMIT)
    success_count = _safe_int(record.get("success_count"), 0)
    remaining_quota = calc_remaining_quota(record)

    send_message(
        chat_id,
        "📊 你的求片额度信息\n\n"
        f"总额度：{quota_limit}\n"
        f"已成功求片：{success_count}\n"
        f"剩余额度：{remaining_quota}"
    )


def handle_message(message: Dict[str, Any]) -> None:
    chat_id = int(message["chat"]["id"])
    text = (message.get("text") or "").strip()
    if not text:
        return

    upsert_user_profile(message.get("from", {}) or {}, chat_id)

    command, args = parse_command(text)
    in_group = is_group_chat(message)

    if command == "/start":
        send_message(
            chat_id,
            "欢迎使用 Emby 求片 Bot\n\n"
            "用法：\n"
            "1. 私聊可直接发送片名，例如：沙丘\n"
            "2. 也可以发送 /qp 片名\n"
            "3. 发送 /id 查看你自己的 UID\n"
            "4. 发送 /cx 查看剩余求片次数\n",
        )
        return

    if command == "/help":
        send_message(
            chat_id,
            "命令说明：\n"
            "/start - 查看欢迎信息\n"
            "/help - 查看帮助\n"
            "/id - 查看你的 UID\n"
            "/cx - 查看剩余求片次数\n"
            "/qp 片名 - 求片\n\n",
        )
        return

    if command == "/id":
         from_user_id = _safe_int((message.get("from") or {}).get("id"), 0)
         send_message(chat_id, f"UID：{from_user_id}")
         return

    if command == "/cx":
        handle_quota(message)
        return

    if command == "/ed":
        handle_setquota(message, args)
        return

    if command == "/qp":
        if not args:
            send_message(chat_id, "请这样用：/qp 沙丘")
            return
        handle_search(message, args)
        return

    if command:
        return

    # 群组里：忽略普通消息，不当成片名
    if in_group:
        return

    # 私聊里：保留直接发片名就搜索
    handle_search(message, text)



SQLITE_DB_FILE = os.getenv("SQLITE_DB_FILE", "bot_data.sqlite3").strip() or "bot_data.sqlite3"
SQLITE_DB_PATH = os.path.join(DATA_DIR, SQLITE_DB_FILE)
DB_LOCK = Lock()
EMBY_NOTIFY_SETTLE_DELAY_SECONDS = int(os.getenv("EMBY_NOTIFY_SETTLE_DELAY_SECONDS", "30"))
EMBY_NOTIFY_SETTLE_POLL_SECONDS = int(os.getenv("EMBY_NOTIFY_SETTLE_POLL_SECONDS", "5"))
EMBY_NOTIFY_MAX_MISSING_FETCH = int(os.getenv("EMBY_NOTIFY_MAX_MISSING_FETCH", "500"))


def get_db_conn() -> sqlite3.Connection:
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(SQLITE_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_sqlite_db() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    tg_id INTEGER PRIMARY KEY,
                    chat_id INTEGER,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    display_name TEXT,
                    quota_limit INTEGER NOT NULL DEFAULT 3,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    remaining_quota INTEGER NOT NULL DEFAULT 3,
                    updated_at TEXT,
                    last_seen_at TEXT
                );

                CREATE TABLE IF NOT EXISTS pending_requests (
                    request_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    requester_user_id INTEGER NOT NULL,
                    requester_chat_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL,
                    tmdb_id INTEGER NOT NULL,
                    requester_name TEXT NOT NULL,
                    admin_message_refs TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT,
                    created_at_ts INTEGER,
                    handled_by INTEGER,
                    handled_at TEXT,
                    action TEXT
                );

                CREATE TABLE IF NOT EXISTS emby_settlement_jobs (
                    job_key TEXT PRIMARY KEY,
                    media_type TEXT NOT NULL,
                    title TEXT,
                    year TEXT,
                    tmdb_id TEXT,
                    representative_item_id TEXT,
                    representative_item_type TEXT,
                    series_id TEXT,
                    series_name TEXT,
                    season_number INTEGER,
                    season_item_id TEXT,
                    due_at_ts INTEGER NOT NULL,
                    last_event_ts INTEGER NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS emby_snapshots (
                    snapshot_key TEXT PRIMARY KEY,
                    media_type TEXT NOT NULL,
                    series_id TEXT,
                    season_number INTEGER,
                    episode_numbers_json TEXT NOT NULL DEFAULT '[]',
                    last_total_known INTEGER,
                    last_notified_at TEXT,
                    updated_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_pending_requests_status_created
                ON pending_requests(status, created_at_ts);

                CREATE INDEX IF NOT EXISTS idx_emby_settlement_due
                ON emby_settlement_jobs(due_at_ts);
                """
            )
            conn.commit()
        finally:
            conn.close()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(text_value: Optional[str], default: Any) -> Any:
    if not text_value:
        return default
    try:
        return json.loads(text_value)
    except Exception:
        return default


def _row_to_dict(row: Optional[sqlite3.Row]) -> Dict[str, Any]:
    if row is None:
        return {}
    data = dict(row)
    if 'admin_message_refs' in data:
        data['admin_message_refs'] = _json_loads(data.get('admin_message_refs'), [])
    return data


def migrate_json_to_sqlite() -> None:
    init_sqlite_db()

    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                users_data = json.load(f)
            if isinstance(users_data, dict) and users_data:
                with DB_LOCK:
                    conn = get_db_conn()
                    try:
                        for key, value in users_data.items():
                            if not isinstance(value, dict):
                                continue
                            tg_id = _safe_int(value.get('tg_id') or key, 0)
                            if not tg_id:
                                continue
                            quota_limit = _safe_int(value.get('quota_limit'), DEFAULT_QUOTA_LIMIT)
                            success_count = _safe_int(value.get('success_count'), 0)
                            remaining_quota = max(quota_limit - success_count, 0)
                            conn.execute(
                                """
                                INSERT INTO users (
                                    tg_id, chat_id, username, first_name, last_name, display_name,
                                    quota_limit, success_count, remaining_quota, updated_at, last_seen_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(tg_id) DO UPDATE SET
                                    chat_id=excluded.chat_id,
                                    username=excluded.username,
                                    first_name=excluded.first_name,
                                    last_name=excluded.last_name,
                                    display_name=excluded.display_name,
                                    quota_limit=excluded.quota_limit,
                                    success_count=excluded.success_count,
                                    remaining_quota=excluded.remaining_quota,
                                    updated_at=excluded.updated_at,
                                    last_seen_at=excluded.last_seen_at
                                """,
                                (
                                    tg_id,
                                    _safe_int(value.get('chat_id'), 0) or None,
                                    (value.get('username') or '').strip(),
                                    value.get('first_name') or '',
                                    value.get('last_name') or '',
                                    value.get('display_name') or '',
                                    quota_limit,
                                    success_count,
                                    remaining_quota,
                                    value.get('updated_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    value.get('last_seen_at') or value.get('updated_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                )
                            )
                        conn.commit()
                        logging.info('已将 users.json 迁移到 SQLite')
                    finally:
                        conn.close()
        except Exception:
            logging.exception('迁移 users.json 到 SQLite 失败')

    if os.path.exists(REQUESTS_FILE):
        try:
            with open(REQUESTS_FILE, 'r', encoding='utf-8') as f:
                req_data = json.load(f)
            if isinstance(req_data, dict) and req_data:
                with DB_LOCK:
                    conn = get_db_conn()
                    try:
                        for request_id, value in req_data.items():
                            if not isinstance(value, dict):
                                continue
                            conn.execute(
                                """
                                INSERT INTO pending_requests (
                                    request_id, status, requester_user_id, requester_chat_id, media_type, tmdb_id,
                                    requester_name, admin_message_refs, created_at, created_at_ts,
                                    handled_by, handled_at, action
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(request_id) DO UPDATE SET
                                    status=excluded.status,
                                    requester_user_id=excluded.requester_user_id,
                                    requester_chat_id=excluded.requester_chat_id,
                                    media_type=excluded.media_type,
                                    tmdb_id=excluded.tmdb_id,
                                    requester_name=excluded.requester_name,
                                    admin_message_refs=excluded.admin_message_refs,
                                    created_at=excluded.created_at,
                                    created_at_ts=excluded.created_at_ts,
                                    handled_by=excluded.handled_by,
                                    handled_at=excluded.handled_at,
                                    action=excluded.action
                                """,
                                (
                                    request_id,
                                    str(value.get('status') or 'pending'),
                                    _safe_int(value.get('requester_user_id'), 0),
                                    _safe_int(value.get('requester_chat_id'), 0),
                                    str(value.get('media_type') or ''),
                                    _safe_int(value.get('tmdb_id'), 0),
                                    value.get('requester_name') or '',
                                    _json_dumps(value.get('admin_message_refs') or []),
                                    value.get('created_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                    _safe_int(value.get('created_at_ts'), int(time.time())),
                                    _safe_int(value.get('handled_by'), 0) or None,
                                    value.get('handled_at'),
                                    value.get('action'),
                                )
                            )
                        conn.commit()
                        logging.info('已将 pending_requests.json 迁移到 SQLite')
                    finally:
                        conn.close()
        except Exception:
            logging.exception('迁移 pending_requests.json 到 SQLite 失败')


def _build_user_record(existing: Optional[Dict[str, Any]], tg_id: int, chat_id: Optional[int] = None) -> Dict[str, Any]:
    existing = dict(existing or {})

    quota_limit = _safe_int(existing.get('quota_limit'), DEFAULT_QUOTA_LIMIT)
    success_count = _safe_int(existing.get('success_count'), 0)
    effective_chat_id = chat_id if chat_id is not None else existing.get('chat_id')

    record: Dict[str, Any] = {}
    record['tg_id'] = tg_id
    if effective_chat_id is not None:
        record['chat_id'] = int(effective_chat_id)

    record['username'] = (existing.get('username') or '').strip()
    record['first_name'] = existing.get('first_name') or ''
    record['last_name'] = existing.get('last_name') or ''
    record['display_name'] = existing.get('display_name') or ''
    record['quota_limit'] = quota_limit
    record['success_count'] = success_count
    record['remaining_quota'] = max(quota_limit - success_count, 0)
    record['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    record['last_seen_at'] = existing.get('last_seen_at') or record['updated_at']
    return record


def _get_user_record_from_db(user_id: int) -> Dict[str, Any]:
    with DB_LOCK:
        conn = get_db_conn()
        try:
            row = conn.execute('SELECT * FROM users WHERE tg_id = ?', (user_id,)).fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()


def _upsert_user_record_to_db(record: Dict[str, Any]) -> Dict[str, Any]:
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute(
                """
                INSERT INTO users (
                    tg_id, chat_id, username, first_name, last_name, display_name,
                    quota_limit, success_count, remaining_quota, updated_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tg_id) DO UPDATE SET
                    chat_id=excluded.chat_id,
                    username=excluded.username,
                    first_name=excluded.first_name,
                    last_name=excluded.last_name,
                    display_name=excluded.display_name,
                    quota_limit=excluded.quota_limit,
                    success_count=excluded.success_count,
                    remaining_quota=excluded.remaining_quota,
                    updated_at=excluded.updated_at,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    _safe_int(record.get('tg_id'), 0),
                    (_safe_int(record.get('chat_id'), 0) or None),
                    (record.get('username') or '').strip(),
                    record.get('first_name') or '',
                    record.get('last_name') or '',
                    record.get('display_name') or '',
                    _safe_int(record.get('quota_limit'), DEFAULT_QUOTA_LIMIT),
                    _safe_int(record.get('success_count'), 0),
                    _safe_int(record.get('remaining_quota'), 0),
                    record.get('updated_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    record.get('last_seen_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                )
            )
            conn.commit()
            row = conn.execute(
                'SELECT * FROM users WHERE tg_id = ?',
                (_safe_int(record.get('tg_id'), 0),)
            ).fetchone()
            return dict(row) if row else record
        finally:
            conn.close()


def upsert_user_profile(user: Dict[str, Any], chat_id: Optional[int] = None) -> Dict[str, Any]:
    if not isinstance(user, dict):
        return {}

    tg_id = _safe_int(user.get('id'), 0)
    if not tg_id:
        return {}

    existing = _get_user_record_from_db(tg_id)
    record = _build_user_record(existing, tg_id, chat_id)

    username = (user.get('username') or '').strip()
    if username:
        record['username'] = username

    record['first_name'] = user.get('first_name') or record.get('first_name') or ''
    record['last_name'] = user.get('last_name') or record.get('last_name') or ''
    record['display_name'] = (
        f"@{record['username']}"
        if record.get('username')
        else ((f"{record.get('first_name', '')} {record.get('last_name', '')}".strip()) or str(tg_id))
    )
    record['last_seen_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    record['updated_at'] = record['last_seen_at']

    return _upsert_user_record_to_db(record)


def get_user_record(user_id: int) -> Dict[str, Any]:
    user_id = _safe_int(user_id, 0)
    if not user_id:
        return {}

    existing = _get_user_record_from_db(user_id)
    record = _build_user_record(existing, user_id, existing.get('chat_id'))
    if existing.get('display_name'):
        record['display_name'] = existing.get('display_name')
    if existing.get('last_seen_at'):
        record['last_seen_at'] = existing.get('last_seen_at')
    return _upsert_user_record_to_db(record)


def get_user_remaining_quota(user_id: int) -> int:
    record = get_user_record(user_id)
    return calc_remaining_quota(record)


def adjust_user_success_count(user_id: int, delta: int) -> Dict[str, Any]:
    user_id = _safe_int(user_id, 0)
    delta = _safe_int(delta, 0)

    if not user_id:
        return {}

    record = get_user_record(user_id)
    current_success_count = _safe_int(record.get('success_count'), 0)
    record['success_count'] = max(current_success_count + delta, 0)
    record['remaining_quota'] = calc_remaining_quota(record)
    record['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return _upsert_user_record_to_db(record)


def consume_user_quota(user_id: int) -> Dict[str, Any]:
    return adjust_user_success_count(user_id, 1)


def rollback_user_quota(user_id: int) -> Dict[str, Any]:
    return adjust_user_success_count(user_id, -1)


def set_user_quota(user_id: int, new_quota: int) -> Dict[str, Any]:
    user_id = _safe_int(user_id, 0)
    new_quota = _safe_int(new_quota, -1)

    if user_id <= 0 or new_quota < 0:
        return {}

    record = get_user_record(user_id)
    record['quota_limit'] = new_quota
    record['remaining_quota'] = calc_remaining_quota(record)
    record['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return _upsert_user_record_to_db(record)


def save_pending_request(
    request_id: str,
    requester_user_id: int,
    requester_chat_id: int,
    media_type: str,
    tmdb_id: int,
    requester_name: str,
    admin_message_refs: List[Dict[str, int]],
) -> None:
    now_ts = int(time.time())
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO pending_requests (
                    request_id, status, requester_user_id, requester_chat_id, media_type, tmdb_id,
                    requester_name, admin_message_refs, created_at, created_at_ts,
                    handled_by, handled_at, action
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    'pending',
                    requester_user_id,
                    requester_chat_id,
                    media_type,
                    tmdb_id,
                    requester_name,
                    _json_dumps(admin_message_refs or []),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    now_ts,
                    None,
                    None,
                    None,
                )
            )
            conn.commit()
        finally:
            conn.close()


def get_pending_request(request_id: str) -> Dict[str, Any]:
    with DB_LOCK:
        conn = get_db_conn()
        try:
            row = conn.execute(
                'SELECT * FROM pending_requests WHERE request_id = ?',
                (request_id,)
            ).fetchone()
            req = _row_to_dict(row)
            if req and _is_request_expired(req):
                conn.execute('DELETE FROM pending_requests WHERE request_id = ?', (request_id,))
                conn.commit()
                return {}
            return req
        finally:
            conn.close()


def claim_pending_request(request_id: str, action: str, handled_by: int) -> Dict[str, Any]:
    with DB_LOCK:
        conn = get_db_conn()
        try:
            row = conn.execute(
                'SELECT * FROM pending_requests WHERE request_id = ?',
                (request_id,)
            ).fetchone()
            req = _row_to_dict(row)
            if not req:
                return {}

            if _is_request_expired(req):
                conn.execute('DELETE FROM pending_requests WHERE request_id = ?', (request_id,))
                conn.commit()
                return {}

            if req.get('status') != 'pending':
                return {}

            req['status'] = 'processing'
            req['action'] = action
            req['handled_by'] = handled_by
            req['handled_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            conn.execute(
                """
                UPDATE pending_requests
                SET status = ?, action = ?, handled_by = ?, handled_at = ?
                WHERE request_id = ?
                """,
                (req['status'], req['action'], req['handled_by'], req['handled_at'], request_id)
            )
            conn.commit()
            return req
        finally:
            conn.close()


def delete_pending_request(request_id: str) -> None:
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute('DELETE FROM pending_requests WHERE request_id = ?', (request_id,))
            conn.commit()
        finally:
            conn.close()


def restore_pending_request(request_id: str, req: Dict[str, Any]) -> Dict[str, Any]:
    restored = dict(req or {})
    restored['status'] = 'pending'
    restored['action'] = None
    restored['handled_by'] = None
    restored['handled_at'] = None

    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO pending_requests (
                    request_id, status, requester_user_id, requester_chat_id, media_type, tmdb_id,
                    requester_name, admin_message_refs, created_at, created_at_ts,
                    handled_by, handled_at, action
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    restored.get('status') or 'pending',
                    _safe_int(restored.get('requester_user_id'), 0),
                    _safe_int(restored.get('requester_chat_id'), 0),
                    restored.get('media_type') or '',
                    _safe_int(restored.get('tmdb_id'), 0),
                    restored.get('requester_name') or '',
                    _json_dumps(restored.get('admin_message_refs') or []),
                    restored.get('created_at') or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    _safe_int(restored.get('created_at_ts'), int(time.time())),
                    None,
                    None,
                    None,
                )
            )
            conn.commit()
            return restored
        finally:
            conn.close()


def cleanup_expired_pending_requests() -> int:
    now_ts = int(time.time())
    with DB_LOCK:
        conn = get_db_conn()
        try:
            rows = conn.execute(
                'SELECT request_id, status, created_at_ts, created_at FROM pending_requests'
            ).fetchall()

            removed_ids: List[str] = []
            for row in rows:
                req = dict(row)
                status = str(req.get('status') or 'pending').lower()

                if status == 'pending' and _is_request_expired(req, now_ts):
                    removed_ids.append(str(req.get('request_id')))

            if removed_ids:
                conn.executemany(
                    'DELETE FROM pending_requests WHERE request_id = ?',
                    [(rid,) for rid in removed_ids]
                )
                conn.commit()
                logging.info('已清理 %s 条过期待审核记录（SQLite）', len(removed_ids))

            return len(removed_ids)
        finally:
            conn.close()


def pending_cleanup_loop() -> None:
    while True:
        try:
            cleanup_expired_pending_requests()
        except Exception:
            logging.exception('定时清理 SQLite pending_requests 失败')
        time.sleep(PENDING_CLEANUP_INTERVAL)


def emby_get_item_by_id(item_id: str) -> Optional[Dict[str, Any]]:
    if not item_id or not emby_enabled():
        return None

    data = emby_get('/Items', {
        'Ids': item_id,
        'Fields': 'Overview,ProviderIds,DateCreated,CommunityRating,CriticRating,ParentIndexNumber,IndexNumber,SeriesName,SeriesId,ParentId,PremiereDate,Path',
        'EnableImages': 'true',
        'Limit': 1,
    })
    items = data.get('Items', [])
    return items[0] if items else None


def _extract_repr_item_id(item: Dict[str, Any], normalized: Dict[str, Any]) -> str:
    return str(item.get('Id') or (normalized.get('raw_item') or {}).get('Id') or '')


def _get_normalized_raw_type(normalized: Dict[str, Any]) -> str:
    raw_item = normalized.get('raw_item') or {}
    return str(raw_item.get('Type') or normalized.get('item_type') or '').strip().lower()


def build_notify_job_key(normalized: Dict[str, Any], item: Dict[str, Any]) -> Optional[str]:
    raw_type = _get_normalized_raw_type(normalized)
    if raw_type == 'movie':
        tmdb_id = item.get('_tmdb_id') or normalized.get('tmdb_id') or (item.get('ProviderIds') or {}).get('Tmdb')
        title = normalize_title(normalized.get('title') or item.get('Name') or item.get('_tmdb_name') or 'unknown')
        year = str(normalized.get('year') or item.get('ProductionYear') or item.get('_tmdb_year') or '')
        if tmdb_id:
            return f'movie:tmdb:{tmdb_id}'
        return f'movie:title:{title}:{year}'

    series_id = str((normalized.get('raw_item') or {}).get('SeriesId') or item.get('SeriesId') or item.get('Id') or '')
    season_number = normalized.get('season_number') or item.get('ParentIndexNumber')
    season_number = _safe_int(season_number, 0)

    if not series_id or season_number <= 0:
        return None

    return f'tv:{series_id}:s{season_number:02d}'


def enqueue_emby_notify_job(normalized: Dict[str, Any], item: Dict[str, Any]) -> Optional[str]:
    job_key = build_notify_job_key(normalized, item)
    if not job_key:
        logging.info('Webhook 缺少可结算的 series_id/season_number，已跳过延迟结算：%s', normalized.get('log_title') or normalized.get('title'))
        return None

    raw_type = _get_normalized_raw_type(normalized)
    media_type = 'movie' if raw_type == 'movie' else 'tv'
    now_ts = int(time.time())
    due_at_ts = now_ts + EMBY_NOTIFY_SETTLE_DELAY_SECONDS

    raw_item = normalized.get('raw_item') or {}
    payload_to_store = {
        'normalized': normalized,
        'item': {
            'Id': item.get('Id'),
            'Type': item.get('Type'),
            'SeriesId': item.get('SeriesId'),
            'SeriesName': item.get('SeriesName'),
            'ParentIndexNumber': item.get('ParentIndexNumber'),
            'IndexNumber': item.get('IndexNumber'),
            '_tmdb_id': item.get('_tmdb_id'),
            '_tmdb_media_type': item.get('_tmdb_media_type'),
            '_tmdb_name': item.get('_tmdb_name'),
            '_tmdb_year': item.get('_tmdb_year'),
            '_tmdb_rating': item.get('_tmdb_rating'),
        },
    }

    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute(
                """
                INSERT INTO emby_settlement_jobs (
                    job_key, media_type, title, year, tmdb_id, representative_item_id,
                    representative_item_type, series_id, series_name, season_number, season_item_id,
                    due_at_ts, last_event_ts, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_key) DO UPDATE SET
                    title=excluded.title,
                    year=excluded.year,
                    tmdb_id=excluded.tmdb_id,
                    representative_item_id=excluded.representative_item_id,
                    representative_item_type=excluded.representative_item_type,
                    series_id=excluded.series_id,
                    series_name=excluded.series_name,
                    season_number=excluded.season_number,
                    season_item_id=excluded.season_item_id,
                    due_at_ts=excluded.due_at_ts,
                    last_event_ts=excluded.last_event_ts,
                    payload_json=excluded.payload_json
                """,
                (
                    job_key,
                    media_type,
                    normalized.get('title') or item.get('_tmdb_name') or item.get('Name') or item.get('SeriesName') or '未知标题',
                    str(normalized.get('year') or item.get('_tmdb_year') or item.get('ProductionYear') or ''),
                    str(item.get('_tmdb_id') or normalized.get('tmdb_id') or ''),
                    _extract_repr_item_id(item, normalized),
                    raw_type,
                    str(raw_item.get('SeriesId') or item.get('SeriesId') or item.get('Id') or ''),
                    normalized.get('title') or item.get('SeriesName') or item.get('_tmdb_name') or item.get('Name') or '未知标题',
                    _safe_int(normalized.get('season_number') or item.get('ParentIndexNumber'), 0) or None,
                    str(raw_item.get('SeasonId') or item.get('SeasonId') or ''),
                    due_at_ts,
                    now_ts,
                    _json_dumps(payload_to_store),
                )
            )
            conn.commit()
            logging.info('已加入延迟结算队列：job=%s due_at=%s', job_key, due_at_ts)
            return job_key
        finally:
            conn.close()


def get_due_emby_jobs(now_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    now_ts = now_ts or int(time.time())
    with DB_LOCK:
        conn = get_db_conn()
        try:
            rows = conn.execute(
                'SELECT * FROM emby_settlement_jobs WHERE due_at_ts <= ? ORDER BY due_at_ts ASC, last_event_ts ASC',
                (now_ts,)
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()


def delete_emby_job(job_key: str) -> None:
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute('DELETE FROM emby_settlement_jobs WHERE job_key = ?', (job_key,))
            conn.commit()
        finally:
            conn.close()


def postpone_emby_job(job_key: str, seconds: int = EMBY_NOTIFY_SETTLE_DELAY_SECONDS) -> None:
    due_at_ts = int(time.time()) + max(seconds, 5)
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute('UPDATE emby_settlement_jobs SET due_at_ts = ? WHERE job_key = ?', (due_at_ts, job_key))
            conn.commit()
        finally:
            conn.close()


def get_snapshot(snapshot_key: str) -> Dict[str, Any]:
    with DB_LOCK:
        conn = get_db_conn()
        try:
            row = conn.execute('SELECT * FROM emby_snapshots WHERE snapshot_key = ?', (snapshot_key,)).fetchone()
            if not row:
                return {}
            data = dict(row)
            data['episode_numbers'] = set(_json_loads(data.get('episode_numbers_json'), []))
            return data
        finally:
            conn.close()


def save_snapshot(snapshot_key: str, media_type: str, series_id: str, season_number: Optional[int], episode_numbers: Set[int], total_known: Optional[int]) -> None:
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute(
                """
                INSERT INTO emby_snapshots (
                    snapshot_key, media_type, series_id, season_number, episode_numbers_json,
                    last_total_known, last_notified_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(snapshot_key) DO UPDATE SET
                    media_type=excluded.media_type,
                    series_id=excluded.series_id,
                    season_number=excluded.season_number,
                    episode_numbers_json=excluded.episode_numbers_json,
                    last_total_known=excluded.last_total_known,
                    last_notified_at=excluded.last_notified_at,
                    updated_at=excluded.updated_at
                """,
                (
                    snapshot_key,
                    media_type,
                    series_id,
                    season_number,
                    _json_dumps(sorted(int(x) for x in episode_numbers)),
                    total_known,
                    now_str,
                    now_str,
                )
            )
            conn.commit()
        finally:
            conn.close()


def build_snapshot_key(job: Dict[str, Any]) -> str:
    if job.get('media_type') == 'movie':
        return str(job.get('job_key'))
    return f"tv:{job.get('series_id')}:s{_safe_int(job.get('season_number'), 0):02d}"


def parse_local_datetime(raw: Any) -> Optional[datetime]:
    raw = str(raw or '').strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        return dt.astimezone()
    except Exception:
        return None


def is_episode_aired(item: Dict[str, Any]) -> bool:
    dt = parse_local_datetime(item.get('PremiereDate'))
    if not dt:
        return True
    return dt <= datetime.now().astimezone()


def format_episode_list(numbers: List[int]) -> str:
    cleaned = sorted({int(n) for n in numbers if _safe_int(n, 0) > 0})
    if not cleaned:
        return '无'

    ranges: List[str] = []
    start = prev = cleaned[0]
    for num in cleaned[1:]:
        if num == prev + 1:
            prev = num
            continue
        if start == prev:
            ranges.append(f'E{start:02d}')
        else:
            ranges.append(f'E{start:02d}-E{prev:02d}')
        start = prev = num
    if start == prev:
        ranges.append(f'E{start:02d}')
    else:
        ranges.append(f'E{start:02d}-E{prev:02d}')
    return '、'.join(ranges)


def emby_get_seasons_for_series(series_id: str) -> List[Dict[str, Any]]:
    if not series_id:
        return []
    try:
        data = emby_get(f'/Shows/{series_id}/Seasons', {
            'Fields': 'ProviderIds,DateCreated',
            'EnableImages': 'true',
        })
        return data.get('Items', [])
    except Exception:
        logging.exception('获取剧集季列表失败：series_id=%s', series_id)
        return []


def emby_find_season_item(series_id: str, season_number: int) -> Optional[Dict[str, Any]]:
    seasons = emby_get_seasons_for_series(series_id)
    for season in seasons:
        if _safe_int(season.get('IndexNumber'), 0) == _safe_int(season_number, 0):
            return season
    return None


def emby_get_episodes_for_season(series_id: str, season_item_id: str = "", season_number: int = 0) -> List[Dict[str, Any]]:
    if not series_id:
        return []

    params: Dict[str, Any] = {
        'Fields': 'DateCreated,PremiereDate,Path,ProviderIds',
        'EnableImages': 'true',
    }

    if season_item_id:
        params['SeasonId'] = season_item_id
    elif season_number > 0:
        params['Season'] = season_number

    try:
        data = emby_get(f'/Shows/{series_id}/Episodes', params)
        return data.get('Items', []) or []
    except Exception:
        logging.exception(
            '获取季剧集列表失败：series_id=%s season_item_id=%s season_number=%s',
            series_id, season_item_id, season_number
        )
        return []


def emby_get_missing_for_series_season(series_id: str, season_number: int, is_unaired: Optional[bool]) -> List[Dict[str, Any]]:
    if not series_id or _safe_int(season_number, 0) <= 0:
        return []

    params: Dict[str, Any] = {
        'ParentId': series_id,
        'ParentIndexNumber': season_number,
        'IncludeItemTypes': 'Episode',
        'Recursive': 'true',
        'Limit': EMBY_NOTIFY_MAX_MISSING_FETCH,
        'Fields': 'DateCreated,PremiereDate,ProviderIds',
    }
    if is_unaired is not None:
        params['IsUnaired'] = 'true' if is_unaired else 'false'

    try:
        data = emby_get('/Shows/Missing', params)
        items = data.get('Items', []) or []
        filtered: List[Dict[str, Any]] = []
        for item in items:
            if _safe_int(item.get('ParentIndexNumber'), 0) == _safe_int(season_number, 0):
                filtered.append(item)
        return filtered
    except Exception:
        logging.exception('获取缺失剧集失败：series_id=%s season=%s is_unaired=%s', series_id, season_number, is_unaired)
        return []


def compute_tv_notify_stats(job: Dict[str, Any], item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    series_id = str(job.get('series_id') or item.get('SeriesId') or item.get('Id') or '')
    season_number = _safe_int(
        job.get('season_number')
        or (item.get('_normalized') or {}).get('season_number')
        or item.get('ParentIndexNumber'),
        0,
    )
    if not series_id or season_number <= 0:
        return None

    season_item = None
    season_item_id = str(job.get('season_item_id') or '')
    if season_item_id:
        season_item = {'Id': season_item_id, 'IndexNumber': season_number}
    else:
        season_item = emby_find_season_item(series_id, season_number)
        season_item_id = str((season_item or {}).get('Id') or '')

    if not season_item_id:
        logging.info('未找到季项目，跳过精确结算：series_id=%s season=%s', series_id, season_number)
        return None

    episodes = emby_get_episodes_for_season(
        series_id=series_id,
        season_item_id=season_item_id,
        season_number=season_number,
    )

    existing_numbers: Set[int] = set()
    for ep in episodes:
        idx = _safe_int(ep.get('IndexNumber'), 0)
        if idx > 0:
            existing_numbers.add(idx)

    stats: Dict[str, Any] = {
        'series_id': series_id,
        'season_number': season_number,
        'season_item_id': season_item_id,
        'existing_numbers': sorted(existing_numbers),
        'current_in_library': len(existing_numbers),
        'snapshot_key': f'tv:{series_id}:s{season_number:02d}',
    }

    tmdb_id = item.get("_tmdb_id") or (item.get("_normalized") or {}).get("tmdb_id")
    tmdb_media_type = item.get("_tmdb_media_type") or "tv"

    season_detail = None
    if tmdb_media_type == "tv" and tmdb_id:
        season_detail = get_tmdb_season_detail_for_notify(tmdb_id, season_number)

    # 优先用 TMDB 季详情
    if season_detail and season_detail.get("total_count", 0) > 0:
        aired_numbers = set(season_detail.get("aired_numbers") or [])
        unaired_numbers = set(season_detail.get("unaired_numbers") or [])
        missing_numbers = sorted(aired_numbers - existing_numbers)

        is_ongoing = len(unaired_numbers) > 0

        stats.update({
            'is_ongoing': is_ongoing,
            'total_count': _safe_int(season_detail.get('total_count'), 0),
            'current_aired': len(aired_numbers),
            'missing_count': len(missing_numbers),
            'missing_text': format_episode_list(missing_numbers),
            'pending_count': len(unaired_numbers),
            'pending_text': format_episode_list(sorted(unaired_numbers)),
        })
        return stats

    # TMDB 季详情拿不到时，才退回 Emby Missing 旧逻辑
    aired_missing_items = emby_get_missing_for_series_season(series_id, season_number, False)
    unaired_missing_items = emby_get_missing_for_series_season(series_id, season_number, True)

    aired_missing_numbers = sorted({
        _safe_int(x.get('IndexNumber'), 0)
        for x in aired_missing_items
        if _safe_int(x.get('IndexNumber'), 0) > 0
    })
    unaired_missing_numbers = sorted({
        _safe_int(x.get('IndexNumber'), 0)
        for x in unaired_missing_items
        if _safe_int(x.get('IndexNumber'), 0) > 0
    })

    is_ongoing = len(unaired_missing_numbers) > 0
    missing_numbers = aired_missing_numbers

    stats.update({
        'is_ongoing': is_ongoing,
        'total_count': len(set(existing_numbers) | set(aired_missing_numbers) | set(unaired_missing_numbers)),
        'current_aired': len(set(existing_numbers) | set(aired_missing_numbers)),
        'missing_count': len(missing_numbers),
        'missing_text': format_episode_list(missing_numbers),
        'pending_count': len(unaired_missing_numbers),
        'pending_text': format_episode_list(unaired_missing_numbers),
    })
    return stats


def build_tv_stats_lines(stats: Dict[str, Any]) -> List[str]:
    lines = [
        '━━━━━━━━━━━━━━━━━━━━',
    ]

    missing_count = _safe_int(stats.get('missing_count'), 0)
    missing_text = stats.get('missing_text') or '无'
    pending_count = _safe_int(stats.get('pending_count'), 0)
    pending_text = stats.get('pending_text') or '无'

    if stats.get('is_ongoing'):
        lines.extend([
            f"📡 *当前已播：* {escape_markdown_v2(stats.get('current_aired'))} 集",
            f"✅ *当前已入库：* {escape_markdown_v2(stats.get('current_in_library'))} 集",
        ])

        if missing_count > 0:
            lines.extend([
                f"❗ *缺集：* {escape_markdown_v2(missing_count)} 集",
                f"📝 *缺少：* {escape_markdown_v2(missing_text)}",
            ])

        if pending_count > 0:
            lines.extend([
                f"🕒 *未播待更新：* {escape_markdown_v2(pending_count)} 集",
                f"📅 *待更新集：* {escape_markdown_v2(pending_text)}",
            ])
    else:
        lines.extend([
            f"📦 *总集数：* {escape_markdown_v2(stats.get('total_count'))} 集",
            f"✅ *已入库：* {escape_markdown_v2(stats.get('current_in_library'))} 集",
        ])

        if missing_count > 0:
            lines.extend([
                f"❗ *缺失：* {escape_markdown_v2(missing_count)} 集",
                f"📝 *缺少：* {escape_markdown_v2(missing_text)}",
            ])

    return lines

def notify_channel_new_item(
    item: Dict[str, Any],
    extra_lines: Optional[List[str]] = None,
    force_skip_cache: bool = False,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    if not TG_CHANNEL_ID:
        logging.info("未配置 TG_CHANNEL_ID，跳过频道通知")
        return

    if not force_skip_cache and should_skip_recent_notify(item):
        logging.info("同一波入库重复通知，已跳过：%s", format_emby_notify_title(item))
        return

    text = build_channel_new_item_text(item, extra_lines=extra_lines, stats=stats)
    image_url = build_channel_notify_image_url(item)

    if image_url:
        try:
            tg_api("sendPhoto", {
                "chat_id": TG_CHANNEL_ID,
                "photo": image_url,
                "caption": text,
                "parse_mode": "MarkdownV2",
            })
            return
        except Exception:
            logging.exception("sendPhoto 失败，回退到纯文本通知")

    send_message(
        TG_CHANNEL_ID,
        text,
        parse_mode="MarkdownV2",
        disable_web_page_preview=True,
    )


def process_movie_job(job: Dict[str, Any]) -> bool:
    payload = _json_loads(job.get('payload_json'), {})
    normalized = payload.get('normalized') or {}
    item = payload.get('item') or {}
    rep_id = str(job.get('representative_item_id') or item.get('Id') or '')
    fresh_item = emby_get_item_by_id(rep_id) if rep_id else None
    merged_item = dict(fresh_item or {})
    if not merged_item:
        merged_item = dict(item)
    merged_item['_normalized'] = normalized
    merged_item = enrich_item_with_tmdb(merged_item)
    snapshot_key = build_snapshot_key(job)
    existing_snapshot = get_snapshot(snapshot_key)
    current_marker = {1}
    prev_marker = set(existing_snapshot.get('episode_numbers') or set())
    if prev_marker == current_marker:
        logging.info('电影结算无变化，跳过通知：%s', job.get('title'))
        return True
    notify_channel_new_item(merged_item)
    save_snapshot(snapshot_key, 'movie', str(job.get('series_id') or ''), None, current_marker, 1)
    return True


def process_tv_job(job: Dict[str, Any]) -> bool:
    payload = _json_loads(job.get('payload_json'), {})
    normalized = payload.get('normalized') or {}
    rep_item = payload.get('item') or {}
    representative_item_id = str(job.get('representative_item_id') or rep_item.get('Id') or '')

    item = emby_get_item_by_id(representative_item_id) if representative_item_id else None
    if not item:
        item = dict(rep_item)
    item['_normalized'] = normalized
    item = enrich_item_with_tmdb(item)

    stats = compute_tv_notify_stats(job, item)
    if not stats:
        logging.info('TV 结算缺少统计信息，跳过通知：job=%s', job.get('job_key'))
        return True

    snapshot_key = stats['snapshot_key']
    prev_snapshot = get_snapshot(snapshot_key)
    previous_numbers = set(prev_snapshot.get('episode_numbers') or set())
    current_numbers = set(stats.get('existing_numbers') or [])
    diff_numbers = sorted(current_numbers - previous_numbers)

    if previous_numbers and current_numbers == previous_numbers:
        logging.info('TV 结算无新集变化，跳过通知：job=%s season=%s', job.get('job_key'), stats.get('season_number'))
        return True

    extra_lines = build_tv_stats_lines(stats)
    notify_channel_new_item(item, extra_lines=extra_lines, force_skip_cache=True, stats=stats)
    save_snapshot(snapshot_key, 'tv', stats['series_id'], stats['season_number'], current_numbers, stats.get('total_known'))
    logging.info(
        'TV 结算完成：series_id=%s season=%s prev=%s current=%s new=%s',
        stats['series_id'],
        stats['season_number'],
        sorted(previous_numbers),
        sorted(current_numbers),
        diff_numbers,
    )
    return True


def process_emby_settlement_job(job: Dict[str, Any]) -> bool:
    if str(job.get('media_type') or '').lower() == 'movie':
        return process_movie_job(job)
    return process_tv_job(job)


def emby_settlement_loop() -> None:
    while True:
        try:
            due_jobs = get_due_emby_jobs()
            if due_jobs:
                for job in due_jobs:
                    job_key = str(job.get('job_key'))
                    try:
                        ok = process_emby_settlement_job(job)
                        if ok:
                            delete_emby_job(job_key)
                    except Exception:
                        logging.exception('处理延迟结算任务失败：job=%s', job_key)
                        postpone_emby_job(job_key, EMBY_NOTIFY_SETTLE_DELAY_SECONDS)
        except Exception:
            logging.exception('延迟结算轮询失败')
        time.sleep(EMBY_NOTIFY_SETTLE_POLL_SECONDS)


@app.post("/emby/webhook/new-media")
def emby_webhook_new_media():
    secret_check = _verify_emby_webhook_secret()
    if secret_check is not True:
        return secret_check

    payload = _parse_emby_webhook_data()
    event_type = str(payload.get("Event") or "").strip().lower()

    logging.info("收到 Emby webhook: %s", event_type)

    add_events = {"item.added", "library.new"}
    delete_events = {"item.removed", "library.deleted", "item.deleted"}

    if event_type not in add_events and event_type not in delete_events:
        return jsonify({
            "ok": True,
            "message": "非关注事件，已忽略",
            "event": event_type,
        })

    try:
        normalized = normalize_emby_webhook_item(payload)

        logging.info(
            "Emby webhook 解析结果: type=%s title=%s year=%s tmdb=%s season=%s episode=%s ep_title=%s",
            normalized.get("item_type"),
            normalized.get("log_title") or normalized.get("title"),
            normalized.get("year"),
            normalized.get("tmdb_id"),
            normalized.get("season_number"),
            normalized.get("episode_number"),
            normalized.get("episode_title"),
        )

        item = enrich_emby_item_from_api(normalized)
        item = enrich_item_with_tmdb(item)

        if event_type in delete_events:
            clear_recent_notify(item)
            return jsonify({
                "ok": True,
                "event": event_type,
                "action": "cache_cleared",
                "title": normalized.get("title"),
                "tmdb_id": get_emby_tmdb_id(item),
            })

        job_key = enqueue_emby_notify_job(normalized, item)
        if not job_key:
            return jsonify({
                "ok": True,
                "event": event_type,
                "action": "ignored",
                "reason": "missing_settlement_key",
                "title": normalized.get("title"),
                "tmdb_id": get_emby_tmdb_id(item),
                "item_type": normalized.get("item_type"),
            })

        return jsonify({
            "ok": True,
            "event": event_type,
            "action": "queued",
            "job_key": job_key,
            "title": normalized.get("title"),
            "tmdb_id": get_emby_tmdb_id(item),
            "item_type": normalized.get("item_type"),
        })
    except Exception as e:
        logging.exception("处理 Emby webhook 失败")
        return jsonify({"ok": False, "error": str(e)}), 500

def run_http_server() -> None:
    app.run(
        host=EMBY_WEBHOOK_HOST,
        port=EMBY_WEBHOOK_PORT,
        debug=False,
        use_reloader=False,
    )

def poll_forever() -> None:
    offset = None

    try:
        tg_api("deleteWebhook", {"drop_pending_updates": False})
        logging.info("已清理旧 webhook")
    except Exception as e:
        logging.warning("清理 webhook 失败：%s", e)

    while True:
        try:
            updates = tg_api("getUpdates", {
                "offset": offset,
                "timeout": POLL_TIMEOUT,
                "allowed_updates": ["message", "callback_query"],
            })

            for upd in updates:
                offset = upd["update_id"] + 1

                try:
                    if upd.get("callback_query"):
                        handle_callback_query(upd["callback_query"])
                    elif upd.get("message"):
                        handle_message(upd["message"])
                except Exception:
                    logging.exception("处理更新失败")
        except Exception:
            logging.exception("轮询失败，3 秒后重试")
            time.sleep(3)


if __name__ == "__main__":
    logging.info("Bot 启动中...")

    init_sqlite_db()
    migrate_json_to_sqlite_if_needed()

    cleanup_expired_pending_requests()

    Thread(target=pending_cleanup_loop, daemon=True).start()
    Thread(target=emby_settlement_loop, daemon=True).start()
    Thread(target=run_http_server, daemon=True).start()
    poll_forever()