import json
import logging
import time
from datetime import datetime
from threading import Thread, local
from typing import Any, Dict, List, Optional, Tuple

import app as core

# =========================
# 扩展说明
# - 这是一个独立入口文件，和原 app.py 配合使用
# - 不改原 app.py，直接运行本文件即可
# - 本文件把主要用户功能做成私聊按钮菜单，并增加管理员中心、管理员增删、功能开关
# =========================

orig_parse_admin_chat_ids = core.parse_admin_chat_ids
orig_is_admin_user = core.is_admin_user
orig_save_pending_request = core.save_pending_request
orig_notify_requester_result = core.notify_requester_result
orig_handle_message = core.handle_message
orig_handle_callback_query = core.handle_callback_query

_REQUEST_CTX = local()

# -------------------------
# 数据表初始化
# -------------------------

def _db_execute(script: str) -> None:
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            conn.executescript(script)
            conn.commit()
        finally:
            conn.close()


def init_private_menu_db() -> None:
    _db_execute(
        """
        CREATE TABLE IF NOT EXISTS bot_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value TEXT NOT NULL,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS input_states (
            user_id INTEGER PRIMARY KEY,
            state_key TEXT NOT NULL,
            payload_json TEXT DEFAULT '{}',
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS request_history (
            request_id TEXT PRIMARY KEY,
            requester_user_id INTEGER,
            requester_chat_id INTEGER,
            media_type TEXT,
            tmdb_id INTEGER,
            requester_name TEXT,
            season_number INTEGER,
            status TEXT,
            action TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_request_history_user ON request_history(requester_user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_request_history_chat ON request_history(requester_chat_id, created_at DESC);
        """
    )

    defaults = {
        "force_bind": "1",                  # 搜索/额度/在线播报前是否强制先绑定 Emby
        "enable_online_report": "1",       # 是否允许查看在线播报
        "enable_private_text_search": "1", # 私聊直接发文字是否当作搜索
        "enable_quota_limit": "1",         # 是否启用额度限制
        "allow_bound_user_bf": "1",        # 1=绑定用户可看在线播报；0=仅管理员可看
    }
    for key, value in defaults.items():
        if get_setting(key) is None:
            set_setting(key, value)


# -------------------------
# 通用 DB Helper
# -------------------------

def _json_dumps(data: Any) -> str:
    try:
        return json.dumps(data or {}, ensure_ascii=False)
    except Exception:
        return "{}"


def _json_loads(text: Any, default: Any) -> Any:
    if text in (None, ""):
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def get_setting(key: str) -> Optional[str]:
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            row = conn.execute(
                "SELECT setting_value FROM bot_settings WHERE setting_key = ?",
                (str(key),),
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()


def set_setting(key: str, value: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            conn.execute(
                """
                INSERT INTO bot_settings(setting_key, setting_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET
                    setting_value=excluded.setting_value,
                    updated_at=excluded.updated_at
                """,
                (str(key), str(value), now),
            )
            conn.commit()
        finally:
            conn.close()


def is_enabled(key: str, default: bool = False) -> bool:
    value = get_setting(key)
    if value is None:
        return default
    return str(value).strip() in ("1", "true", "True", "on", "yes")


def toggle_setting(key: str) -> bool:
    new_state = not is_enabled(key)
    set_setting(key, "1" if new_state else "0")
    return new_state


def set_input_state(user_id: int, state_key: str, payload: Optional[Dict[str, Any]] = None) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload_json = _json_dumps(payload or {})
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            conn.execute(
                """
                INSERT INTO input_states(user_id, state_key, payload_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    state_key=excluded.state_key,
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                """,
                (int(user_id), state_key, payload_json, now),
            )
            conn.commit()
        finally:
            conn.close()


def get_input_state(user_id: int) -> Tuple[str, Dict[str, Any]]:
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            row = conn.execute(
                "SELECT state_key, payload_json FROM input_states WHERE user_id = ?",
                (int(user_id),),
            ).fetchone()
            if not row:
                return "", {}
            return str(row[0] or ""), _json_loads(row[1], {})
        finally:
            conn.close()


def clear_input_state(user_id: int) -> None:
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            conn.execute("DELETE FROM input_states WHERE user_id = ?", (int(user_id),))
            conn.commit()
        finally:
            conn.close()
            
# -------------------------
# 管理员来源：仅 .env + OWNER_CHAT_ID
# -------------------------

def _get_owner_id() -> int:
    return core._safe_int(core.get_owner_chat_id_or_none(), 0)


def parse_admin_chat_ids_patched() -> List[int]:
    base_ids = set(orig_parse_admin_chat_ids())
    owner_id = _get_owner_id()
    if owner_id > 0:
        base_ids.add(owner_id)

    return sorted({
        core._safe_int(x, 0)
        for x in base_ids
        if core._safe_int(x, 0) > 0
    })


def is_admin_user_patched(user_id: int) -> bool:
    user_id = core._safe_int(user_id, 0)
    if user_id <= 0:
        return False
    return user_id in parse_admin_chat_ids_patched()


def list_admin_records() -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    owner_id = _get_owner_id()

    for uid in parse_admin_chat_ids_patched():
        rec = core.get_user_record(uid) or {}
        records.append(
            {
                "user_id": uid,
                "record": rec,
                "is_owner": uid == owner_id,
            }
        )
    return records


core.parse_admin_chat_ids = parse_admin_chat_ids_patched
core.is_admin_user = is_admin_user_patched


# -------------------------
# 请求历史补丁（可用于“我的记录”）
# -------------------------

def save_pending_request_patched(
    request_id: str,
    requester_user_id: int,
    requester_chat_id: int,
    media_type: str,
    tmdb_id: int,
    requester_name: str,
    admin_message_refs: List[Dict[str, int]],
) -> None:
    orig_save_pending_request(
        request_id=request_id,
        requester_user_id=requester_user_id,
        requester_chat_id=requester_chat_id,
        media_type=media_type,
        tmdb_id=tmdb_id,
        requester_name=requester_name,
        admin_message_refs=admin_message_refs,
    )

    season_number = core._safe_int((core.parse_request_id_meta(request_id) or {}).get("season_number"), 0)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            conn.execute(
                """
                INSERT INTO request_history(
                    request_id, requester_user_id, requester_chat_id, media_type, tmdb_id,
                    requester_name, season_number, status, action, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    requester_user_id=excluded.requester_user_id,
                    requester_chat_id=excluded.requester_chat_id,
                    media_type=excluded.media_type,
                    tmdb_id=excluded.tmdb_id,
                    requester_name=excluded.requester_name,
                    season_number=excluded.season_number,
                    status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                (
                    request_id,
                    core._safe_int(requester_user_id, 0),
                    core._safe_int(requester_chat_id, 0),
                    str(media_type),
                    core._safe_int(tmdb_id, 0),
                    requester_name or "",
                    season_number,
                    "pending",
                    None,
                    now,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()


def mark_request_history_result_by_request_id(request_id: str, action: str) -> None:
    request_id = str(request_id or "").strip()
    if not request_id:
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            conn.execute(
                """
                UPDATE request_history
                SET status = 'done', action = ?, updated_at = ?
                WHERE request_id = ?
                """,
                (str(action), now, request_id),
            )
            conn.commit()
        finally:
            conn.close()

def notify_requester_result_patched(
    requester_chat_id: int,
    action: str,
    media_type: str,
    detail: Dict[str, Any],
    remaining_quota: Optional[int] = None,
    season_number: Optional[int] = None,
) -> None:
    try:
        request_id = str(getattr(_REQUEST_CTX, "request_id", "") or "").strip()
        if request_id:
            mark_request_history_result_by_request_id(
                request_id=request_id,
                action=action,
            )
    except Exception:
        logging.exception("更新 request_history 结果失败")

    orig_notify_requester_result(
        requester_chat_id=requester_chat_id,
        action=action,
        media_type=media_type,
        detail=detail,
        remaining_quota=remaining_quota,
        season_number=season_number,
    )


core.save_pending_request = save_pending_request_patched
core.notify_requester_result = notify_requester_result_patched


# -------------------------
# 展示/消息辅助
# -------------------------

def _status_icon(enabled: bool) -> str:
    return "✅" if enabled else "❌"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def append_global_home_button(reply_markup: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """给所有私聊卡片底部统一追加一个居中的“主界面”按钮。"""
    if reply_markup is None:
        return None
    rows = reply_markup.get("inline_keyboard") if isinstance(reply_markup, dict) else None
    if not isinstance(rows, list):
        return reply_markup

    new_rows: List[List[Dict[str, Any]]] = []
    has_home_button = False
    for row in rows:
        if not isinstance(row, list):
            continue
        new_row: List[Dict[str, Any]] = []
        for btn in row:
            if not isinstance(btn, dict):
                continue
            copied = dict(btn)
            if copied.get("callback_data") == "pm|home" and "主界面" in str(copied.get("text") or ""):
                has_home_button = True
            new_row.append(copied)
        if new_row:
            new_rows.append(new_row)

    if not has_home_button:
        new_rows.append([{"text": "🏠 主界面", "callback_data": "pm|home"}])

    return {"inline_keyboard": new_rows}


def card_nav_keyboard(
    back_code: str = "pm|home",
    back_text: str = "🔙 返回",
    refresh_code: Optional[str] = None,
    refresh_text: str = "🔄 刷新",
) -> Dict[str, Any]:
    rows: List[List[Dict[str, Any]]] = []
    if refresh_code:
        rows.append([{"text": refresh_text, "callback_data": refresh_code}])
    if back_code:
        rows.append([{"text": back_text, "callback_data": back_code}])
    rows.append([{"text": "🏠 主界面", "callback_data": "pm|home"}])
    return {"inline_keyboard": rows}


def home_only_keyboard() -> Dict[str, Any]:
    return {"inline_keyboard": [[{"text": "🏠 主界面", "callback_data": "pm|home"}]]}


def edit_or_send_text(
    chat_id: int,
    text: str,
    reply_markup: Optional[Dict[str, Any]] = None,
    message_id: Optional[int] = None,
    parse_mode: Optional[str] = None,
) -> None:
    reply_markup = append_global_home_button(reply_markup)
    if message_id:
        try:
            core.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
            return
        except Exception as e:
            err = str(e)
            if "message is not modified" in err:
                # 内容和按钮都没变化，Telegram 会直接拒绝编辑；这是正常情况，静默跳过
                return
            logging.exception("编辑消息失败，改为发送新消息")

    core.send_message(
        chat_id,
        text,
        parse_mode=parse_mode,
        reply_markup=reply_markup,
        disable_web_page_preview=True,
    )


def send_message_result(
    chat_id: int,
    text: str,
    parse_mode: Optional[str] = None,
    reply_markup: Optional[Dict[str, Any]] = None,
    disable_web_page_preview: bool = True,
) -> Dict[str, Any]:
    """发送消息并返回 Telegram 的 message 结果，方便后续删除提示消息。"""
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = append_global_home_button(reply_markup)
    try:
        result = core.tg_api("sendMessage", payload)
        return result if isinstance(result, dict) else {}
    except Exception:
        logging.exception("发送消息失败")
        return {}


def safe_delete_message(chat_id: int, message_id: Any) -> None:
    message_id = core._safe_int(message_id, 0)
    chat_id = core._safe_int(chat_id, 0)
    if chat_id == 0 or message_id <= 0:
        return
    try:
        core.tg_api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception as e:
        # 删除失败通常是消息过旧、无权限、已删除，不影响主流程。
        logging.debug("删除消息失败：chat_id=%s message_id=%s err=%s", chat_id, message_id, e)


def get_message_id(message: Dict[str, Any]) -> int:
    return core._safe_int((message or {}).get("message_id"), 0)


def input_back_keyboard(back_code: str = "pm|home", refresh_code: Optional[str] = None) -> Dict[str, Any]:
    return card_nav_keyboard(back_code, "🔙 返回", refresh_code=refresh_code)


def _payload_prompt_ids(payload: Optional[Dict[str, Any]]) -> List[int]:
    result: List[int] = []
    for mid in ((payload or {}).get("prompt_message_ids") or []):
        mid_int = core._safe_int(mid, 0)
        if mid_int > 0 and mid_int not in result:
            result.append(mid_int)
    return result


def start_card_input_state(
    chat_id: int,
    user_id: int,
    state_key: str,
    prompt_text: str,
    source_message_id: int,
    payload: Optional[Dict[str, Any]] = None,
    delete_source_prompt: bool = False,
    delete_user_messages: bool = True,
    refresh_code: Optional[str] = None,
) -> None:
    payload = dict(payload or {})
    payload.update({
        "mode": "card",
        "source_message_id": core._safe_int(source_message_id, 0),
        "delete_source_prompt": bool(delete_source_prompt),
        "delete_user_messages": bool(delete_user_messages),
        "prompt_message_ids": _payload_prompt_ids(payload),
    })
    edit_or_send_text(
        chat_id,
        prompt_text,
        input_back_keyboard(refresh_code=refresh_code),
        message_id=source_message_id,
    )
    set_input_state(user_id, state_key, payload)


def send_new_input_state(
    chat_id: int,
    user_id: int,
    state_key: str,
    prompt_text: str,
    payload: Optional[Dict[str, Any]] = None,
    delete_user_messages: bool = True,
) -> None:
    payload = dict(payload or {})
    prompt_ids = _payload_prompt_ids(payload)
    result = send_message_result(chat_id, prompt_text, reply_markup=input_back_keyboard())
    prompt_mid = core._safe_int(result.get("message_id"), 0)
    if prompt_mid > 0:
        prompt_ids.append(prompt_mid)
    payload.update({
        "mode": "manual",
        "prompt_message_ids": prompt_ids,
        "delete_user_messages": bool(delete_user_messages),
    })
    set_input_state(user_id, state_key, payload)


def update_input_prompt(
    chat_id: int,
    user_id: int,
    state_key: str,
    prompt_text: str,
    payload: Dict[str, Any],
) -> None:
    payload = dict(payload or {})
    source_mid = core._safe_int(payload.get("source_message_id"), 0)
    prompt_ids = _payload_prompt_ids(payload)

    if source_mid > 0:
        edit_or_send_text(chat_id, prompt_text, input_back_keyboard(), message_id=source_mid)
    elif prompt_ids:
        edit_or_send_text(chat_id, prompt_text, input_back_keyboard(), message_id=prompt_ids[-1])
    else:
        result = send_message_result(chat_id, prompt_text, reply_markup=input_back_keyboard())
        prompt_mid = core._safe_int(result.get("message_id"), 0)
        if prompt_mid > 0:
            prompt_ids.append(prompt_mid)
        payload["prompt_message_ids"] = prompt_ids

    set_input_state(user_id, state_key, payload)


def cleanup_input_messages(
    chat_id: int,
    payload: Optional[Dict[str, Any]],
    user_message: Optional[Dict[str, Any]] = None,
    keep_message_id: Optional[int] = None,
) -> None:
    payload = dict(payload or {})
    keep_message_id = core._safe_int(keep_message_id, 0)

    if user_message and payload.get("delete_user_messages", True):
        safe_delete_message(chat_id, get_message_id(user_message))

    for mid in _payload_prompt_ids(payload):
        if mid != keep_message_id:
            safe_delete_message(chat_id, mid)

    source_mid = core._safe_int(payload.get("source_message_id"), 0)
    if payload.get("delete_source_prompt") and source_mid > 0 and source_mid != keep_message_id:
        safe_delete_message(chat_id, source_mid)


def get_result_card_message_id(payload: Optional[Dict[str, Any]]) -> int:
    payload = dict(payload or {})
    source_mid = core._safe_int(payload.get("source_message_id"), 0)
    if source_mid > 0:
        return source_mid
    prompt_ids = _payload_prompt_ids(payload)
    return prompt_ids[-1] if prompt_ids else 0


def show_card_result(
    chat_id: int,
    payload: Optional[Dict[str, Any]],
    text: str,
    parse_mode: Optional[str] = None,
    reply_markup: Optional[Dict[str, Any]] = None,
    fallback_back_code: str = "pm|home",
) -> None:
    message_id = get_result_card_message_id(payload)
    markup = reply_markup if reply_markup is not None else pending_keyboard(fallback_back_code)
    edit_or_send_text(
        chat_id,
        text,
        markup,
        message_id=message_id or None,
        parse_mode=parse_mode,
    )


def capture_core_send_messages(func) -> Tuple[List[Dict[str, Any]], Optional[BaseException]]:
    """捕获 app.py 内部原本要 send_message 的提示，改为后续写回当前卡片。"""
    captured: List[Dict[str, Any]] = []
    old_send_message = core.send_message

    def _capture_send_message(
        chat_id: int,
        text: str,
        parse_mode: Optional[str] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
        disable_web_page_preview: bool = True,
    ) -> None:
        captured.append({
            "chat_id": core._safe_int(chat_id, 0),
            "text": text or "",
            "parse_mode": parse_mode,
            "reply_markup": reply_markup,
            "disable_web_page_preview": disable_web_page_preview,
        })

    core.send_message = _capture_send_message
    try:
        func()
        return captured, None
    except BaseException as e:
        return captured, e
    finally:
        core.send_message = old_send_message


def render_captured_messages_to_card(
    chat_id: int,
    message_id: Optional[int],
    captured: List[Dict[str, Any]],
    fallback_text: str,
    fallback_back_code: str = "pm|home",
) -> None:
    texts: List[str] = []
    parse_modes: List[str] = []
    reply_markup: Optional[Dict[str, Any]] = None

    for item in captured or []:
        if core._safe_int(item.get("chat_id"), 0) not in (0, core._safe_int(chat_id, 0)):
            continue
        text = str(item.get("text") or "").strip()
        if text:
            texts.append(text)
        pm = item.get("parse_mode")
        if pm:
            parse_modes.append(str(pm))
        if item.get("reply_markup"):
            reply_markup = item.get("reply_markup")

    final_text = "\n\n".join(texts).strip() or fallback_text
    parse_mode = parse_modes[0] if parse_modes and all(x == parse_modes[0] for x in parse_modes) else None
    edit_or_send_text(
        chat_id,
        final_text,
        reply_markup or pending_keyboard(fallback_back_code),
        message_id=core._safe_int(message_id, 0) or None,
        parse_mode=parse_mode,
    )


def edit_search_choices_to_user(
    chat_id: int,
    message_id: int,
    requester_user_id: int,
    query: str,
    results: List[Dict[str, Any]],
) -> bool:
    if core._safe_int(message_id, 0) <= 0:
        return False
    try:
        core.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=core.build_user_choice_text(query, results),
            parse_mode="MarkdownV2",
            reply_markup=append_global_home_button(core.build_user_select_keyboard(results, requester_user_id, chat_id)),
            disable_web_page_preview=True,
        )
        return True
    except Exception:
        logging.exception("编辑卡片为搜索结果失败，回退为发送新消息")
        return False


def home_keyboard(user_id: int) -> Dict[str, Any]:
    rows = [
        [
            {"text": "🎬 求片", "callback_data": "pm|search"},
            {"text": "👤 我的 Emby", "callback_data": "pm|emby"},
        ],
        [
            {"text": "📄 我的待审", "callback_data": "pm|pending"},
            {"text": "🕘 我的记录", "callback_data": "pm|history"},
        ],
        [
            {"text": "💳 我的额度", "callback_data": "pm|quota"},
            {"text": "🪪 我的信息", "callback_data": "pm|me"},
        ],
        [
            {"text": "📡 在线播报", "callback_data": "pm|online"},
            {"text": "❓帮助", "callback_data": "pm|help"},
        ],
    ]
    if core.is_admin_user(user_id):
        rows.append([
            {"text": "🛠 管理员中心", "callback_data": "pm|admin"},
        ])
    return {"inline_keyboard": rows}


def emby_keyboard() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "🔐 绑定 Emby", "callback_data": "pm|bind"},
                {"text": "🔓 解绑 Emby", "callback_data": "pm|unbind"},
            ],
            [{"text": "🔄 刷新", "callback_data": "pm|emby"}],
            [{"text": "🔙 返回首页", "callback_data": "pm|home"}],
        ]
    }


def admin_center_keyboard(user_id: int) -> Dict[str, Any]:
    rows = [
        [
            {"text": "🎬 求片管理", "callback_data": "pm|apanel"},
            {"text": "👥 用户管理", "callback_data": "pm|users"},
        ],
        [
            {"text": "⚙️ 功能开关", "callback_data": "pm|settings"},
            {"text": "📊 数据面板", "callback_data": "pm|stats"},
        ],
        [{"text": "👮 管理员管理", "callback_data": "pm|admins"}],
        [{"text": "🔄 刷新", "callback_data": "pm|admin"}],
        [{"text": "🔙 返回首页", "callback_data": "pm|home"}],
    ]
    return {"inline_keyboard": rows}


def settings_keyboard() -> Dict[str, Any]:
    keys = [
        ("force_bind", "强制先绑定 Emby"),
        ("enable_online_report", "启用在线播报"),
        ("allow_bound_user_bf", "绑定用户可看在线播报"),
        ("enable_private_text_search", "私聊文字直接搜索"),
        ("enable_quota_limit", "启用额度限制"),
    ]
    rows = []
    for key, label in keys:
        rows.append([
            {
                "text": f"{_status_icon(is_enabled(key))} {label}",
                "callback_data": f"pm|toggle|{key}",
            }
        ])
    rows.append([
        {"text": "🔄 刷新", "callback_data": "pm|settings"},
    ])
    rows.append([
        {"text": "🔙 返回管理员中心", "callback_data": "pm|admin"},
    ])
    return {"inline_keyboard": rows}


def admin_manage_keyboard(user_id: int) -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": "📋 管理员列表", "callback_data": "pm|adminlist"}],
            [{"text": "🔄 刷新", "callback_data": "pm|admins"}],
            [{"text": "🔙 返回管理员中心", "callback_data": "pm|admin"}],
        ]
    }


def user_manage_prompt_keyboard() -> Dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "🔍 查询用户", "callback_data": "pm|finduser"},
                {"text": "💳 额度管理", "callback_data": "pm|quota_manage"},
            ],
            [{"text": "🔄 刷新", "callback_data": "pm|users"}],
            [{"text": "🔙 返回管理员中心", "callback_data": "pm|admin"}],
        ]
    }

def admin_list_keyboard(admins: List[Dict[str, Any]], viewer_user_id: int) -> Dict[str, Any]:
    rows = []

    for item in admins:
        uid = core._safe_int(item.get("user_id"), 0)
        rec = item.get("record") or {}
        name = core.get_tg_display_name(rec)
        role = "👑 创建者" if item.get("is_owner") else "👮 管理员"

        rows.append([
            {
                "text": f"{role}｜{name}｜{uid}",
                "callback_data": f"pm|admindetail|{uid}",
            }
        ])

    rows.append([
        {"text": "🔄 刷新", "callback_data": "pm|adminlist"},
    ])
    rows.append([
        {"text": "🔙 返回管理员管理", "callback_data": "pm|admins"},
    ])
    return {"inline_keyboard": rows}



def show_admin_detail(chat_id: int, viewer_user_id: int, target_user_id: int, message_id: Optional[int] = None) -> None:
    target_user_id = core._safe_int(target_user_id, 0)
    rec = core.get_user_record(target_user_id) or {}

    if target_user_id <= 0:
        edit_or_send_text(
            chat_id,
            "❌ 管理员 ID 无效。",
            pending_keyboard("pm|adminlist"),
            message_id=message_id,
        )
        return

    is_owner = core.is_owner_user(target_user_id)
    role = "创建者" if is_owner else "管理员"
    name = core.get_tg_display_name(rec)
    username = str(rec.get("username") or "").strip()
    emby_username = str(rec.get("emby_username") or "").strip() or "未绑定"

    text = (
        f"{'👑' if is_owner else '👮'} 管理员详情\n\n"
        f"身份：{role}\n"
        f"昵称：{name}\n"
        f"用户名：@{username if username else '无'}\n"
        f"TG ID：{target_user_id}\n"
        f"Emby：{emby_username}\n"
        f"最后活跃：{rec.get('last_seen_at') or '未知'}\n\n"
        f"来源：.env / OWNER_CHAT_ID"
    )

    edit_or_send_text(
        chat_id,
        text,
        {"inline_keyboard": [[{"text": "🔙 返回管理员列表", "callback_data": "pm|adminlist"}]]},
        message_id=message_id,
    )

def user_action_keyboard(target_user_id: int) -> Dict[str, Any]:
    uid = core._safe_int(target_user_id, 0)
    return {
        "inline_keyboard": [
            [
                {"text": "✏️ 修改额度", "callback_data": f"pm|uquota|{uid}"},
                {"text": "♻️ 清零成功次数", "callback_data": f"pm|ureset|{uid}"},
            ],
            [
                {"text": "🔓 强制解绑 Emby", "callback_data": f"pm|uunbind|{uid}"},
                {"text": "📄 查看待审核", "callback_data": f"pm|upending|{uid}"},
            ],
            [
                {"text": "🔍 重新查用户", "callback_data": "pm|finduser"},
                {"text": "🔙 返回管理员中心", "callback_data": "pm|admin"},
            ],
        ]
    }


def pending_keyboard(back_code: str = "pm|home", refresh_code: Optional[str] = None) -> Dict[str, Any]:
    return card_nav_keyboard(back_code, "🔙 返回", refresh_code=refresh_code)


# 重新补丁审核结果通知：私聊带“主界面”，群聊不带按钮。
def notify_requester_result_patched(
    requester_chat_id: int,
    action: str,
    media_type: str,
    detail: Dict[str, Any],
    remaining_quota: Optional[int] = None,
    season_number: Optional[int] = None,
) -> None:
    try:
        request_id = str(getattr(_REQUEST_CTX, "request_id", "") or "").strip()
        if request_id:
            mark_request_history_result_by_request_id(
                request_id=request_id,
                action=action,
            )
    except Exception:
        logging.exception("更新 request_history 结果失败")

    text = core.build_user_result_text(
        action,
        media_type,
        detail,
        remaining_quota=remaining_quota,
        season_number=season_number,
    )
    core.send_message(
        requester_chat_id,
        text,
        parse_mode="MarkdownV2",
        reply_markup=None if core._safe_int(requester_chat_id, 0) < 0 else home_only_keyboard(),
        disable_web_page_preview=False,
    )


core.notify_requester_result = notify_requester_result_patched


def admin_pending_manage_keyboard(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    inline_rows = []

    for idx, req in enumerate(rows, 1):
        request_id = str(req.get("request_id") or "").strip()
        if not request_id:
            continue

        inline_rows.append([
            {"text": f"✅ 批准 #{idx}", "callback_data": f"a|{request_id}"},
            {"text": f"❌ 拒绝 #{idx}", "callback_data": f"r|{request_id}"},
        ])

    inline_rows.append([{"text": "🔄 刷新", "callback_data": "pm|apanel"}])
    inline_rows.append([{"text": "🔙 返回管理中心", "callback_data": "pm|admin"}])

    return {"inline_keyboard": inline_rows}


# -------------------------
# 业务查询辅助
# -------------------------

def raw_remaining_quota(record: Dict[str, Any]) -> int:
    quota_limit = core._safe_int(record.get("quota_limit"), core.DEFAULT_QUOTA_LIMIT)
    success_count = core._safe_int(record.get("success_count"), 0)
    return max(quota_limit - success_count, 0)


def ensure_private_message(message: Dict[str, Any]) -> bool:
    chat_type = ((message.get("chat") or {}).get("type") or "").lower()
    return chat_type == "private"


def ensure_callback_private(callback: Dict[str, Any]) -> bool:
    msg = callback.get("message") or {}
    chat_type = ((msg.get("chat") or {}).get("type") or "").lower()
    return chat_type == "private"


def ensure_bind_if_needed(message: Dict[str, Any]) -> bool:
    if not is_enabled("force_bind", True):
        return True
    return core.require_user_emby_bound(message)


def search_user_record(text: str) -> Dict[str, Any]:
    text = str(text or "").strip()
    if not text:
        return {}

    if text.startswith("@"):
        text = text[1:]

    uid = core._safe_int(text, 0)
    if uid > 0:
        return core.get_user_record(uid)

    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            row = conn.execute(
                """
                SELECT * FROM users
                WHERE lower(username) = lower(?)
                   OR lower(display_name) = lower(?)
                ORDER BY last_seen_at DESC, updated_at DESC, tg_id DESC
                LIMIT 1
                """,
                (text, text),
            ).fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()

def get_existing_user_record(target_user_id: int) -> Dict[str, Any]:
    target_user_id = core._safe_int(target_user_id, 0)
    if target_user_id <= 0:
        return {}

    record = core.get_user_record(target_user_id) or {}
    if core._safe_int(record.get("tg_id"), 0) != target_user_id:
        return {}

    return record


def get_user_pending_requests(target_user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            rows = conn.execute(
                """
                SELECT * FROM pending_requests
                WHERE requester_user_id = ?
                ORDER BY created_at_ts DESC, request_id DESC
                LIMIT ?
                """,
                (core._safe_int(target_user_id, 0), core._safe_int(limit, 10)),
            ).fetchall()
            return [core._row_to_dict(r) for r in rows]
        finally:
            conn.close()


def get_user_history(target_user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            rows = conn.execute(
                """
                SELECT * FROM request_history
                WHERE requester_user_id = ?
                ORDER BY created_at DESC, request_id DESC
                LIMIT ?
                """,
                (core._safe_int(target_user_id, 0), core._safe_int(limit, 10)),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def get_pending_stats(limit: int = 8) -> List[Dict[str, Any]]:
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM pending_requests ORDER BY created_at_ts DESC, request_id DESC LIMIT ?",
                (core._safe_int(limit, 8),),
            ).fetchall()
            return [core._row_to_dict(r) for r in rows]
        finally:
            conn.close()


def resolve_request_title(media_type: str, tmdb_id: int, season_number: Optional[int]) -> str:
    try:
        detail = core.get_detail_with_fallback(media_type, core._safe_int(tmdb_id, 0))
        return core.get_display_name(detail, media_type, season_number=season_number)
    except Exception:
        base = f"{media_type}:{tmdb_id}"
        if core._safe_int(season_number, 0) > 0:
            base += f" S{int(season_number):02d}"
        return base


def format_user_card(record: Dict[str, Any]) -> str:
    uid = core._safe_int(record.get("tg_id"), 0)
    username = str(record.get("username") or "").strip()
    quota_limit = core._safe_int(record.get("quota_limit"), core.DEFAULT_QUOTA_LIMIT)
    success_count = core._safe_int(record.get("success_count"), 0)
    remain = raw_remaining_quota(record)
    emby_username = str(record.get("emby_username") or "").strip() or "未绑定"
    display_name = core.get_tg_display_name(record)
    role = "创建者" if core.is_owner_user(uid) else ("管理员" if core.is_admin_user(uid) else "普通用户")

    return (
        "👤 用户详情\n\n"
        f"昵称：{display_name}\n"
        f"用户名：@{username if username else '无'}\n"
        f"TG ID：{uid}\n"
        f"身份：{role}\n"
        f"Emby：{emby_username}\n"
        f"总额度：{quota_limit}\n"
        f"已成功：{success_count}\n"
        f"剩余额度：{remain}\n"
        f"最后活跃：{record.get('last_seen_at') or '未知'}"
    )


def format_my_info(record: Dict[str, Any]) -> str:
    uid = core._safe_int(record.get("tg_id"), 0)
    username = str(record.get("username") or "").strip()
    emby_username = str(record.get("emby_username") or "").strip() or "未绑定"
    role = "创建者" if core.is_owner_user(uid) else ("管理员" if core.is_admin_user(uid) else "普通用户")
    quota_limit = core._safe_int(record.get("quota_limit"), core.DEFAULT_QUOTA_LIMIT)
    success_count = core._safe_int(record.get("success_count"), 0)
    remain = raw_remaining_quota(record)
    return (
        "🪪 我的信息\n\n"
        f"昵称：{core.get_tg_display_name(record)}\n"
        f"用户名：@{username if username else '无'}\n"
        f"TG ID：{uid}\n"
        f"身份：{role}\n"
        f"Emby：{emby_username}\n"
        f"总额度：{quota_limit}\n"
        f"已成功：{success_count}\n"
        f"剩余额度：{remain}\n"
        f"最后活跃：{record.get('last_seen_at') or '未知'}"
    )


def show_home(chat_id: int, user_id: int, message_id: Optional[int] = None, extra: str = "") -> None:
    record = core.get_user_record(user_id)
    emby_name = str(record.get("emby_username") or "").strip() or "未绑定"
    text = (
        "🏠 Emby 私聊控制台\n\n"
        f"当前绑定：{emby_name}\n"
        f"当前时间：{_now()}\n"
    )
    if extra:
        text += f"\n{extra}\n"
    text += "\n请选择下方功能："
    edit_or_send_text(chat_id, text, home_keyboard(user_id), message_id=message_id)


def show_emby_panel(chat_id: int, user_id: int, message_id: Optional[int] = None) -> None:
    record = core.get_user_record(user_id)
    emby_name = str(record.get("emby_username") or "").strip() or "未绑定"
    text = (
        "👤 我的 Emby\n\n"
        f"当前绑定：{emby_name}\n\n"
        "可以通过下方按钮绑定或解绑 Emby 账号。"
    )
    edit_or_send_text(chat_id, text, emby_keyboard(), message_id=message_id)


def show_quota_panel(chat_id: int, user_id: int, message_id: Optional[int] = None) -> None:
    record = core.get_user_record(user_id)
    quota_limit = core._safe_int(record.get("quota_limit"), core.DEFAULT_QUOTA_LIMIT)
    success_count = core._safe_int(record.get("success_count"), 0)
    remain = raw_remaining_quota(record)
    text = (
        "💳 我的额度\n\n"
        f"总额度：{quota_limit}\n"
        f"已成功求片：{success_count}\n"
        f"剩余额度：{remain}\n\n"
    )
    edit_or_send_text(chat_id, text, pending_keyboard("pm|home", refresh_code="pm|quota"), message_id=message_id)


def show_my_pending(chat_id: int, user_id: int, message_id: Optional[int] = None) -> None:
    rows = get_user_pending_requests(user_id, limit=10)
    lines = ["📄 我的待审核\n"]
    if not rows:
        lines.append("当前没有待审核请求。")
    else:
        for idx, req in enumerate(rows, 1):
            meta = core.parse_request_id_meta(req.get("request_id") or "") or {}
            season_number = core._safe_int(meta.get("season_number"), 0)
            title = resolve_request_title(
                str(req.get("media_type") or ""),
                core._safe_int(req.get("tmdb_id"), 0),
                season_number,
            )
            lines.append(
                f"{idx}. {title}\n"
                f"   类型：{req.get('media_type')}\n"
                f"   提交时间：{req.get('created_at') or '未知'}\n"
            )
    edit_or_send_text(chat_id, "\n".join(lines).strip(), pending_keyboard("pm|home", refresh_code="pm|pending"), message_id=message_id)


def show_my_history(chat_id: int, user_id: int, message_id: Optional[int] = None) -> None:
    rows = get_user_history(user_id, limit=10)
    lines = ["🕘 我的申请记录\n"]
    if not rows:
        lines.append("暂无记录。")
    else:
        for idx, item in enumerate(rows, 1):
            season_number = core._safe_int(item.get("season_number"), 0)
            title = resolve_request_title(
                str(item.get("media_type") or ""),
                core._safe_int(item.get("tmdb_id"), 0),
                season_number,
            )
            action = str(item.get("action") or "pending")
            status_text = {
                "approve": "✅ 已通过",
                "reject": "❌ 已拒绝",
                "pending": "🕒 待审核",
                "": "🕒 待审核",
            }.get(action, "🕒 待审核")
            lines.append(
                f"{idx}. {title}\n"
                f"   状态：{status_text}\n"
                f"   时间：{item.get('created_at') or '未知'}\n"
            )
    edit_or_send_text(chat_id, "\n".join(lines).strip(), pending_keyboard("pm|home", refresh_code="pm|history"), message_id=message_id)


def show_online_panel(chat_id: int, user_id: int, message_id: Optional[int] = None) -> None:
    if not is_enabled("enable_online_report", True):
        edit_or_send_text(chat_id, "📡 在线播报功能当前已关闭。", pending_keyboard("pm|home", refresh_code="pm|online"), message_id=message_id)
        return

    allow_bound_user = is_enabled("allow_bound_user_bf", True)
    if allow_bound_user:
        record = core.get_user_record(user_id)
        if is_enabled("force_bind", True) and not str(record.get("emby_user_id") or "").strip() and not core.is_admin_user(user_id):
            edit_or_send_text(chat_id, "❌ 你还没有绑定 Emby 账号，暂时不能查看在线播报。", pending_keyboard("pm|home", refresh_code="pm|online"), message_id=message_id)
            return
    else:
        if not core.is_admin_user(user_id):
            edit_or_send_text(chat_id, "❌ 当前仅管理员可以查看在线播报。", pending_keyboard("pm|home", refresh_code="pm|online"), message_id=message_id)
            return

    if not core.emby_enabled():
        edit_or_send_text(chat_id, "❌ Emby 未配置，无法查询在线播报。", pending_keyboard("pm|home", refresh_code="pm|online"), message_id=message_id)
        return

    try:
        sessions = core.get_emby_online_sessions()
        text = core.build_bf_message(sessions)
    except Exception:
        logging.exception("查询在线播报失败")
        text = "❌ 查询 Emby 在线会话失败，请稍后重试。"

    edit_or_send_text(
        chat_id,
        text,
        pending_keyboard("pm|home", refresh_code="pm|online"),
        message_id=message_id,
        parse_mode="MarkdownV2",
    )


def show_help(chat_id: int, message_id: Optional[int] = None) -> None:
    text = (
        "❓ 帮助\n\n"
        "1. 点“🎬 求片”后，机器人会提示你输入片名/剧名。\n"
        "2. 点“👤 我的 Emby”可绑定或解绑 Emby 账号。\n"
        "3. 点“💳 我的额度”可查看求片额度。\n"
        "4. 点“📄 我的待审”可看当前待审核内容。\n"
        "5. 管理员会额外看到“🛠 管理员中心”。\n\n"
        "兼容命令：/start /menu /help /qp /bd /jb /cx /id /bf /ed"
    )
    edit_or_send_text(chat_id, text, pending_keyboard("pm|home", refresh_code="pm|help"), message_id=message_id)


def show_admin_center(chat_id: int, user_id: int, message_id: Optional[int] = None) -> None:
    if not core.is_admin_user(user_id):
        edit_or_send_text(chat_id, "❌ 只有管理员可见。", pending_keyboard("pm|home"), message_id=message_id)
        return
    text = (
        "🛠 管理员中心\n\n"
        "你可以在这里查看待审核、管理用户、切换功能开关，以及管理管理员。"
    )
    edit_or_send_text(chat_id, text, admin_center_keyboard(user_id), message_id=message_id)


def show_admin_stats(chat_id: int, message_id: Optional[int] = None) -> None:
    with core.DB_LOCK:
        conn = core.get_db_conn()
        try:
            total_users = core._safe_int(conn.execute("SELECT COUNT(*) FROM users").fetchone()[0], 0)
            bound_users = core._safe_int(conn.execute("SELECT COUNT(*) FROM users WHERE emby_user_id <> ''").fetchone()[0], 0)
            pending_count = core._safe_int(conn.execute("SELECT COUNT(*) FROM pending_requests").fetchone()[0], 0)
            history_count = core._safe_int(conn.execute("SELECT COUNT(*) FROM request_history").fetchone()[0], 0)
        finally:
            conn.close()

    text = (
        "📊 数据面板\n\n"
        f"用户总数：{total_users}\n"
        f"已绑定 Emby：{bound_users}\n"
        f"当前待审核：{pending_count}\n"
        f"累计记录数：{history_count}\n"
        f"当前管理员数：{len(parse_admin_chat_ids_patched())}"
    )
    edit_or_send_text(chat_id, text, pending_keyboard("pm|admin", refresh_code="pm|stats"), message_id=message_id)


def show_admin_pending_panel(chat_id: int, message_id: Optional[int] = None) -> None:
    rows = get_pending_stats(limit=10)
    lines = ["🎬 求片管理\n"]

    if not rows:
        lines.append("当前没有待审核请求。")
        reply_markup = pending_keyboard("pm|admin", refresh_code="pm|apanel")
    else:
        for idx, req in enumerate(rows, 1):
            meta = core.parse_request_id_meta(req.get("request_id") or "") or {}
            season_number = core._safe_int(meta.get("season_number"), 0)
            title = resolve_request_title(
                str(req.get("media_type") or ""),
                core._safe_int(req.get("tmdb_id"), 0),
                season_number,
            )

            lines.append(
                f"{idx}. {title}\n"
                f"   用户：{req.get('requester_name') or req.get('requester_user_id')}\n"
                f"   类型：{req.get('media_type') or '未知'}\n"
                f"   时间：{req.get('created_at') or '未知'}\n"
            )

        lines.append("\n可直接点击下方按钮进行批准或拒绝。")
        reply_markup = admin_pending_manage_keyboard(rows)

    edit_or_send_text(
        chat_id,
        "\n".join(lines).strip(),
        reply_markup,
        message_id=message_id,
    )

def show_admin_list(chat_id: int, user_id: int, message_id: Optional[int] = None, extra: str = "") -> None:
    admins = list_admin_records()

    text = (
        "👮 管理员列表\n\n"
        "当前管理员仅从 .env 和 OWNER_CHAT_ID 读取，运行中不可增删。"
    )

    if extra:
        text += f"\n\n{extra}"

    edit_or_send_text(
        chat_id,
        text,
        admin_list_keyboard(admins, user_id),
        message_id=message_id,
    )

def show_settings(chat_id: int, message_id: Optional[int] = None) -> None:
    text = (
        "⚙️ 功能开关\n\n"
        "点击下方按钮可直接开关对应功能。\n"
    )
    edit_or_send_text(chat_id, text, settings_keyboard(), message_id=message_id)

def setup_private_commands() -> None:
    commands = [
        {"command": "start", "description": "开始使用"},
        {"command": "qp", "description": "求片/搜索影片"},
        {"command": "bd", "description": "绑定账号"},
        {"command": "jb", "description": "解绑账号"},
        {"command": "cx", "description": "查看我的额度"},
        {"command": "id", "description": "查看我的信息"},
        {"command": "bf", "description": "查看在线播报"},
        {"command": "help", "description": "查看帮助说明"},
    ]

    try:
        # 所有私聊会话的命令菜单
        core.tg_api("setMyCommands", {
            "commands": commands,
            "scope": {"type": "all_private_chats"},
        })
    
        # 群菜单
        core.tg_api("setMyCommands", {
            "commands": commands,
            "scope": {"type": "all_group_chats"},
        })

        # 左下角菜单按钮设为命令菜单
        core.tg_api("setChatMenuButton", {
            "menu_button": {"type": "commands"}
        })

        logging.info("已自动设置 Telegram 私聊命令菜单")
    except Exception:
        logging.exception("自动设置 Telegram 私聊命令菜单失败")


# -------------------------
# 搜索流程（自带开关判断）
# -------------------------

def handle_search_with_flags(
    message: Dict[str, Any],
    query: str,
    result_message_id: Optional[int] = None,
    delete_user_message: bool = False,
) -> None:
    reply_chat_id = int(message["chat"]["id"])

    if is_enabled("force_bind", True):
        user_id_for_bind = core._safe_int((message.get("from") or {}).get("id"), 0)
        if not user_id_for_bind or not core.is_user_emby_bound(user_id_for_bind):
            text = (
                "❌ 你还没有绑定 Emby 账号，绑定后才能使用。\n\n"
                "请点击下方按钮绑定，或手动发送：\n"
                "/bd 账号 密码"
            )
            if result_message_id:
                edit_or_send_text(reply_chat_id, text, emby_keyboard(), message_id=result_message_id)
            else:
                core.send_message(reply_chat_id, text, reply_markup=append_global_home_button(emby_keyboard()))
            if delete_user_message:
                safe_delete_message(reply_chat_id, get_message_id(message))
            return

    user = message.get("from", {}) or {}
    requester_user_id = core._safe_int(user.get("id"), 0)

    record = core.upsert_user_profile(user, reply_chat_id)
    remaining_quota = raw_remaining_quota(record)

    if is_enabled("enable_quota_limit", True) and remaining_quota <= 0:
        text = "❌ 你的求片额度已用完，暂时不能继续求片。\n发送 /cx 查看详情。"
        if result_message_id:
            edit_or_send_text(reply_chat_id, text, pending_keyboard("pm|home"), message_id=result_message_id)
        else:
            core.send_message(reply_chat_id, text, reply_markup=home_only_keyboard())
        if delete_user_message:
            safe_delete_message(reply_chat_id, get_message_id(message))
        return

    results = core.search_tmdb(query)
    if not results:
        text = f"❌ 没有找到和 {query} 相关的电影或剧集，请换个关键词试试。"
        if result_message_id:
            edit_or_send_text(reply_chat_id, text, pending_keyboard("pm|home"), message_id=result_message_id)
        else:
            core.send_message(reply_chat_id, text, reply_markup=home_only_keyboard())
        if delete_user_message:
            safe_delete_message(reply_chat_id, get_message_id(message))
        return

    if result_message_id and edit_search_choices_to_user(
        reply_chat_id,
        result_message_id,
        requester_user_id,
        query,
        results,
    ):
        if delete_user_message:
            safe_delete_message(reply_chat_id, get_message_id(message))
        return

    core.send_message(
        reply_chat_id,
        core.build_user_choice_text(query, results),
        parse_mode="MarkdownV2",
        reply_markup=append_global_home_button(core.build_user_select_keyboard(results, requester_user_id, reply_chat_id)),
        disable_web_page_preview=True,
    )
    if delete_user_message:
        safe_delete_message(reply_chat_id, get_message_id(message))


# -------------------------
# 状态输入处理
# -------------------------

def handle_stateful_input(message: Dict[str, Any], state_key: str, payload: Dict[str, Any]) -> bool:
    chat_id = int(message["chat"]["id"])
    user_id = core._safe_int((message.get("from") or {}).get("id"), 0)
    text = (message.get("text") or "").strip()
    if not text:
        return True

    result_message_id = get_result_card_message_id(payload)

    if text.lower() in ("/cancel", "取消"):
        clear_input_state(user_id)
        cleanup_input_messages(chat_id, payload, message, keep_message_id=result_message_id or None)
        show_card_result(chat_id, payload, "✅ 已取消当前输入。", fallback_back_code="pm|home")
        return True

    if state_key == "search_query":
        clear_input_state(user_id)
        delete_user_message = bool((payload or {}).get("delete_user_messages", False))
        handle_search_with_flags(
            message,
            text,
            result_message_id=result_message_id or None,
            delete_user_message=delete_user_message,
        )
        cleanup_input_messages(
            chat_id,
            payload,
            None,
            keep_message_id=result_message_id or None,
        )
        return True

    if state_key == "bind_username":
        safe_delete_message(chat_id, get_message_id(message))
        payload = dict(payload or {})
        payload["username"] = text
        payload["delete_user_messages"] = True
        update_input_prompt(
            chat_id,
            user_id,
            "bind_password",
            "🔐 绑定 Emby\n\n请输入 Emby 密码：\n（发送后账号、密码和提示消息会保留在本卡片内）",
            payload,
        )
        return True

    if state_key == "bind_password":
        username = str((payload or {}).get("username") or "").strip()
        clear_input_state(user_id)
        cleanup_input_messages(chat_id, payload, message, keep_message_id=result_message_id or None)
        if not username:
            show_card_result(chat_id, payload, "❌ 绑定流程已失效，请重新点击“绑定 Emby”。", reply_markup=emby_keyboard())
            return True

        captured, err = capture_core_send_messages(lambda: core.handle_bind(message, f"{username} {text}"))
        if err:
            logging.exception("绑定 Emby 处理失败", exc_info=err)
            show_card_result(chat_id, payload, "❌ 绑定失败，请稍后重试。", reply_markup=emby_keyboard())
            return True
        render_captured_messages_to_card(
            chat_id,
            result_message_id,
            captured,
            "✅ Emby 账号绑定处理完成。",
            fallback_back_code="pm|emby",
        )
        return True

    if state_key == "unbind_password":
        clear_input_state(user_id)
        cleanup_input_messages(chat_id, payload, message, keep_message_id=result_message_id or None)
        captured, err = capture_core_send_messages(lambda: core.handle_unbind(message, text))
        if err:
            logging.exception("解绑 Emby 处理失败", exc_info=err)
            show_card_result(chat_id, payload, "❌ 解绑失败，请稍后重试。", reply_markup=emby_keyboard())
            return True
        render_captured_messages_to_card(
            chat_id,
            result_message_id,
            captured,
            "✅ 解绑处理完成。",
            fallback_back_code="pm|emby",
        )
        return True

    if state_key == "admin_find_user":
        clear_input_state(user_id)
        cleanup_input_messages(chat_id, payload, message, keep_message_id=result_message_id or None)
        record = search_user_record(text)
        if not record or core._safe_int(record.get("tg_id"), 0) <= 0:
            show_card_result(
                chat_id,
                payload,
                "❌ 没有找到这个用户，请重新输入 TG ID 或 @用户名。",
                reply_markup=user_manage_prompt_keyboard(),
                fallback_back_code="pm|users",
            )
            return True
        show_card_result(
            chat_id,
            payload,
            format_user_card(record),
            reply_markup=user_action_keyboard(core._safe_int(record.get("tg_id"), 0)),
            fallback_back_code="pm|users",
        )
        return True

    if state_key == "admin_quota_user_id":
        target_user_id = core._safe_int(text, 0)
        if target_user_id <= 0:
            cleanup_input_messages(chat_id, payload, message, keep_message_id=result_message_id or None)
            show_card_result(
                chat_id,
                payload,
                "❌ 请输入正确的用户 TG ID。",
                reply_markup=user_manage_prompt_keyboard(),
                fallback_back_code="pm|users",
            )
            clear_input_state(user_id)
            return True

        payload = dict(payload or {})
        payload["target_user_id"] = target_user_id
        payload["delete_user_messages"] = True
        safe_delete_message(chat_id, get_message_id(message))
        update_input_prompt(
            chat_id,
            user_id,
            "admin_set_quota",
            f"💳 修改额度\n\n用户 TG ID：{target_user_id}\n请输入新的总额度：",
            payload,
        )
        return True

    if state_key == "admin_set_quota":
        target_user_id = core._safe_int((payload or {}).get("target_user_id"), 0)
        new_quota = core._safe_int(text, -1)
        clear_input_state(user_id)
        cleanup_input_messages(chat_id, payload, message, keep_message_id=result_message_id or None)

        if target_user_id <= 0:
            show_card_result(chat_id, payload, "❌ 目标用户无效，请重新操作。", reply_markup=user_manage_prompt_keyboard(), fallback_back_code="pm|users")
            return True

        if new_quota < 0:
            show_card_result(chat_id, payload, "❌ 额度必须是大于等于 0 的整数。", reply_markup=user_action_keyboard(target_user_id), fallback_back_code="pm|users")
            return True

        record = core.set_user_quota(target_user_id, new_quota)
        if not record:
            show_card_result(chat_id, payload, "❌ 设置额度失败。", reply_markup=user_manage_prompt_keyboard(), fallback_back_code="pm|users")
            return True

        show_card_result(
            chat_id,
            payload,
            "✅ 额度已更新。\n\n" + format_user_card(record),
            reply_markup=user_action_keyboard(target_user_id),
            fallback_back_code="pm|users",
        )
        return True

    return False


# -------------------------
# 私聊消息入口
# -------------------------

def handle_message_private_menu(message: Dict[str, Any]) -> None:
    if not ensure_private_message(message):
        orig_handle_message(message)
        return

    chat_id = int((message.get("chat") or {}).get("id") or 0)
    text = (message.get("text") or "").strip()
    if not text:
        return

    from_user = message.get("from", {}) or {}
    user_id = core._safe_int(from_user.get("id"), 0)
    core.upsert_user_profile(from_user, chat_id)

    command, args = core.parse_command(text)

    # 优先处理状态输入；但保留命令跳出能力
    if not command:
        state_key, payload = get_input_state(user_id)
        if state_key:
            if handle_stateful_input(message, state_key, payload):
                return

    if command in ("/start", "/menu"):
        clear_input_state(user_id)
        show_home(chat_id, user_id)
        return

    if command == "/help":
        clear_input_state(user_id)
        show_help(chat_id)
        return

    if command == "/qp":
        clear_input_state(user_id)
        if args:
            handle_search_with_flags(message, args)
        else:
            send_new_input_state(
                chat_id,
                user_id,
                "search_query",
                "请输入你要搜索的片名 / 剧名：\n（直接在下方输入框发送即可）",
                delete_user_messages=False,
            )
        return

    if command == "/bd":
        clear_input_state(user_id)
        if args and len(args.split(maxsplit=1)) == 2:
            safe_delete_message(chat_id, get_message_id(message))
            captured, err = capture_core_send_messages(lambda: core.handle_bind(message, args))
            if err:
                logging.exception("手动绑定 Emby 失败", exc_info=err)
                send_message_result(chat_id, "❌ 绑定失败，请稍后重试。", reply_markup=emby_keyboard())
            else:
                render_captured_messages_to_card(chat_id, None, captured, "✅ Emby 账号绑定处理完成。", fallback_back_code="pm|emby")
        else:
            send_new_input_state(
                chat_id,
                user_id,
                "bind_username",
                "🔐 绑定 Emby\n\n请输入 Emby 账号：\n（发送后账号、密码和提示消息会保留在本卡片内）",
                delete_user_messages=True,
            )
            safe_delete_message(chat_id, get_message_id(message))
        return

    if command == "/jb":
        clear_input_state(user_id)
        if args:
            safe_delete_message(chat_id, get_message_id(message))
            captured, err = capture_core_send_messages(lambda: core.handle_unbind(message, args))
            if err:
                logging.exception("手动解绑 Emby 失败", exc_info=err)
                send_message_result(chat_id, "❌ 解绑失败，请稍后重试。", reply_markup=emby_keyboard())
            else:
                render_captured_messages_to_card(chat_id, None, captured, "✅ 解绑处理完成。", fallback_back_code="pm|emby")
        else:
            send_new_input_state(
                chat_id,
                user_id,
                "unbind_password",
                "🔓 解绑 Emby\n\n请输入当前 Emby 密码以确认解绑：\n（发送后密码和提示消息会保留在本卡片内）",
                delete_user_messages=True,
            )
            safe_delete_message(chat_id, get_message_id(message))
        return

    if command == "/cx":
        clear_input_state(user_id)
        show_quota_panel(chat_id, user_id)
        return

    if command == "/id":
        clear_input_state(user_id)
        send_message_result(chat_id, format_my_info(core.get_user_record(user_id)), reply_markup=pending_keyboard("pm|home"))
        return

    if command == "/bf":
        clear_input_state(user_id)
        captured, err = capture_core_send_messages(lambda: core.handle_bf(message))
        if err:
            logging.exception("在线播报查询失败", exc_info=err)
            send_message_result(chat_id, "❌ 查询在线播报失败，请稍后重试。", reply_markup=pending_keyboard("pm|home"))
        else:
            render_captured_messages_to_card(chat_id, None, captured, "❌ 没有可显示的在线播报结果。", fallback_back_code="pm|home")
        return

    if command == "/ed":
        clear_input_state(user_id)
        captured, err = capture_core_send_messages(lambda: core.handle_setquota(message, args))
        if err:
            logging.exception("手动修改额度失败", exc_info=err)
            send_message_result(chat_id, "❌ 修改额度失败，请检查参数。", reply_markup=pending_keyboard("pm|home"))
        else:
            render_captured_messages_to_card(chat_id, None, captured, "✅ 额度处理完成。", fallback_back_code="pm|home")
        return
        
    if command:
        send_message_result(chat_id, "❌ 未知命令，请发送 /help 查看可用命令。", reply_markup=pending_keyboard("pm|home"))
        return

    # 私聊里：允许直接发文字搜索
    if is_enabled("enable_private_text_search", True):
        handle_search_with_flags(message, text)
    else:
        send_message_result(
            chat_id,
            "ℹ️ 当前已关闭“私聊直接发文字搜索”，请点首页的“🎬 求片”按钮后按提示输入。",
            reply_markup=pending_keyboard("pm|home"),
        )
        show_home(chat_id, user_id)


# -------------------------
# 私聊按钮入口
# -------------------------

def handle_private_menu_callback(callback: Dict[str, Any]) -> bool:
    data = str((callback.get("data") or "").strip())
    if not data.startswith("pm|"):
        return False

    callback_id = callback.get("id")
    msg = callback.get("message") or {}
    chat_id = core._safe_int((msg.get("chat") or {}).get("id"), 0)
    message_id = core._safe_int(msg.get("message_id"), 0)
    from_user = callback.get("from", {}) or {}
    user_id = core._safe_int(from_user.get("id"), 0)
    core.upsert_user_profile(from_user, chat_id)
    clear_input_state(user_id)

    parts = data.split("|")
    action = parts[1] if len(parts) > 1 else ""

    if callback_id:
        try:
            core.answer_callback(callback_id, "好的", show_alert=False)
        except Exception:
            pass

    if action == "home":
        show_home(chat_id, user_id, message_id=message_id)
        return True

    if action == "help":
        show_help(chat_id, message_id=message_id)
        return True

    if action == "search":
        start_card_input_state(
            chat_id,
            user_id,
            "search_query",
            "🎬 求片搜索\n\n请输入你要搜索的片名 / 剧名：\n（发送后会直接在这张卡片上显示选择结果）",
            message_id,
            delete_source_prompt=False,
            delete_user_messages=True,
            refresh_code="pm|search",
        )
        return True

    if action == "emby":
        show_emby_panel(chat_id, user_id, message_id=message_id)
        return True

    if action == "bind":
        start_card_input_state(
            chat_id,
            user_id,
            "bind_username",
            "🔐 绑定 Emby\n\n请输入 Emby 账号：\n（发送后账号、密码和提示消息会保留在本卡片内）",
            message_id,
            delete_source_prompt=False,
            delete_user_messages=True,
            refresh_code="pm|bind",
        )
        return True

    if action == "unbind":
        start_card_input_state(
            chat_id,
            user_id,
            "unbind_password",
            "🔓 解绑 Emby\n\n请输入当前 Emby 密码以确认解绑：\n（发送后密码和提示消息会保留在本卡片内）",
            message_id,
            delete_source_prompt=False,
            delete_user_messages=True,
            refresh_code="pm|unbind",
        )
        return True

    if action == "quota":
        show_quota_panel(chat_id, user_id, message_id=message_id)
        return True

    if action == "pending":
        show_my_pending(chat_id, user_id, message_id=message_id)
        return True

    if action == "history":
        show_my_history(chat_id, user_id, message_id=message_id)
        return True

    if action == "me":
        edit_or_send_text(
            chat_id,
            format_my_info(core.get_user_record(user_id)),
            pending_keyboard("pm|home", refresh_code="pm|me"),
            message_id=message_id,
        )
        return True

    if action == "online":
        show_online_panel(chat_id, user_id, message_id=message_id)
        return True

    if action == "admin":
        show_admin_center(chat_id, user_id, message_id=message_id)
        return True

    if action == "stats":
        show_admin_stats(chat_id, message_id=message_id)
        return True

    if action == "apanel":
        show_admin_pending_panel(chat_id, message_id=message_id)
        return True

    if action == "settings":
        show_settings(chat_id, message_id=message_id)
        return True

    if action == "toggle":
        if not core.is_admin_user(user_id):
            edit_or_send_text(
                chat_id,
                "❌ 只有管理员可操作。",
                pending_keyboard("pm|home"),
                message_id=message_id,
            )
            return True
        setting_key = parts[2] if len(parts) > 2 else ""
        if setting_key:
            toggle_setting(setting_key)
        show_settings(chat_id, message_id=message_id)
        return True

    if action == "users":
        if not core.is_admin_user(user_id):
            edit_or_send_text(
                chat_id,
                "❌ 只有管理员可操作。",
                pending_keyboard("pm|home"),
                message_id=message_id,
            )
            return True

        clear_input_state(user_id)
        text = (
            "👥 用户管理\n\n"
            "请选择下方功能：\n"
            "1. 查询用户：输入 TG ID 或 @用户名查看详情\n"
            "2. 额度管理：先输入用户 TG ID，再输入新的额度"
        )
        edit_or_send_text(
            chat_id,
            text,
            user_manage_prompt_keyboard(),
            message_id=message_id,
        )
        return True

    if action == "finduser":
        if not core.is_admin_user(user_id):
            return True
        start_card_input_state(
            chat_id,
            user_id,
            "admin_find_user",
            "🔍 查询用户\n\n请输入要查询的用户 TG ID 或 @用户名：",
            message_id,
            delete_source_prompt=False,
            delete_user_messages=True,
            refresh_code="pm|finduser",
        )
        return True

    if action == "quota_manage":
        if not core.is_admin_user(user_id):
            return True
        start_card_input_state(
            chat_id,
            user_id,
            "admin_quota_user_id",
            "💳 额度管理\n\n请输入要修改额度的用户 TG ID：",
            message_id,
            delete_source_prompt=False,
            delete_user_messages=True,
            refresh_code="pm|quota_manage",
        )
        return True

    if action == "uquota":
        if not core.is_admin_user(user_id):
            return True

        target_user_id = core._safe_int(parts[2] if len(parts) > 2 else 0, 0)
        record = get_existing_user_record(target_user_id)
        if not record:
            edit_or_send_text(
                chat_id,
                "❌ 目标用户不存在。",
                pending_keyboard("pm|users"),
                message_id=message_id,
            )
            return True

        start_card_input_state(
            chat_id,
            user_id,
            "admin_set_quota",
            f"💳 修改额度\n\n用户 TG ID：{target_user_id}\n请输入新的总额度：",
            message_id,
            payload={"target_user_id": target_user_id},
            delete_source_prompt=False,
            delete_user_messages=True,
        )
        return True

    if action == "ureset":
        if not core.is_admin_user(user_id):
            return True

        target_user_id = core._safe_int(parts[2] if len(parts) > 2 else 0, 0)
        record = get_existing_user_record(target_user_id)
        if not record:
            edit_or_send_text(
                chat_id,
                "❌ 目标用户不存在。",
                pending_keyboard("pm|users"),
                message_id=message_id,
            )
            return True

        record["success_count"] = 0
        record["updated_at"] = _now()
        record = core._upsert_user_record_to_db(record)

        edit_or_send_text(
            chat_id,
            format_user_card(record),
            user_action_keyboard(target_user_id),
            message_id=message_id,
        )
        return True

    if action == "uunbind":
        if not core.is_admin_user(user_id):
            return True

        target_user_id = core._safe_int(parts[2] if len(parts) > 2 else 0, 0)
        record = get_existing_user_record(target_user_id)
        if not record:
            edit_or_send_text(
                chat_id,
                "❌ 目标用户不存在。",
                pending_keyboard("pm|users"),
                message_id=message_id,
            )
            return True

        record = core.clear_user_emby_binding(target_user_id) or {}
        if core._safe_int(record.get("tg_id"), 0) != target_user_id:
            record = get_existing_user_record(target_user_id)

        edit_or_send_text(
            chat_id,
            format_user_card(record),
            user_action_keyboard(target_user_id),
            message_id=message_id,
        )
        return True

    if action == "upending":
        if not core.is_admin_user(user_id):
            return True

        target_user_id = core._safe_int(parts[2] if len(parts) > 2 else 0, 0)
        record = get_existing_user_record(target_user_id)
        if not record:
            edit_or_send_text(
                chat_id,
                "❌ 目标用户不存在。",
                pending_keyboard("pm|users"),
                message_id=message_id,
            )
            return True

        rows = get_user_pending_requests(target_user_id, limit=10)
        lines = [f"📄 用户 {target_user_id} 的待审核\n"]
        if not rows:
            lines.append("当前没有待审核请求。")
        else:
            for idx, req in enumerate(rows, 1):
                meta = core.parse_request_id_meta(req.get("request_id") or "") or {}
                season_number = core._safe_int(meta.get("season_number"), 0)
                title = resolve_request_title(
                    str(req.get("media_type") or ""),
                    core._safe_int(req.get("tmdb_id"), 0),
                    season_number,
                )
                lines.append(f"{idx}. {title}\n   时间：{req.get('created_at') or '未知'}\n")

        edit_or_send_text(
            chat_id,
            "\n".join(lines).strip(),
            user_action_keyboard(target_user_id),
            message_id=message_id,
        )
        return True

    if action == "admins":
        if not core.is_admin_user(user_id):
            edit_or_send_text(
                chat_id,
                "❌ 只有管理员可操作。",
                pending_keyboard("pm|home"),
                message_id=message_id,
            )
            return True
        edit_or_send_text(chat_id, "👮 管理员管理", admin_manage_keyboard(user_id), message_id=message_id)
        return True

    if action == "adminlist":
        if not core.is_admin_user(user_id):
            return True
        show_admin_list(chat_id, user_id, message_id=message_id)
        return True

    if action in {
        "addadmin",
        "deladmin",
        "deladminquick",
        "adminnoop",
        "admintoggle",
        "adminclearselect",
        "admindelselected",
    }:
        edit_or_send_text(
            chat_id,
            "ℹ️ 管理员现在仅从 .env 和 OWNER_CHAT_ID 读取，运行中不可增删。",
            pending_keyboard("pm|admins"),
            message_id=message_id,
        )
        return True


    return True

# -------------------------
# 回调总入口
# - pm|... 由本文件处理
# - s/ss（用户选片/选季）由本文件处理，以支持功能开关
# - a/r（管理员审核）交给原 app.py 处理
# -------------------------
def call_orig_callback_with_request_ctx(callback: Dict[str, Any]) -> None:
    data = str((callback.get("data") or "").strip())
    action_code = data.split("|", 1)[0] if data else ""

    request_id = ""
    if action_code in ("a", "r"):
        try:
            _, request_id = data.split("|", 1)
        except Exception:
            request_id = ""

    old_request_id = str(getattr(_REQUEST_CTX, "request_id", "") or "").strip()

    try:
        if request_id:
            _REQUEST_CTX.request_id = request_id
        orig_handle_callback_query(callback)
    finally:
        if old_request_id:
            _REQUEST_CTX.request_id = old_request_id
        else:
            try:
                delattr(_REQUEST_CTX, "request_id")
            except Exception:
                pass


def submit_user_request_to_current_card(
    callback_message: Dict[str, Any],
    requester_user_id: int,
    requester_chat_id: int,
    requester_name: str,
    media_type: str,
    detail: Dict[str, Any],
    season_number: Optional[int] = None,
) -> None:
    card_chat_id = core._safe_int((callback_message.get("chat") or {}).get("id"), requester_chat_id)
    card_message_id = core._safe_int(callback_message.get("message_id"), 0)

    captured, err = capture_core_send_messages(lambda: core.submit_user_request(
        callback_message=callback_message,
        requester_user_id=requester_user_id,
        requester_chat_id=requester_chat_id,
        requester_name=requester_name,
        media_type=media_type,
        detail=detail,
        season_number=season_number,
    ))

    if err:
        logging.exception("提交求片失败", exc_info=err)
        edit_or_send_text(
            card_chat_id,
            f"❌ 提交失败：{err}",
            pending_keyboard("pm|home"),
            message_id=card_message_id or None,
        )
        return

    render_captured_messages_to_card(
        card_chat_id,
        card_message_id,
        captured,
        "✅ 已处理你的求片请求。",
        fallback_back_code="pm|home",
    )


def handle_callback_query_private_menu(callback: Dict[str, Any]) -> None:
    if not ensure_callback_private(callback):
        call_orig_callback_with_request_ctx(callback)
        return

    if handle_private_menu_callback(callback):
        return

    data = str((callback.get("data") or "").strip())
    action_code = data.split("|", 1)[0] if data else ""
    callback_id = callback.get("id")
    from_user = callback.get("from", {}) or {}
    from_user_id = core._safe_int(from_user.get("id"), 0)
    callback_message = callback.get("message", {}) or {}
    callback_chat_id = core._safe_int((callback_message.get("chat") or {}).get("id"), 0)
    core.upsert_user_profile(from_user, callback_chat_id)

    # 用户点击搜索结果/选季
    if action_code in ("s", "ss"):
        fake_message = {
            "chat": callback_message.get("chat") or {"id": callback_chat_id, "type": "private"},
            "from": from_user,
            "text": "",
        }

        if is_enabled("force_bind", True) and (not from_user_id or not core.is_user_emby_bound(from_user_id)):
            if callback_id:
                try:
                    core.answer_callback(callback_id, "请先绑定 Emby 账号", show_alert=True)
                except Exception:
                    pass
            edit_or_send_text(
                callback_chat_id,
                "❌ 你还没有绑定 Emby 账号，绑定后才能继续求片。",
                emby_keyboard(),
                message_id=core._safe_int(callback_message.get("message_id"), 0) or None,
            )
            return

        if action_code == "s":
            try:
                parts = data.split("|")
                if len(parts) < 5:
                    raise ValueError("按钮数据不完整")

                _, media_type, tmdb_id_str, requester_user_id_str, reply_chat_id_str = parts[:5]
                command_message_id = core._safe_int(parts[5], 0) if len(parts) >= 6 else 0

                tmdb_id = int(tmdb_id_str)
                requester_user_id = int(requester_user_id_str)
                requester_chat_id = int(reply_chat_id_str)
            except Exception:
                core.answer_callback(callback_id, "按钮数据无效", show_alert=True)
                return

            quota_record = core.get_user_record(from_user_id)
            remaining_quota = raw_remaining_quota(quota_record)
            if is_enabled("enable_quota_limit", True) and remaining_quota <= 0:
                core.answer_callback(callback_id, "你的求片额度已用完", show_alert=True)
                edit_or_send_text(
                    requester_chat_id,
                    "❌ 你的求片额度已用完，无法继续提交求片。\n发送 /cx 查看剩余额度。",
                    pending_keyboard("pm|home"),
                    message_id=core._safe_int(callback_message.get("message_id"), 0) or None,
                )
                return

            try:
                detail = core.get_detail_with_fallback(media_type, tmdb_id)
            except Exception as e:
                logging.exception("获取 TMDB 详情失败")
                core.answer_callback(callback_id, "获取详情失败", show_alert=True)
                edit_or_send_text(
                    requester_chat_id,
                    f"❌ 获取详情失败：{e}",
                    pending_keyboard("pm|home"),
                    message_id=core._safe_int(callback_message.get("message_id"), 0) or None,
                )
                return

            if media_type == "tv" and core._safe_int(detail.get("number_of_seasons"), 0) > 1:
                try:
                    core.edit_message_text(
                        chat_id=int(callback_message["chat"]["id"]),
                        message_id=int(callback_message["message_id"]),
                        text=core.build_tv_season_choice_text(detail),
                        parse_mode="MarkdownV2",
                        reply_markup=append_global_home_button(core.build_tv_season_select_keyboard(
                            detail,
                            requester_user_id=requester_user_id,
                            reply_chat_id=requester_chat_id,
                            command_message_id=command_message_id,
                        )),
                        disable_web_page_preview=True,
                    )
                    core.answer_callback(callback_id, "请选择季数", show_alert=False)
                except Exception:
                    logging.exception("切换到季选择界面失败")
                    core.answer_callback(callback_id, "请选择季数失败", show_alert=True)
                return

            requester_name = (
                f"@{from_user.get('username')}"
                if from_user.get("username")
                else (((from_user.get("first_name") or "") + " " + (from_user.get("last_name") or "")).strip() or str(from_user_id))
            )

            core.answer_callback(callback_id, "正在提交审核...", show_alert=False)
            submit_user_request_to_current_card(
                callback_message=callback_message,
                requester_user_id=requester_user_id,
                requester_chat_id=requester_chat_id,
                requester_name=requester_name,
                media_type=media_type,
                detail=detail,
                season_number=None,
            )
            return

        if action_code == "ss":
            try:
                parts = data.split("|")
                if len(parts) < 5:
                    raise ValueError("按钮数据不完整")

                _, tmdb_id_str, season_number_str, requester_user_id_str, reply_chat_id_str = parts[:5]
                command_message_id = core._safe_int(parts[5], 0) if len(parts) >= 6 else 0

                tmdb_id = int(tmdb_id_str)
                season_number = int(season_number_str)
                requester_user_id = int(requester_user_id_str)
                requester_chat_id = int(reply_chat_id_str)
            except Exception:
                core.answer_callback(callback_id, "按钮数据无效", show_alert=True)
                return

            if from_user_id != requester_user_id:
                core.answer_callback(callback_id, "这不是你的按钮", show_alert=True)
                return

            quota_record = core.get_user_record(from_user_id)
            remaining_quota = raw_remaining_quota(quota_record)
            if is_enabled("enable_quota_limit", True) and remaining_quota <= 0:
                core.answer_callback(callback_id, "你的求片额度已用完", show_alert=True)
                edit_or_send_text(
                    requester_chat_id,
                    "❌ 你的求片额度已用完，无法继续提交求片。\n发送 /cx 查看剩余额度。",
                    pending_keyboard("pm|home"),
                    message_id=core._safe_int(callback_message.get("message_id"), 0) or None,
                )
                return

            requester_name = (
                f"@{from_user.get('username')}"
                if from_user.get("username")
                else (((from_user.get("first_name") or "") + " " + (from_user.get("last_name") or "")).strip() or str(from_user_id))
            )

            core.answer_callback(callback_id, "正在提交审核...", show_alert=False)
            try:
                detail = core.get_detail_with_fallback("tv", tmdb_id)
            except Exception as e:
                logging.exception("处理用户选季失败")
                edit_or_send_text(
                    requester_chat_id,
                    f"❌ 提交失败：{e}",
                    pending_keyboard("pm|home"),
                    message_id=core._safe_int(callback_message.get("message_id"), 0) or None,
                )
                return

            submit_user_request_to_current_card(
                callback_message=callback_message,
                requester_user_id=requester_user_id,
                requester_chat_id=requester_chat_id,
                requester_name=requester_name,
                media_type="tv",
                detail=detail,
                season_number=season_number,
            )
            return

    # 管理员审核 a/r 仍然走原逻辑
    call_orig_callback_with_request_ctx(callback)


core.handle_message = handle_message_private_menu
core.handle_callback_query = handle_callback_query_private_menu


# -------------------------
# 运行入口
# -------------------------
def check_bot_privacy_mode() -> None:
    GREEN = "\033[1;32m"   # 绿色字体
    YELLOW = "\033[1;33m"  # 黄色字体
    RESET = "\033[0m"      # 恢复默认颜色

    try:
        me = core.tg_api("getMe", {})
        username = str(me.get("username") or "").strip()
        can_read_all = bool(me.get("can_read_all_group_messages"))

        if can_read_all:
            logging.info(
                "✅ %s[隐私模式已禁用] Bot @%s 可以接收群普通消息%s",
                GREEN,
                username or "unknown",
                RESET,
            )
        else:
            logging.warning(
                "❌ %s[隐私模式仍然开启] Bot @%s 群普通消息可能收不到！"
                "请到 @BotFather 执行 /setprivacy，选择你的 bot，然后点 Disable%s",
                YELLOW,
                username or "unknown",
                RESET,
            )

    except Exception:
        logging.exception(
            "⚠️ %s[隐私模式检测失败] 无法检测 Telegram Bot 隐私模式%s",
            YELLOW,
            RESET,
        )
def main() -> None:
    logging.info("私聊按钮增强版入口启动中...")
    core.init_sqlite_db()

    # 启动时检测 Telegram Bot 隐私模式
    check_bot_privacy_mode()

    init_private_menu_db()
    core.cleanup_expired_pending_requests()

    # 启动时自动设置左下角私聊命令菜单
    setup_private_commands()

    Thread(target=core.pending_cleanup_loop, daemon=True).start()
    Thread(target=core.emby_settlement_loop, daemon=True).start()
    Thread(target=core.run_http_server, daemon=True).start()
    core.poll_forever()


if __name__ == "__main__":
    main()