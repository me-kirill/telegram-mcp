import os
import sys
import json
import time
import asyncio
import sqlite3
import logging
import mimetypes
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import List, Dict, Optional, Union, Any

# Third-party libraries
import nest_asyncio
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pythonjsonlogger import jsonlogger
from telethon import TelegramClient, functions, utils, events
from telethon.sessions import StringSession
from telethon.tl.types import (
    User,
    Chat,
    Channel,
    ChatAdminRights,
    ChatBannedRights,
    ChannelParticipantsKicked,
    ChannelParticipantsAdmins,
    InputChatPhoto,
    InputChatUploadedPhoto,
    InputChatPhotoEmpty,
    InputPeerUser,
    InputPeerChat,
    InputPeerChannel,
    PeerChannel,
    UpdateNewMessage,
    UpdateNewChannelMessage,
    UpdateEditMessage,
    UpdateEditChannelMessage,
    UpdateDeleteMessages,
    UpdateDeleteChannelMessages,
    updates,
)
import re
from functools import wraps
import telethon.errors.rpcerrorlist
from storage import (
    SQLiteStore,
    build_chat_record,
    build_message_record,
    decode_db_bytes,
)


class ValidationError(Exception):
    """Custom exception for validation errors."""

    pass


def json_serializer(obj):
    """Helper function to convert non-serializable objects for JSON serialization."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    # Add other non-serializable types as needed
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


load_dotenv()

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME")

# Check if a string session exists in environment, otherwise use file-based session
SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING")

mcp = FastMCP("telegram")

if SESSION_STRING:
    # Use the string session if available
    telegram_client = TelegramClient(
        StringSession(SESSION_STRING), TELEGRAM_API_ID, TELEGRAM_API_HASH
    )
else:
    # Use file-based session
    telegram_client = TelegramClient(TELEGRAM_SESSION_NAME, TELEGRAM_API_ID, TELEGRAM_API_HASH)

# Setup robust logging with both file and console output
logger = logging.getLogger("telegram_mcp")
logger.setLevel(logging.ERROR)  # Set to ERROR for production, INFO for debugging

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)  # Set to ERROR for production, INFO for debugging

# Create file handler with absolute path
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(script_dir, "mcp_errors.log")

try:
    file_handler = logging.FileHandler(log_file_path, mode="a")  # Append mode
    file_handler.setLevel(logging.ERROR)

    # Create formatters
    # Console formatter remains in the old format
    console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    console_handler.setFormatter(console_formatter)

    # File formatter is now JSON
    json_formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    file_handler.setFormatter(json_formatter)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.info(f"Logging initialized to {log_file_path}")
except Exception as log_error:
    print(f"WARNING: Error setting up log file: {log_error}")
    # Fallback to console-only logging
    logger.addHandler(console_handler)
    logger.error(f"Failed to set up log file handler: {log_error}")

# SQLite storage
DB_PATH = os.getenv("TELEGRAM_MCP_DB_PATH", os.path.join(script_dir, "telegram_mcp.sqlite3"))
store = SQLiteStore(DB_PATH)
store.init_schema()


class StoreBackedClient:
    def __init__(self, raw_client: TelegramClient, store: SQLiteStore) -> None:
        self.raw = raw_client
        self.store = store

    def __getattr__(self, name: str) -> Any:
        return getattr(self.raw, name)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.raw(*args, **kwargs)

    def _safe_setattr(self, obj: Any, name: str, value: Any) -> None:
        try:
            setattr(obj, name, value)
        except Exception:
            try:
                object.__setattr__(obj, name, value)
            except Exception:
                return

    def _parse_payload(self, raw_json: Optional[str]) -> Optional[Dict[str, Any]]:
        if not raw_json:
            return None
        try:
            return json.loads(raw_json)
        except json.JSONDecodeError:
            return None

    def _peer_id_to_raw_id(self, peer_id: int) -> int:
        if peer_id <= -1000000000000:
            return -peer_id - 1000000000000
        if peer_id < 0:
            return -peer_id
        return peer_id

    def _resolve_chat_id(self, entity: Any) -> Optional[int]:
        if isinstance(entity, (int, str)):
            return resolve_chat_identifier(entity)
        if hasattr(entity, "id"):
            return resolve_chat_identifier(getattr(entity, "id"))
        return None

    def _buttons_from_payload(
        self, payload: Optional[Dict[str, Any]]
    ) -> Optional[List[List[Any]]]:
        if not payload:
            return None
        reply_markup = payload.get("reply_markup")
        if not isinstance(reply_markup, dict):
            return None
        rows = reply_markup.get("rows") or []
        output_rows: List[List[Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            buttons = row.get("buttons") or []
            row_buttons = []
            for button in buttons:
                if not isinstance(button, dict):
                    continue
                text = button.get("text") or ""
                data = button.get("data")
                if data is not None:
                    decoded = decode_db_bytes(data)
                    if decoded is not None:
                        data = decoded
                url = button.get("url")
                raw_button = type("RawButton", (), {"url": url})()
                row_buttons.append(
                    type("Button", (), {"text": text, "data": data, "button": raw_button})()
                )
            if row_buttons:
                output_rows.append(row_buttons)
        return output_rows or None

    def _message_from_row(self, row: Dict[str, Any]) -> Any:
        if not row:
            return None
        payload = self._parse_payload(row.get("raw_json"))

        reply_to_id = row.get("reply_to_msg_id")
        if reply_to_id is None and payload:
            reply_payload = payload.get("reply_to")
            if isinstance(reply_payload, dict):
                reply_to_id = reply_payload.get("reply_to_msg_id")

        reply_to = None
        if reply_to_id is not None:
            reply_to = type("Reply", (), {"reply_to_msg_id": reply_to_id})()

        sender = None
        sender_name = row.get("sender_name")
        if sender_name:
            name_parts = sender_name.split(" ", 1)
            sender = type(
                "Sender",
                (),
                {
                    "first_name": name_parts[0],
                    "last_name": name_parts[1] if len(name_parts) > 1 else "",
                    "title": sender_name,
                },
            )()

        media = None
        if payload and payload.get("media") is not None:
            media = payload.get("media")
        elif row.get("has_media"):
            media_type = row.get("media_type") or "Media"
            media = type(media_type, (), {})()

        date = None
        if row.get("date") is not None:
            date = datetime.fromtimestamp(row["date"], tz=timezone.utc)

        buttons = self._buttons_from_payload(payload)

        return type(
            "StoreMessage",
            (),
            {
                "id": row["message_id"],
                "message": row.get("text") or "",
                "date": date,
                "reply_to": reply_to,
                "sender": sender,
                "sender_id": row.get("sender_id"),
                "media": media,
                "pinned": bool(row.get("pinned")),
                "out": bool(row.get("out")),
                "chat_id": row.get("chat_id"),
                "buttons": buttons,
            },
        )()

    def _entity_from_row(self, row: Dict[str, Any]) -> Any:
        if row is None:
            return None

        peer_id = row["id"]
        payload = self._parse_payload(row.get("raw_json"))
        is_channel_like = peer_id <= -1000000000000
        if payload and payload.get("_") in {"Channel", "ChannelForbidden"}:
            is_channel_like = True

        if row.get("type") == "user":
            entity = User.__new__(User)
            entity_id = self._peer_id_to_raw_id(peer_id)
            name = row.get("title") or ""
            name_parts = name.split(" ", 1)
            self._safe_setattr(entity, "id", entity_id)
            self._safe_setattr(entity, "first_name", name_parts[0] if name_parts else "")
            self._safe_setattr(
                entity,
                "last_name",
                name_parts[1] if len(name_parts) > 1 else "",
            )
            self._safe_setattr(entity, "username", row.get("username"))
            self._safe_setattr(entity, "phone", row.get("phone"))
            if row.get("is_bot") is not None:
                self._safe_setattr(entity, "bot", bool(row.get("is_bot")))
            if row.get("is_verified") is not None:
                self._safe_setattr(entity, "verified", bool(row.get("is_verified")))
            return entity

        if row.get("type") == "channel" or is_channel_like:
            entity = Channel.__new__(Channel)
            entity_id = self._peer_id_to_raw_id(peer_id)
            broadcast = row.get("type") == "channel"
            self._safe_setattr(entity, "id", entity_id)
            self._safe_setattr(entity, "title", row.get("title") or "Unknown")
            self._safe_setattr(entity, "username", row.get("username"))
            self._safe_setattr(entity, "broadcast", bool(broadcast))
            self._safe_setattr(entity, "megagroup", bool(not broadcast))
            return entity

        entity = Chat.__new__(Chat)
        entity_id = self._peer_id_to_raw_id(peer_id)
        self._safe_setattr(entity, "id", entity_id)
        self._safe_setattr(entity, "title", row.get("title") or "Unknown")
        return entity

    def _dialog_from_row(self, row: Dict[str, Any]) -> Any:
        entity = self._entity_from_row(row)
        last_msg = self.store.get_last_message(row["id"])
        message = self._message_from_row(last_msg) if last_msg else None
        return type(
            "StoreDialog",
            (),
            {
                "entity": entity,
                "unread_count": row.get("unread_count") or 0,
                "message": message,
                "id": getattr(entity, "id", row["id"]),
            },
        )()

    async def get_dialogs(self, limit: Optional[int] = None, **kwargs: Any) -> List[Any]:
        offset_peer = kwargs.get("offset_peer")
        if offset_peer is not None:
            chat_id = self._resolve_chat_id(offset_peer)
            if chat_id is None:
                return []
            row = self.store.get_chat_by_id(chat_id)
            if not row:
                return []
            return [self._dialog_from_row(row)]

        if limit == 0:
            return []

        if limit is None:
            rows = self.store.conn.execute(
                """
                SELECT * FROM chats
                ORDER BY COALESCE(last_message_date, 0) DESC, id DESC
                """
            ).fetchall()
            rows = [dict(row) for row in rows]
        else:
            rows = self.store.get_chats(page=1, page_size=limit)
        return [self._dialog_from_row(row) for row in rows]

    async def get_messages(
        self,
        entity: Any,
        limit: int = 20,
        add_offset: int = 0,
        ids: Optional[Union[int, List[int]]] = None,
        search: Optional[str] = None,
        max_id: Optional[int] = None,
        min_id: Optional[int] = None,
        reverse: bool = False,
        filter: Optional[Any] = None,
        **kwargs: Any,
    ) -> Any:
        chat_id = self._resolve_chat_id(entity)

        if chat_id is None:
            return [] if ids is None else None

        if ids is not None:
            if isinstance(ids, list):
                messages = []
                for msg_id in ids:
                    row = self.store.get_message(chat_id, msg_id)
                    if row:
                        messages.append(self._message_from_row(row))
                return messages
            row = self.store.get_message(chat_id, ids)
            return self._message_from_row(row)

        pinned_only = False
        if filter is not None:
            try:
                from telethon.tl.types import InputMessagesFilterPinned

                if isinstance(filter, InputMessagesFilterPinned):
                    pinned_only = True
            except Exception:
                pinned_only = False

        rows = self._fetch_message_rows(
            chat_id=chat_id,
            limit=limit,
            offset=add_offset,
            search=search,
            max_id=max_id,
            min_id=min_id,
            reverse=reverse,
            pinned_only=pinned_only,
        )
        return [self._message_from_row(row) for row in rows]

    def _fetch_message_rows(
        self,
        chat_id: int,
        limit: Optional[int] = None,
        offset: int = 0,
        search: Optional[str] = None,
        max_id: Optional[int] = None,
        min_id: Optional[int] = None,
        reverse: bool = False,
        offset_date: Optional[datetime] = None,
        pinned_only: bool = False,
    ) -> List[Dict[str, Any]]:
        conditions = ["chat_id = ?"]
        params: List[Any] = [chat_id]

        if search:
            conditions.append("text LIKE ? COLLATE NOCASE")
            params.append(f"%{search}%")
        if max_id is not None:
            conditions.append("message_id < ?")
            params.append(max_id)
        if min_id is not None:
            conditions.append("message_id > ?")
            params.append(min_id)
        if offset_date is not None:
            if offset_date.tzinfo is None:
                offset_date = offset_date.replace(tzinfo=timezone.utc)
            offset_ts = int(offset_date.timestamp())
            conditions.append("date >= ?" if reverse else "date < ?")
            params.append(offset_ts)
        if pinned_only:
            conditions.append("pinned = 1")

        where_clause = " AND ".join(conditions)
        order = "date ASC, message_id ASC" if reverse else "date DESC, message_id DESC"
        query = f"SELECT * FROM messages WHERE {where_clause} ORDER BY {order}"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
            if offset:
                query += " OFFSET ?"
                params.append(offset)
        elif offset:
            query += " LIMIT -1 OFFSET ?"
            params.append(offset)

        rows = self.store.conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    async def iter_messages(self, entity: Any, limit: int = 100, **kwargs: Any) -> Any:
        chat_id = self._resolve_chat_id(entity)

        if chat_id is None:
            return

        search = kwargs.get("search")
        offset_date = kwargs.get("offset_date")
        reverse = bool(kwargs.get("reverse", False))
        max_id = kwargs.get("max_id")
        min_id = kwargs.get("min_id")

        rows = self._fetch_message_rows(
            chat_id=chat_id,
            limit=limit,
            offset=0,
            search=search,
            max_id=max_id,
            min_id=min_id,
            reverse=reverse,
            offset_date=offset_date,
        )
        for row in rows:
            yield self._message_from_row(row)


client = StoreBackedClient(telegram_client, store)


# Error code prefix mapping for better error tracing
class ErrorCategory(str, Enum):
    CHAT = "CHAT"
    MSG = "MSG"
    CONTACT = "CONTACT"
    GROUP = "GROUP"
    MEDIA = "MEDIA"
    PROFILE = "PROFILE"
    AUTH = "AUTH"
    ADMIN = "ADMIN"


def log_and_format_error(
    function_name: str,
    error: Exception,
    prefix: Optional[Union[ErrorCategory, str]] = None,
    user_message: str = None,
    **kwargs,
) -> str:
    """
    Centralized error handling function.

    Logs an error and returns a formatted, user-friendly message.

    Args:
        function_name: Name of the function where the error occurred.
        error: The exception that was raised.
        prefix: Error code prefix (e.g., ErrorCategory.CHAT, "VALIDATION-001").
            If None, it will be derived from the function_name.
        user_message: A custom user-facing message to return. If None, a generic one is created.
        **kwargs: Additional context parameters to include in the log.

    Returns:
        A user-friendly error message with an error code.
    """
    # Generate a consistent error code
    if isinstance(prefix, str) and prefix == "VALIDATION-001":
        # Special case for validation errors
        error_code = prefix
    else:
        if prefix is None:
            # Try to derive prefix from function name
            for category in ErrorCategory:
                if category.name.lower() in function_name.lower():
                    prefix = category
                    break

        prefix_str = prefix.value if isinstance(prefix, ErrorCategory) else (prefix or "GEN")
        error_code = f"{prefix_str}-ERR-{abs(hash(function_name)) % 1000:03d}"

    # Format the additional context parameters
    context = ", ".join(f"{k}={v}" for k, v in kwargs.items())

    # Log the full technical error
    logger.error(f"Error in {function_name} ({context}) - Code: {error_code}", exc_info=True)

    # Return a user-friendly message
    if user_message:
        return user_message

    return f"An error occurred (code: {error_code}). Check mcp_errors.log for details."


def validate_id(*param_names_to_validate):
    """
    Decorator to validate chat_id and user_id parameters, including lists of IDs.
    It checks for valid integer ranges, string representations of integers,
    and username formats.
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for param_name in param_names_to_validate:
                if param_name not in kwargs or kwargs[param_name] is None:
                    continue

                param_value = kwargs[param_name]

                def validate_single_id(value, p_name):
                    # Handle integer IDs
                    if isinstance(value, int):
                        if not (-(2**63) <= value <= 2**63 - 1):
                            return (
                                None,
                                f"Invalid {p_name}: {value}. ID is out of the valid integer range.",
                            )
                        return value, None

                    # Handle string IDs
                    if isinstance(value, str):
                        try:
                            int_value = int(value)
                            if not (-(2**63) <= int_value <= 2**63 - 1):
                                return (
                                    None,
                                    f"Invalid {p_name}: {value}. ID is out of the valid integer range.",
                                )
                            return int_value, None
                        except ValueError:
                            if re.match(r"^@?[a-zA-Z0-9_]{5,}$", value):
                                return value, None
                            else:
                                return (
                                    None,
                                    f"Invalid {p_name}: '{value}'. Must be a valid integer ID, or a username string.",
                                )

                    # Handle other invalid types
                    return (
                        None,
                        f"Invalid {p_name}: {value}. Type must be an integer or a string.",
                    )

                if isinstance(param_value, list):
                    validated_list = []
                    for item in param_value:
                        validated_item, error_msg = validate_single_id(item, param_name)
                        if error_msg:
                            return log_and_format_error(
                                func.__name__,
                                ValidationError(error_msg),
                                prefix="VALIDATION-001",
                                user_message=error_msg,
                                **{param_name: param_value},
                            )
                        validated_list.append(validated_item)
                    kwargs[param_name] = validated_list
                else:
                    validated_value, error_msg = validate_single_id(param_value, param_name)
                    if error_msg:
                        return log_and_format_error(
                            func.__name__,
                            ValidationError(error_msg),
                            prefix="VALIDATION-001",
                            user_message=error_msg,
                            **{param_name: param_value},
                        )
                    kwargs[param_name] = validated_value

            return await func(*args, **kwargs)

        return wrapper

    return decorator


def format_entity(entity) -> Dict[str, Any]:
    """Helper function to format entity information consistently."""
    result = {"id": entity.id}

    if hasattr(entity, "title"):
        result["name"] = entity.title
        result["type"] = "group" if isinstance(entity, Chat) else "channel"
    elif hasattr(entity, "first_name"):
        name_parts = []
        if entity.first_name:
            name_parts.append(entity.first_name)
        if hasattr(entity, "last_name") and entity.last_name:
            name_parts.append(entity.last_name)
        result["name"] = " ".join(name_parts)
        result["type"] = "user"
        if hasattr(entity, "username") and entity.username:
            result["username"] = entity.username
        if hasattr(entity, "phone") and entity.phone:
            result["phone"] = entity.phone

    return result


def format_message(message) -> Dict[str, Any]:
    """Helper function to format message information consistently."""
    result = {
        "id": message.id,
        "date": message.date.isoformat(),
        "text": message.message or "",
    }

    if message.from_id:
        result["from_id"] = utils.get_peer_id(message.from_id)

    if message.media:
        result["has_media"] = True
        result["media_type"] = type(message.media).__name__

    return result


def get_sender_name(message) -> str:
    """Helper function to get sender name from a message."""
    if not message.sender:
        return "Unknown"

    # Check for group/channel title first
    if hasattr(message.sender, "title") and message.sender.title:
        return message.sender.title
    elif hasattr(message.sender, "first_name"):
        # User sender
        first_name = getattr(message.sender, "first_name", "") or ""
        last_name = getattr(message.sender, "last_name", "") or ""
        full_name = f"{first_name} {last_name}".strip()
        return full_name if full_name else "Unknown"
    else:
        return "Unknown"


def format_db_date(timestamp: Optional[int]) -> str:
    if timestamp is None:
        return "Unknown"
    return datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def resolve_chat_identifier(chat_id: Union[int, str]) -> Optional[int]:
    if isinstance(chat_id, int):
        if store.get_chat_by_id(chat_id):
            return chat_id
        if chat_id > 0:
            candidate = -chat_id
            if store.get_chat_by_id(candidate):
                return candidate
            channel_candidate = -1000000000000 - chat_id
            if store.get_chat_by_id(channel_candidate):
                return channel_candidate
        if chat_id < 0:
            candidate = -chat_id
            if store.get_chat_by_id(candidate):
                return candidate
            if chat_id <= -1000000000000:
                channel_candidate = -chat_id - 1000000000000
                if store.get_chat_by_id(channel_candidate):
                    return channel_candidate
        return None
    if isinstance(chat_id, str):
        trimmed = chat_id.strip()
        if trimmed.lstrip("-").isdigit():
            numeric = int(trimmed)
            if store.get_chat_by_id(numeric):
                return numeric
            if numeric > 0:
                candidate = -numeric
                if store.get_chat_by_id(candidate):
                    return candidate
                channel_candidate = -1000000000000 - numeric
                if store.get_chat_by_id(channel_candidate):
                    return channel_candidate
            if numeric < 0:
                candidate = -numeric
                if store.get_chat_by_id(candidate):
                    return candidate
                if numeric <= -1000000000000:
                    channel_candidate = -numeric - 1000000000000
                    if store.get_chat_by_id(channel_candidate):
                        return channel_candidate
            return None
        row = store.get_chat_by_username(trimmed)
        if row:
            return row["id"]
    return None


def get_chat_row(chat_id: Union[int, str]) -> Optional[Dict[str, Any]]:
    resolved_id = resolve_chat_identifier(chat_id)
    if resolved_id is None:
        return None
    return store.get_chat_by_id(resolved_id)


def extract_inline_buttons(raw_json: str) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(raw_json) if raw_json else {}
    except json.JSONDecodeError:
        return []

    reply_markup = payload.get("reply_markup") or {}
    rows = reply_markup.get("rows") or []
    buttons: List[Dict[str, Any]] = []

    for row in rows:
        for button in row.get("buttons", []) or []:
            text = button.get("text") or "<no text>"
            url = button.get("url")
            data_raw = button.get("data")
            data_bytes = decode_db_bytes(data_raw)
            buttons.append(
                {
                    "text": text,
                    "url": url,
                    "data": data_bytes,
                }
            )

    return buttons


def _normalize_state_date(state_date: Any) -> int:
    if isinstance(state_date, datetime):
        if state_date.tzinfo is None:
            state_date = state_date.replace(tzinfo=timezone.utc)
        return int(state_date.timestamp())
    return int(state_date)


def _load_updates_state() -> Optional[Dict[str, int]]:
    pts = store.get_meta_int("updates_pts")
    qts = store.get_meta_int("updates_qts")
    date = store.get_meta_int("updates_date")
    seq = store.get_meta_int("updates_seq")
    if pts is None or qts is None or date is None:
        return None
    return {"pts": pts, "qts": qts, "date": date, "seq": seq or 0}


def _save_updates_state(pts: int, qts: int, date: int, seq: int) -> None:
    store.set_meta_int("updates_pts", pts)
    store.set_meta_int("updates_qts", qts)
    store.set_meta_int("updates_date", date)
    store.set_meta_int("updates_seq", seq)


def _peer_id_from_channel_id(channel_id: int) -> int:
    return utils.get_peer_id(PeerChannel(channel_id))


def _apply_message_record(record: Dict[str, Any], increment_unread: bool = True) -> None:
    if not record.get("chat_id"):
        return
    store.upsert_message_record(record)
    if increment_unread and not record.get("out"):
        store.increment_unread(record["chat_id"])


def _apply_difference_update(update: Any) -> None:
    if isinstance(update, (UpdateNewMessage, UpdateNewChannelMessage)):
        msg = getattr(update, "message", None)
        if msg:
            record = build_message_record(msg)
            if record.get("chat_id") is None and getattr(update, "channel_id", None):
                record["chat_id"] = _peer_id_from_channel_id(update.channel_id)
            _apply_message_record(record, increment_unread=True)
        return

    if isinstance(update, (UpdateEditMessage, UpdateEditChannelMessage)):
        msg = getattr(update, "message", None)
        if msg:
            record = build_message_record(msg)
            if record.get("chat_id") is None and getattr(update, "channel_id", None):
                record["chat_id"] = _peer_id_from_channel_id(update.channel_id)
            _apply_message_record(record, increment_unread=False)
        return

    if isinstance(update, UpdateDeleteChannelMessages):
        chat_id = _peer_id_from_channel_id(update.channel_id)
        store.delete_messages(chat_id, update.messages)
        return

    if isinstance(update, UpdateDeleteMessages):
        for message_id in update.messages:
            chat_ids = store.find_chat_ids_by_message_id(message_id)
            if len(chat_ids) == 1:
                store.delete_messages(chat_ids[0], [message_id])
            elif len(chat_ids) > 1:
                logger.warning("Ambiguous delete for message %s in chats %s", message_id, chat_ids)


async def sync_chats_from_telegram(limit: Optional[int] = None) -> None:
    dialogs = await client.raw.get_dialogs(limit=limit)
    for dialog in dialogs:
        try:
            record = build_chat_record(dialog.entity, dialog)
            store.upsert_chat_record(record)
            if getattr(dialog, "message", None):
                msg_record = build_message_record(dialog.message)
                if msg_record.get("chat_id") is None:
                    msg_record["chat_id"] = utils.get_peer_id(dialog.entity)
                store.upsert_message_record(msg_record)
        except Exception:
            logger.exception("Failed to persist dialog info")


async def sync_new_messages() -> None:
    saved_state = _load_updates_state()
    if saved_state is None:
        state = await client(functions.updates.GetStateRequest())
        _save_updates_state(
            state.pts,
            state.qts,
            _normalize_state_date(state.date),
            state.seq,
        )
        return

    pts = saved_state["pts"]
    qts = saved_state["qts"]
    date = saved_state["date"]
    seq = saved_state["seq"]

    while True:
        diff = await client(
            functions.updates.GetDifferenceRequest(
                pts=pts,
                date=date,
                qts=qts,
            )
        )

        if isinstance(diff, updates.DifferenceEmpty):
            _save_updates_state(
                pts,
                qts,
                _normalize_state_date(diff.date),
                diff.seq,
            )
            break

        if isinstance(diff, updates.DifferenceTooLong):
            state = await client(functions.updates.GetStateRequest())
            _save_updates_state(
                state.pts,
                state.qts,
                _normalize_state_date(state.date),
                state.seq,
            )
            break

        for user in getattr(diff, "users", []) or []:
            store.upsert_chat_record(build_chat_record(user))
        for chat in getattr(diff, "chats", []) or []:
            store.upsert_chat_record(build_chat_record(chat))

        for msg in getattr(diff, "new_messages", []) or []:
            record = build_message_record(msg)
            _apply_message_record(record, increment_unread=True)

        for update in getattr(diff, "other_updates", []) or []:
            _apply_difference_update(update)

        next_state = getattr(diff, "intermediate_state", None) or getattr(diff, "state", None)
        if next_state is None:
            break

        pts = next_state.pts
        qts = next_state.qts
        date = _normalize_state_date(next_state.date)
        seq = next_state.seq
        _save_updates_state(pts, qts, date, seq)

        if isinstance(diff, updates.DifferenceSlice):
            continue
        break


@client.on(events.NewMessage)
async def _handle_new_message(event: events.NewMessage.Event) -> None:
    message = event.message
    if message is None:
        return
    try:
        if getattr(message, "chat", None):
            store.upsert_chat_record(build_chat_record(message.chat))
        record = build_message_record(message)
        if record.get("chat_id") is None:
            record["chat_id"] = event.chat_id
        store.upsert_message_record(record)
        if record.get("chat_id") and not getattr(message, "out", False):
            store.increment_unread(record["chat_id"])
    except Exception:
        logger.exception("Failed to persist new message event")


@client.on(events.MessageEdited)
async def _handle_message_edited(event: events.MessageEdited.Event) -> None:
    message = event.message
    if message is None:
        return
    try:
        if getattr(message, "chat", None):
            store.upsert_chat_record(build_chat_record(message.chat))
        record = build_message_record(message)
        if record.get("chat_id") is None:
            record["chat_id"] = event.chat_id
        store.upsert_message_record(record)
    except Exception:
        logger.exception("Failed to persist edited message event")


@client.on(events.MessageDeleted)
async def _handle_message_deleted(event: events.MessageDeleted.Event) -> None:
    try:
        chat_id = event.chat_id
        if chat_id is None:
            return
        deleted_ids = event.deleted_ids or []
        store.delete_messages(chat_id, deleted_ids)
    except Exception:
        logger.exception("Failed to persist deleted message event")


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def get_chats(page: int = 1, page_size: int = 20) -> str:
    """
    Get a paginated list of chats.
    Args:
        page: Page number (1-indexed).
        page_size: Number of chats per page.
    """
    try:
        dialogs = await client.get_dialogs()
        start = (page - 1) * page_size
        end = start + page_size
        if start >= len(dialogs):
            return "Page out of range."
        chats = dialogs[start:end]
        lines = []
        for dialog in chats:
            entity = dialog.entity
            chat_id = entity.id
            title = getattr(entity, "title", None) or getattr(entity, "first_name", "Unknown")
            lines.append(f"Chat ID: {chat_id}, Title: {title}")
        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error("get_chats", e)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_messages(chat_id: Union[int, str], page: int = 1, page_size: int = 20) -> str:
    """
    Get paginated messages from a specific chat.
    Args:
        chat_id: The ID or username of the chat.
        page: Page number (1-indexed).
        page_size: Number of messages per page.
    """
    try:
        entity = await client.get_entity(chat_id)
        offset = (page - 1) * page_size
        messages = await client.get_messages(entity, limit=page_size, add_offset=offset)
        if not messages:
            return "No messages found for this page."
        lines = []
        for msg in messages:
            sender_name = get_sender_name(msg)
            reply_info = ""
            if msg.reply_to and msg.reply_to.reply_to_msg_id:
                reply_info = f" | reply to {msg.reply_to.reply_to_msg_id}"
            lines.append(
                f"ID: {msg.id} | {sender_name} | Date: {msg.date}{reply_info} | Message: {msg.message}"
            )
        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error(
            "get_messages", e, chat_id=chat_id, page=page, page_size=page_size
        )


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
@validate_id("chat_id")
async def send_message(chat_id: Union[int, str], message: str) -> str:
    """
    Send a message to a specific chat.
    Args:
        chat_id: The ID or username of the chat.
        message: The message content to send.
    """
    try:
        entity = await client.get_entity(chat_id)
        sent = await client.send_message(entity, message)
        if sent:
            record = build_message_record(sent)
            if record.get("chat_id") is None:
                resolved_id = resolve_chat_identifier(chat_id)
                if resolved_id is not None:
                    record["chat_id"] = resolved_id
            store.upsert_message_record(record)
        return "Message sent successfully."
    except Exception as e:
        return log_and_format_error("send_message", e, chat_id=chat_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("channel")
async def subscribe_public_channel(channel: Union[int, str]) -> str:
    """
    Subscribe (join) to a public channel or supergroup by username or ID.
    """
    try:
        entity = await client.get_entity(channel)
        await client(functions.channels.JoinChannelRequest(channel=entity))
        title = getattr(entity, "title", getattr(entity, "username", "Unknown channel"))
        return f"Subscribed to {title}."
    except telethon.errors.rpcerrorlist.UserAlreadyParticipantError:
        title = getattr(entity, "title", getattr(entity, "username", "this channel"))
        return f"Already subscribed to {title}."
    except telethon.errors.rpcerrorlist.ChannelPrivateError:
        return "Cannot subscribe: this channel is private or requires an invite link."
    except Exception as e:
        return log_and_format_error("subscribe_public_channel", e, channel=channel)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def list_inline_buttons(
    chat_id: Union[int, str], message_id: Optional[Union[int, str]] = None, limit: int = 20
) -> str:
    """
    Inspect inline buttons on a recent message to discover their indices/text/URLs.
    """
    try:
        if isinstance(message_id, str):
            if message_id.isdigit():
                message_id = int(message_id)
            else:
                return "message_id must be an integer."
        entity = await client.get_entity(chat_id)
        target_message = None

        if message_id is not None:
            target_message = await client.get_messages(entity, ids=message_id)
            if isinstance(target_message, list):
                target_message = target_message[0] if target_message else None
        else:
            recent_messages = await client.get_messages(entity, limit=limit)
            target_message = next(
                (msg for msg in recent_messages if getattr(msg, "buttons", None)), None
            )

        if not target_message:
            return "No message with inline buttons found."

        buttons_attr = getattr(target_message, "buttons", None)
        if not buttons_attr:
            return f"Message {target_message.id} does not contain inline buttons."

        buttons = [btn for row in buttons_attr for btn in row]
        if not buttons:
            return f"Message {target_message.id} does not contain inline buttons."

        lines = [
            f"Buttons for message {target_message.id} (date {target_message.date}):",
        ]
        for idx, btn in enumerate(buttons):
            raw_button = getattr(btn, "button", None)
            text = getattr(btn, "text", "") or "<no text>"
            url = getattr(raw_button, "url", None) if raw_button else None
            has_callback = bool(getattr(btn, "data", None))
            parts = [f"[{idx}] text='{text}'"]
            parts.append("callback=yes" if has_callback else "callback=no")
            if url:
                parts.append(f"url={url}")
            lines.append(", ".join(parts))

        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error(
            "list_inline_buttons",
            e,
            chat_id=chat_id,
            message_id=message_id,
            limit=limit,
        )


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
@validate_id("chat_id")
async def press_inline_button(
    chat_id: Union[int, str],
    message_id: Optional[Union[int, str]] = None,
    button_text: Optional[str] = None,
    button_index: Optional[int] = None,
) -> str:
    """
    Press an inline button (callback) in a chat message.

    Args:
        chat_id: Chat or bot where the inline keyboard exists.
        message_id: Specific message ID to inspect. If omitted, searches recent messages for one containing buttons.
        button_text: Exact text of the button to press (case-insensitive).
        button_index: Zero-based index among all buttons if you prefer positional access.
    """
    try:
        if button_text is None and button_index is None:
            return "Provide button_text or button_index to choose a button."

        # Normalize message_id if provided as a string
        if isinstance(message_id, str):
            if message_id.isdigit():
                message_id = int(message_id)
            else:
                return "message_id must be an integer."

        if isinstance(button_index, str):
            if button_index.isdigit():
                button_index = int(button_index)
            else:
                return "button_index must be an integer."

        entity = await client.get_entity(chat_id)

        target_message = None
        if message_id is not None:
            target_message = await client.get_messages(entity, ids=message_id)
            if isinstance(target_message, list):
                target_message = target_message[0] if target_message else None
        else:
            recent_messages = await client.get_messages(entity, limit=20)
            target_message = next(
                (msg for msg in recent_messages if getattr(msg, "buttons", None)), None
            )

        if not target_message:
            return "No message with inline buttons found. Specify message_id to target a specific message."

        buttons_attr = getattr(target_message, "buttons", None)
        if not buttons_attr:
            return f"Message {target_message.id} does not contain inline buttons."

        buttons = [btn for row in buttons_attr for btn in row]
        if not buttons:
            return f"Message {target_message.id} does not contain inline buttons."

        target_button = None
        if button_text:
            normalized = button_text.strip().lower()
            target_button = next(
                (
                    btn
                    for btn in buttons
                    if (getattr(btn, "text", "") or "").strip().lower() == normalized
                ),
                None,
            )

        if target_button is None and button_index is not None:
            if button_index < 0 or button_index >= len(buttons):
                return f"button_index out of range. Valid indices: 0-{len(buttons) - 1}."
            target_button = buttons[button_index]

        if not target_button:
            available = ", ".join(
                f"[{idx}] {getattr(btn, 'text', '') or '<no text>'}"
                for idx, btn in enumerate(buttons)
            )
            return f"Button not found. Available buttons: {available}"

        if not getattr(target_button, "data", None):
            raw_button = getattr(target_button, "button", None)
            url = getattr(raw_button, "url", None) if raw_button else None
            if url:
                return f"Selected button opens a URL instead of sending a callback: {url}"
            return "Selected button does not provide callback data to press."

        callback_result = await client(
            functions.messages.GetBotCallbackAnswerRequest(
                peer=entity, msg_id=target_message.id, data=target_button.data
            )
        )

        response_parts = []
        if getattr(callback_result, "message", None):
            response_parts.append(callback_result.message)
        if getattr(callback_result, "alert", None):
            response_parts.append("Telegram displayed an alert to the user.")
        if not response_parts:
            response_parts.append("Button pressed successfully.")

        return " ".join(response_parts)
    except Exception as e:
        return log_and_format_error(
            "press_inline_button",
            e,
            chat_id=chat_id,
            message_id=message_id,
            button_text=button_text,
            button_index=button_index,
        )


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def list_contacts() -> str:
    """
    List all contacts in your Telegram account.
    """
    try:
        result = await client(functions.contacts.GetContactsRequest(hash=0))
        users = result.users
        if not users:
            return "No contacts found."
        lines = []
        for user in users:
            name = f"{getattr(user, 'first_name', '')} {getattr(user, 'last_name', '')}".strip()
            username = getattr(user, "username", "")
            phone = getattr(user, "phone", "")
            contact_info = f"ID: {user.id}, Name: {name}"
            if username:
                contact_info += f", Username: @{username}"
            if phone:
                contact_info += f", Phone: {phone}"
            lines.append(contact_info)
        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error("list_contacts", e)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def search_contacts(query: str) -> str:
    """
    Search for contacts by name, username, or phone number using Telethon's SearchRequest.
    Args:
        query: The search term to look for in contact names, usernames, or phone numbers.
    """
    try:
        result = await client(functions.contacts.SearchRequest(q=query, limit=50))
        users = result.users
        if not users:
            return f"No contacts found matching '{query}'."
        lines = []
        for user in users:
            name = f"{getattr(user, 'first_name', '')} {getattr(user, 'last_name', '')}".strip()
            username = getattr(user, "username", "")
            phone = getattr(user, "phone", "")
            contact_info = f"ID: {user.id}, Name: {name}"
            if username:
                contact_info += f", Username: @{username}"
            if phone:
                contact_info += f", Phone: {phone}"
            lines.append(contact_info)
        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error("search_contacts", e, query=query)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def search_messages_by_sender(
    sender: Union[int, str],
    limit: int = 50,
    page: int = 1,
    search_query: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> str:
    """
    Search messages from a specific sender across ALL chats.

    Args:
        sender: The sender to search for. Can be:
            - User ID (integer or string): 123456789 or "123456789"
            - Username: "@username" or "username"
            - Name: "Владлен" (may match multiple people - you'll be prompted to choose)
        limit: Maximum number of messages per page (default 50, max 200).
        page: Page number for pagination (1-indexed).
        search_query: Optional text to filter messages containing this text.
        from_date: Filter messages from this date (format: YYYY-MM-DD).
        to_date: Filter messages until this date (format: YYYY-MM-DD).

    Returns:
        Messages from the specified sender across all chats, showing which
        chat each message is from. If sender name matches multiple people,
        returns the list of matches to choose from.
    """
    try:
        # Validate limit and page
        limit = min(max(limit, 1), 200)
        page = max(page, 1)
        offset = (page - 1) * limit

        # Step 1: Resolve sender to ID(s)
        matches = store.find_senders_by_identifier(sender)

        if not matches:
            # Try Telegram API lookup as fallback
            try:
                entity = await client.raw.get_entity(sender)
                if isinstance(entity, User):
                    # Cache it
                    store.upsert_chat_record(build_chat_record(entity))
                    name = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
                    matches = [{"id": entity.id, "title": name or "Unknown"}]
            except Exception:
                pass

        if not matches:
            return (
                f"No sender found matching '{sender}'. "
                "Try using an ID, @username, or an exact name."
            )

        # Step 2: Handle multiple matches (ambiguous name search)
        if len(matches) > 1:
            lines = [
                f"Multiple users found matching '{sender}'. Please specify by ID or username:\n"
            ]
            for match in matches[:10]:  # Limit to 10 suggestions
                info = f"  ID: {match['id']}, Name: {match.get('title', 'Unknown')}"
                if match.get("username"):
                    info += f", Username: @{match['username']}"
                lines.append(info)
            if len(matches) > 10:
                lines.append(f"\n  ... and {len(matches) - 10} more matches")
            return "\n".join(lines)

        # Single match - proceed with search
        sender_id = matches[0]["id"]
        sender_name = matches[0].get("title", "Unknown")

        # Step 3: Parse date filters
        from_ts = None
        to_ts = None

        if from_date:
            try:
                from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
                from_date_obj = from_date_obj.replace(tzinfo=timezone.utc)
                from_ts = int(from_date_obj.timestamp())
            except ValueError:
                return "Invalid from_date format. Use YYYY-MM-DD."

        if to_date:
            try:
                to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
                to_date_obj = to_date_obj.replace(tzinfo=timezone.utc) + timedelta(
                    days=1, microseconds=-1
                )
                to_ts = int(to_date_obj.timestamp())
            except ValueError:
                return "Invalid to_date format. Use YYYY-MM-DD."

        # Step 4: Query messages
        messages = store.search_messages_by_sender(
            sender_ids=[sender_id],
            limit=limit,
            offset=offset,
            search_query=search_query,
            from_ts=from_ts,
            to_ts=to_ts,
        )

        if not messages:
            filter_info = []
            if search_query:
                filter_info.append(f"containing '{search_query}'")
            if from_date:
                filter_info.append(f"from {from_date}")
            if to_date:
                filter_info.append(f"until {to_date}")
            filter_str = " ".join(filter_info) if filter_info else ""
            return f"No messages found from {sender_name} (ID: {sender_id}) {filter_str}."

        # Step 5: Get total count for pagination info
        total_count = store.count_messages_by_sender(
            sender_ids=[sender_id],
            search_query=search_query,
            from_ts=from_ts,
            to_ts=to_ts,
        )
        total_pages = (total_count + limit - 1) // limit

        # Step 6: Format output
        lines = [f"Messages from {sender_name} (ID: {sender_id})"]
        lines.append(f"Page {page}/{total_pages} ({total_count} total messages)\n")

        for msg in messages:
            chat_title = msg.get("chat_title") or "Unknown Chat"
            date_str = format_db_date(msg.get("date"))
            text = msg.get("text") or "[Media/No text]"
            reply_info = ""
            if msg.get("reply_to_msg_id"):
                reply_info = f" | reply to {msg['reply_to_msg_id']}"

            lines.append(
                f"[{chat_title}] ID: {msg['message_id']} | {date_str}{reply_info}\n{text}\n"
            )

        return "\n".join(lines)

    except Exception as e:
        return log_and_format_error("search_messages_by_sender", e, sender=sender)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def get_contact_ids() -> str:
    """
    Get all contact IDs in your Telegram account.
    """
    try:
        result = await client(functions.contacts.GetContactIDsRequest(hash=0))
        if not result:
            return "No contact IDs found."
        return "Contact IDs: " + ", ".join(str(cid) for cid in result)
    except Exception as e:
        return log_and_format_error("get_contact_ids", e)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def list_messages(
    chat_id: Union[int, str],
    limit: int = 20,
    search_query: str = None,
    from_date: str = None,
    to_date: str = None,
) -> str:
    """
    Retrieve messages with optional filters.

    Args:
        chat_id: The ID or username of the chat to get messages from.
        limit: Maximum number of messages to retrieve.
        search_query: Filter messages containing this text.
        from_date: Filter messages starting from this date (format: YYYY-MM-DD).
        to_date: Filter messages until this date (format: YYYY-MM-DD).
    """
    try:
        entity = await client.get_entity(chat_id)

        # Parse date filters if provided
        from_date_obj = None
        to_date_obj = None

        if from_date:
            try:
                from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
                # Make it timezone aware by adding UTC timezone info
                # Use datetime.timezone.utc for Python 3.9+ or import timezone directly for 3.13+
                try:
                    # For Python 3.9+
                    from_date_obj = from_date_obj.replace(tzinfo=datetime.timezone.utc)
                except AttributeError:
                    # For Python 3.13+
                    from datetime import timezone

                    from_date_obj = from_date_obj.replace(tzinfo=timezone.utc)
            except ValueError:
                return f"Invalid from_date format. Use YYYY-MM-DD."

        if to_date:
            try:
                to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
                # Set to end of day and make timezone aware
                to_date_obj = to_date_obj + timedelta(days=1, microseconds=-1)
                # Add timezone info
                try:
                    to_date_obj = to_date_obj.replace(tzinfo=datetime.timezone.utc)
                except AttributeError:
                    from datetime import timezone

                    to_date_obj = to_date_obj.replace(tzinfo=timezone.utc)
            except ValueError:
                return f"Invalid to_date format. Use YYYY-MM-DD."

        # Prepare filter parameters
        params = {}
        if search_query:
            # IMPORTANT: Do not combine offset_date with search.
            # Use server-side search alone, then enforce date bounds client-side.
            params["search"] = search_query
            messages = []
            async for msg in client.iter_messages(entity, **params):  # newest -> oldest
                if to_date_obj and msg.date > to_date_obj:
                    continue
                if from_date_obj and msg.date < from_date_obj:
                    break
                messages.append(msg)
                if len(messages) >= limit:
                    break

        else:
            # Use server-side iteration when only date bounds are present
            # (no search) to avoid over-fetching.
            if from_date_obj or to_date_obj:
                messages = []
                if from_date_obj:
                    # Walk forward from start date (oldest -> newest)
                    async for msg in client.iter_messages(
                        entity, offset_date=from_date_obj, reverse=True
                    ):
                        if to_date_obj and msg.date > to_date_obj:
                            break
                        if msg.date < from_date_obj:
                            continue
                        messages.append(msg)
                        if len(messages) >= limit:
                            break
                else:
                    # Only upper bound: walk backward from end bound
                    async for msg in client.iter_messages(
                        # offset_date is exclusive; +1µs makes to_date inclusive
                        entity,
                        offset_date=to_date_obj + timedelta(microseconds=1),
                    ):
                        messages.append(msg)
                        if len(messages) >= limit:
                            break
            else:
                messages = await client.get_messages(entity, limit=limit, **params)

        if not messages:
            return "No messages found matching the criteria."

        lines = []
        for msg in messages:
            sender_name = get_sender_name(msg)
            message_text = msg.message or "[Media/No text]"
            reply_info = ""
            if msg.reply_to and msg.reply_to.reply_to_msg_id:
                reply_info = f" | reply to {msg.reply_to.reply_to_msg_id}"

            lines.append(
                f"ID: {msg.id} | {sender_name} | Date: {msg.date}{reply_info} | Message: {message_text}"
            )

        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error("list_messages", e, chat_id=chat_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, idempotentHint=True, destructiveHint=True)
)
@validate_id("chat_ids")
async def initialize_db(
    limit_per_chat: int = 5000,
    since_days: Optional[int] = None,
    since_date: Optional[str] = None,
    chat_ids: Optional[List[Union[int, str]]] = None,
) -> str:
    """
    Initialize the local SQLite cache by loading message history.

    Args:
        limit_per_chat: Max messages per chat to load.
        since_days: Load messages from the last N days (UTC).
        since_date: Load messages since this date (YYYY-MM-DD). Overrides since_days if provided.
        chat_ids: Optional list of chat IDs/usernames to initialize. If omitted, initializes all dialogs.
    """
    try:
        if limit_per_chat <= 0:
            return "limit_per_chat must be a positive integer."

        since_dt = None
        if since_date:
            try:
                since_dt = datetime.strptime(since_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                return "Invalid since_date format. Use YYYY-MM-DD."
        elif since_days is not None:
            if since_days <= 0:
                return "since_days must be a positive integer."
            since_dt = datetime.now(timezone.utc) - timedelta(days=since_days)

        chats_to_sync: List[Any] = []
        if chat_ids:
            for chat_id in chat_ids:
                try:
                    entity = await client.get_entity(chat_id)
                    store.upsert_chat_record(build_chat_record(entity))
                    chats_to_sync.append(entity)
                except Exception:
                    logger.exception("Failed to resolve chat %s during initialization", chat_id)
        else:
            dialogs = await client.raw.get_dialogs()
            for dialog in dialogs:
                try:
                    store.upsert_chat_record(build_chat_record(dialog.entity, dialog))
                    chats_to_sync.append(dialog.entity)
                except Exception:
                    logger.exception("Failed to persist dialog during initialization")

        if not chats_to_sync:
            return "No chats found to initialize."

        summary_lines = []
        for entity in chats_to_sync:
            chat_id = utils.get_peer_id(entity)
            count = 0
            batch: List[Dict[str, Any]] = []

            if since_dt:
                iterator = client.raw.iter_messages(entity, offset_date=since_dt, reverse=True)
            else:
                iterator = client.raw.iter_messages(entity, limit=limit_per_chat, reverse=True)

            async for msg in iterator:
                record = build_message_record(msg)
                if record.get("chat_id") is None:
                    record["chat_id"] = chat_id
                batch.append(record)
                count += 1
                if len(batch) >= 200:
                    store.upsert_messages(batch)
                    batch.clear()
                if since_dt and count >= limit_per_chat:
                    break

            if batch:
                store.upsert_messages(batch)

            summary_lines.append(f"Chat {chat_id}: {count} messages")

        return "Initialization complete:\n" + "\n".join(summary_lines)
    except Exception as e:
        return log_and_format_error("initialize_db", e)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def list_topics(
    chat_id: int,
    limit: int = 200,
    offset_topic: int = 0,
    search_query: str = None,
) -> str:
    """
    Retrieve forum topics from a supergroup with the forum feature enabled.

    Note for LLM: You can send a message to a selected topic via reply_to_message tool
    by using Topic ID as the message_id parameter.

    Args:
        chat_id: The ID of the forum-enabled chat (supergroup).
        limit: Maximum number of topics to retrieve.
        offset_topic: Topic ID offset for pagination.
        search_query: Optional query to filter topics by title.
    """
    try:
        entity = await client.get_entity(chat_id)

        if not isinstance(entity, Channel) or not getattr(entity, "megagroup", False):
            return "The specified chat is not a supergroup."

        if not getattr(entity, "forum", False):
            return "The specified supergroup does not have forum topics enabled."

        result = await client(
            functions.channels.GetForumTopicsRequest(
                channel=entity,
                offset_date=0,
                offset_id=0,
                offset_topic=offset_topic,
                limit=limit,
                q=search_query or None,
            )
        )

        topics = getattr(result, "topics", None) or []
        if not topics:
            return "No topics found for this chat."

        messages_map = {}
        if getattr(result, "messages", None):
            messages_map = {message.id: message for message in result.messages}

        lines = []
        for topic in topics:
            line_parts = [f"Topic ID: {topic.id}"]

            title = getattr(topic, "title", None) or "(no title)"
            line_parts.append(f"Title: {title}")

            total_messages = getattr(topic, "total_messages", None)
            if total_messages is not None:
                line_parts.append(f"Messages: {total_messages}")

            unread_count = getattr(topic, "unread_count", None)
            if unread_count:
                line_parts.append(f"Unread: {unread_count}")

            if getattr(topic, "closed", False):
                line_parts.append("Closed: Yes")

            if getattr(topic, "hidden", False):
                line_parts.append("Hidden: Yes")

            top_message_id = getattr(topic, "top_message", None)
            top_message = messages_map.get(top_message_id)
            if top_message and getattr(top_message, "date", None):
                line_parts.append(f"Last Activity: {top_message.date.isoformat()}")

            lines.append(" | ".join(line_parts))

        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error(
            "list_topics",
            e,
            chat_id=chat_id,
            limit=limit,
            offset_topic=offset_topic,
            search_query=search_query,
        )


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def list_chats(chat_type: str = None, limit: int = 20) -> str:
    """
    List available chats with metadata.

    Args:
        chat_type: Filter by chat type ('user', 'group', 'channel', or None for all)
        limit: Maximum number of chats to retrieve.
    """
    try:
        dialogs = await client.get_dialogs(limit=limit)

        results = []
        for dialog in dialogs:
            entity = dialog.entity

            # Filter by type if requested
            current_type = None
            if isinstance(entity, User):
                current_type = "user"
            elif isinstance(entity, Chat):
                current_type = "group"
            elif isinstance(entity, Channel):
                if getattr(entity, "broadcast", False):
                    current_type = "channel"
                else:
                    current_type = "group"  # Supergroup

            if chat_type and current_type != chat_type.lower():
                continue

            # Format chat info
            chat_info = f"Chat ID: {entity.id}"

            if hasattr(entity, "title"):
                chat_info += f", Title: {entity.title}"
            elif hasattr(entity, "first_name"):
                name = f"{entity.first_name}"
                if hasattr(entity, "last_name") and entity.last_name:
                    name += f" {entity.last_name}"
                chat_info += f", Name: {name}"

            chat_info += f", Type: {current_type}"

            if hasattr(entity, "username") and entity.username:
                chat_info += f", Username: @{entity.username}"

            # Add unread count if available
            if hasattr(dialog, "unread_count") and dialog.unread_count > 0:
                chat_info += f", Unread: {dialog.unread_count}"

            results.append(chat_info)

        if not results:
            return f"No chats found matching the criteria."

        return "\n".join(results)
    except Exception as e:
        return log_and_format_error("list_chats", e, chat_type=chat_type, limit=limit)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_chat(chat_id: Union[int, str]) -> str:
    """
    Get detailed information about a specific chat.

    Args:
        chat_id: The ID or username of the chat.
    """
    try:
        entity = await client.get_entity(chat_id)

        result = []
        result.append(f"ID: {entity.id}")

        is_channel = isinstance(entity, Channel)
        is_chat = isinstance(entity, Chat)
        is_user = isinstance(entity, User)

        if hasattr(entity, "title"):
            result.append(f"Title: {entity.title}")
            chat_type = (
                "Channel" if is_channel and getattr(entity, "broadcast", False) else "Group"
            )
            if is_channel and getattr(entity, "megagroup", False):
                chat_type = "Supergroup"
            elif is_chat:
                chat_type = "Group (Basic)"
            result.append(f"Type: {chat_type}")
            if hasattr(entity, "username") and entity.username:
                result.append(f"Username: @{entity.username}")

            # Fetch participants count reliably
            try:
                participants_count = (await client.get_participants(entity, limit=0)).total
                result.append(f"Participants: {participants_count}")
            except Exception as pe:
                result.append(f"Participants: Error fetching ({pe})")

        elif is_user:
            name = f"{entity.first_name}"
            if entity.last_name:
                name += f" {entity.last_name}"
            result.append(f"Name: {name}")
            result.append(f"Type: User")
            if entity.username:
                result.append(f"Username: @{entity.username}")
            if entity.phone:
                result.append(f"Phone: {entity.phone}")
            result.append(f"Bot: {'Yes' if entity.bot else 'No'}")
            result.append(f"Verified: {'Yes' if entity.verified else 'No'}")

        # Get last activity if it's a dialog
        try:
            # Using get_dialogs might be slow if there are many dialogs
            # Alternative: Get entity again via get_dialogs if needed for unread count
            dialog = await client.get_dialogs(limit=1, offset_id=0, offset_peer=entity)
            if dialog:
                dialog = dialog[0]
                result.append(f"Unread Messages: {dialog.unread_count}")
                if dialog.message:
                    last_msg = dialog.message
                    sender_name = "Unknown"
                    if last_msg.sender:
                        sender_name = getattr(last_msg.sender, "first_name", "") or getattr(
                            last_msg.sender, "title", "Unknown"
                        )
                        if hasattr(last_msg.sender, "last_name") and last_msg.sender.last_name:
                            sender_name += f" {last_msg.sender.last_name}"
                    sender_name = sender_name.strip() or "Unknown"
                    result.append(f"Last Message: From {sender_name} at {last_msg.date}")
                    result.append(f"Message: {last_msg.message or '[Media/No text]'}")
        except Exception as diag_ex:
            logger.warning(f"Could not get dialog info for {chat_id}: {diag_ex}")
            pass

        return "\n".join(result)
    except Exception as e:
        return log_and_format_error("get_chat", e, chat_id=chat_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def get_direct_chat_by_contact(contact_query: str) -> str:
    """
    Find a direct chat with a specific contact by name, username, or phone.

    Args:
        contact_query: Name, username, or phone number to search for.
    """
    try:
        # Fetch all contacts using the correct Telethon method
        result = await client(functions.contacts.GetContactsRequest(hash=0))
        contacts = result.users
        found_contacts = []
        for contact in contacts:
            if not contact:
                continue
            name = (
                f"{getattr(contact, 'first_name', '')} {getattr(contact, 'last_name', '')}".strip()
            )
            username = getattr(contact, "username", "")
            phone = getattr(contact, "phone", "")
            if (
                contact_query.lower() in name.lower()
                or (username and contact_query.lower() in username.lower())
                or (phone and contact_query in phone)
            ):
                found_contacts.append(contact)
        if not found_contacts:
            return f"No contacts found matching '{contact_query}'."
        # If we found contacts, look for direct chats with them
        results = []
        dialogs = await client.get_dialogs()
        for contact in found_contacts:
            contact_name = (
                f"{getattr(contact, 'first_name', '')} {getattr(contact, 'last_name', '')}".strip()
            )
            for dialog in dialogs:
                if isinstance(dialog.entity, User) and dialog.entity.id == contact.id:
                    chat_info = f"Chat ID: {dialog.entity.id}, Contact: {contact_name}"
                    if getattr(contact, "username", ""):
                        chat_info += f", Username: @{contact.username}"
                    if dialog.unread_count:
                        chat_info += f", Unread: {dialog.unread_count}"
                    results.append(chat_info)
                    break
        if not results:
            found_names = ", ".join(
                [f"{c.first_name} {c.last_name}".strip() for c in found_contacts]
            )
            return f"Found contacts: {found_names}, but no direct chats were found with them."
        return "\n".join(results)
    except Exception as e:
        return log_and_format_error("get_direct_chat_by_contact", e, contact_query=contact_query)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("contact_id")
async def get_contact_chats(contact_id: Union[int, str]) -> str:
    """
    List all chats involving a specific contact.

    Args:
        contact_id: The ID or username of the contact.
    """
    try:
        # Get contact info
        contact = await client.get_entity(contact_id)
        if not isinstance(contact, User):
            return f"ID {contact_id} is not a user/contact."

        contact_name = (
            f"{getattr(contact, 'first_name', '')} {getattr(contact, 'last_name', '')}".strip()
        )

        # Find direct chat
        direct_chat = None
        dialogs = await client.get_dialogs()

        results = []

        # Look for direct chat
        for dialog in dialogs:
            if isinstance(dialog.entity, User) and dialog.entity.id == contact_id:
                chat_info = f"Direct Chat ID: {dialog.entity.id}, Type: Private"
                if dialog.unread_count:
                    chat_info += f", Unread: {dialog.unread_count}"
                results.append(chat_info)
                break

        # Look for common groups/channels
        common_chats = []
        try:
            common = await client.get_common_chats(contact)
            for chat in common:
                chat_type = "Channel" if getattr(chat, "broadcast", False) else "Group"
                chat_info = f"Chat ID: {chat.id}, Title: {chat.title}, Type: {chat_type}"
                results.append(chat_info)
        except:
            results.append("Could not retrieve common groups.")

        if not results:
            return f"No chats found with {contact_name} (ID: {contact_id})."

        return f"Chats with {contact_name} (ID: {contact_id}):\n" + "\n".join(results)
    except Exception as e:
        return log_and_format_error("get_contact_chats", e, contact_id=contact_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("contact_id")
async def get_last_interaction(contact_id: Union[int, str]) -> str:
    """
    Get the most recent message with a contact.

    Args:
        contact_id: The ID or username of the contact.
    """
    try:
        # Get contact info
        contact = await client.get_entity(contact_id)
        if not isinstance(contact, User):
            return f"ID {contact_id} is not a user/contact."

        contact_name = (
            f"{getattr(contact, 'first_name', '')} {getattr(contact, 'last_name', '')}".strip()
        )

        # Get the last few messages
        messages = await client.get_messages(contact, limit=5)

        if not messages:
            return f"No messages found with {contact_name} (ID: {contact_id})."

        results = [f"Last interactions with {contact_name} (ID: {contact_id}):"]

        for msg in messages:
            sender = "You" if msg.out else contact_name
            message_text = msg.message or "[Media/No text]"
            results.append(f"Date: {msg.date}, From: {sender}, Message: {message_text}")

        return "\n".join(results)
    except Exception as e:
        return log_and_format_error("get_last_interaction", e, contact_id=contact_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_message_context(
    chat_id: Union[int, str], message_id: int, context_size: int = 3
) -> str:
    """
    Retrieve context around a specific message.

    Args:
        chat_id: The ID or username of the chat.
        message_id: The ID of the central message.
        context_size: Number of messages before and after to include.
    """
    try:
        chat = await client.get_entity(chat_id)
        # Get messages around the specified message
        messages_before = await client.get_messages(chat, limit=context_size, max_id=message_id)
        central_message = await client.get_messages(chat, ids=message_id)
        # Fix: get_messages(ids=...) returns a single Message, not a list
        if central_message is not None and not isinstance(central_message, list):
            central_message = [central_message]
        elif central_message is None:
            central_message = []
        messages_after = await client.get_messages(
            chat, limit=context_size, min_id=message_id, reverse=True
        )
        if not central_message:
            return f"Message with ID {message_id} not found in chat {chat_id}."
        # Combine messages in chronological order
        all_messages = list(messages_before) + list(central_message) + list(messages_after)
        all_messages.sort(key=lambda m: m.id)
        results = [f"Context for message {message_id} in chat {chat_id}:"]
        for msg in all_messages:
            sender_name = get_sender_name(msg)
            highlight = " [THIS MESSAGE]" if msg.id == message_id else ""

            # Check if this message is a reply and get the replied message
            reply_content = ""
            if msg.reply_to and msg.reply_to.reply_to_msg_id:
                try:
                    replied_msg = await client.get_messages(chat, ids=msg.reply_to.reply_to_msg_id)
                    if replied_msg:
                        replied_sender = "Unknown"
                        if replied_msg.sender:
                            replied_sender = getattr(
                                replied_msg.sender, "first_name", ""
                            ) or getattr(replied_msg.sender, "title", "Unknown")
                        reply_content = f" | reply to {msg.reply_to.reply_to_msg_id}\n  → Replied message: [{replied_sender}] {replied_msg.message or '[Media/No text]'}"
                except Exception:
                    reply_content = (
                        f" | reply to {msg.reply_to.reply_to_msg_id} (original message not found)"
                    )

            results.append(
                f"ID: {msg.id} | {sender_name} | {msg.date}{highlight}{reply_content}\n{msg.message or '[Media/No text]'}\n"
            )
        return "\n".join(results)
    except Exception as e:
        return log_and_format_error(
            "get_message_context",
            e,
            chat_id=chat_id,
            message_id=message_id,
            context_size=context_size,
        )


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
async def add_contact(phone: str, first_name: str, last_name: str = "") -> str:
    """
    Add a new contact to your Telegram account.
    Args:
        phone: The phone number of the contact (with country code).
        first_name: The contact's first name.
        last_name: The contact's last name (optional).
    """
    try:
        # Try to import the required types first
        from telethon.tl.types import InputPhoneContact

        result = await client(
            functions.contacts.ImportContactsRequest(
                contacts=[
                    InputPhoneContact(
                        client_id=0,
                        phone=phone,
                        first_name=first_name,
                        last_name=last_name,
                    )
                ]
            )
        )
        if result.imported:
            return f"Contact {first_name} {last_name} added successfully."
        else:
            return f"Contact not added. Response: {str(result)}"
    except (ImportError, AttributeError) as type_err:
        # Try alternative approach using raw API
        try:
            result = await client(
                functions.contacts.ImportContactsRequest(
                    contacts=[
                        {
                            "client_id": 0,
                            "phone": phone,
                            "first_name": first_name,
                            "last_name": last_name,
                        }
                    ]
                )
            )
            if hasattr(result, "imported") and result.imported:
                return f"Contact {first_name} {last_name} added successfully (alt method)."
            else:
                return f"Contact not added. Alternative method response: {str(result)}"
        except Exception as alt_e:
            logger.exception(f"add_contact (alt method) failed (phone={phone})")
            return log_and_format_error("add_contact", alt_e, phone=phone)
    except Exception as e:
        logger.exception(f"add_contact failed (phone={phone})")
        return log_and_format_error("add_contact", e, phone=phone)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("user_id")
async def delete_contact(user_id: Union[int, str]) -> str:
    """
    Delete a contact by user ID.
    Args:
        user_id: The Telegram user ID or username of the contact to delete.
    """
    try:
        user = await client.get_entity(user_id)
        await client(functions.contacts.DeleteContactsRequest(id=[user]))
        return f"Contact with user ID {user_id} deleted."
    except Exception as e:
        return log_and_format_error("delete_contact", e, user_id=user_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("user_id")
async def block_user(user_id: Union[int, str]) -> str:
    """
    Block a user by user ID.
    Args:
        user_id: The Telegram user ID or username to block.
    """
    try:
        user = await client.get_entity(user_id)
        await client(functions.contacts.BlockRequest(id=user))
        return f"User {user_id} blocked."
    except Exception as e:
        return log_and_format_error("block_user", e, user_id=user_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("user_id")
async def unblock_user(user_id: Union[int, str]) -> str:
    """
    Unblock a user by user ID.
    Args:
        user_id: The Telegram user ID or username to unblock.
    """
    try:
        user = await client.get_entity(user_id)
        await client(functions.contacts.UnblockRequest(id=user))
        return f"User {user_id} unblocked."
    except Exception as e:
        return log_and_format_error("unblock_user", e, user_id=user_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def get_me() -> str:
    """
    Get your own user information.
    """
    try:
        me = await client.get_me()
        return json.dumps(format_entity(me), indent=2)
    except Exception as e:
        return log_and_format_error("get_me", e)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
@validate_id("user_ids")
async def create_group(title: str, user_ids: List[Union[int, str]]) -> str:
    """
    Create a new group or supergroup and add users.

    Args:
        title: Title for the new group
        user_ids: List of user IDs or usernames to add to the group
    """
    try:
        # Convert user IDs to entities
        users = []
        for user_id in user_ids:
            try:
                user = await client.get_entity(user_id)
                users.append(user)
            except Exception as e:
                logger.error(f"Failed to get entity for user ID {user_id}: {e}")
                return f"Error: Could not find user with ID {user_id}"

        if not users:
            return "Error: No valid users provided"

        # Create the group with the users
        try:
            # Create a new chat with selected users
            result = await client(functions.messages.CreateChatRequest(users=users, title=title))

            # Check what type of response we got
            if hasattr(result, "chats") and result.chats:
                created_chat = result.chats[0]
                return f"Group created with ID: {created_chat.id}"
            elif hasattr(result, "chat") and result.chat:
                return f"Group created with ID: {result.chat.id}"
            elif hasattr(result, "chat_id"):
                return f"Group created with ID: {result.chat_id}"
            else:
                # If we can't determine the chat ID directly from the result
                # Try to find it in recent dialogs
                await asyncio.sleep(1)  # Give Telegram a moment to register the new group
                dialogs = await client.raw.get_dialogs(limit=5)  # Get recent dialogs
                for dialog in dialogs:
                    if dialog.title == title:
                        return f"Group created with ID: {dialog.id}"

                # If we still can't find it, at least return success
                return f"Group created successfully. Please check your recent chats for '{title}'."

        except Exception as create_err:
            if "PEER_FLOOD" in str(create_err):
                return "Error: Cannot create group due to Telegram limits. Try again later."
            else:
                raise  # Let the outer exception handler catch it
    except Exception as e:
        logger.exception(f"create_group failed (title={title}, user_ids={user_ids})")
        return log_and_format_error("create_group", e, title=title, user_ids=user_ids)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("group_id", "user_ids")
async def invite_to_group(group_id: Union[int, str], user_ids: List[Union[int, str]]) -> str:
    """
    Invite users to a group or channel.

    Args:
        group_id: The ID or username of the group/channel.
        user_ids: List of user IDs or usernames to invite.
    """
    try:
        entity = await client.get_entity(group_id)
        users_to_add = []

        for user_id in user_ids:
            try:
                user = await client.get_entity(user_id)
                users_to_add.append(user)
            except ValueError as e:
                return f"Error: User with ID {user_id} could not be found. {e}"

        try:
            result = await client(
                functions.channels.InviteToChannelRequest(channel=entity, users=users_to_add)
            )

            invited_count = 0
            if hasattr(result, "users") and result.users:
                invited_count = len(result.users)
            elif hasattr(result, "count"):
                invited_count = result.count

            return f"Successfully invited {invited_count} users to {entity.title}"
        except telethon.errors.rpcerrorlist.UserNotMutualContactError:
            return "Error: Cannot invite users who are not mutual contacts. Please ensure the users are in your contacts and have added you back."
        except telethon.errors.rpcerrorlist.UserPrivacyRestrictedError:
            return (
                "Error: One or more users have privacy settings that prevent you from adding them."
            )
        except Exception as e:
            return log_and_format_error("invite_to_group", e, group_id=group_id, user_ids=user_ids)

    except Exception as e:
        logger.error(
            f"telegram_mcp invite_to_group failed (group_id={group_id}, user_ids={user_ids})",
            exc_info=True,
        )
        return log_and_format_error("invite_to_group", e, group_id=group_id, user_ids=user_ids)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def leave_chat(chat_id: Union[int, str]) -> str:
    """
    Leave a group or channel by chat ID.

    Args:
        chat_id: The chat ID or username to leave.
    """
    try:
        entity = await client.get_entity(chat_id)

        # Check the entity type carefully
        if isinstance(entity, Channel):
            # Handle both channels and supergroups (which are also channels in Telegram)
            try:
                await client(functions.channels.LeaveChannelRequest(channel=entity))
                chat_name = getattr(entity, "title", str(chat_id))
                return f"Left channel/supergroup {chat_name} (ID: {chat_id})."
            except Exception as chan_err:
                return log_and_format_error("leave_chat", chan_err, chat_id=chat_id)

        elif isinstance(entity, Chat):
            # Traditional basic groups (not supergroups)
            try:
                # First try with InputPeerUser
                me = await client.get_me(input_peer=True)
                await client(
                    functions.messages.DeleteChatUserRequest(
                        chat_id=entity.id,
                        user_id=me,  # Use the entity ID directly
                    )
                )
                chat_name = getattr(entity, "title", str(chat_id))
                return f"Left basic group {chat_name} (ID: {chat_id})."
            except Exception as chat_err:
                # If the above fails, try the second approach
                logger.warning(
                    f"First leave attempt failed: {chat_err}, trying alternative method"
                )

                try:
                    # Alternative approach - sometimes this works better
                    me_full = await client.get_me()
                    await client(
                        functions.messages.DeleteChatUserRequest(
                            chat_id=entity.id, user_id=me_full.id
                        )
                    )
                    chat_name = getattr(entity, "title", str(chat_id))
                    return f"Left basic group {chat_name} (ID: {chat_id})."
                except Exception as alt_err:
                    return log_and_format_error("leave_chat", alt_err, chat_id=chat_id)
        else:
            # Cannot leave a user chat this way
            entity_type = type(entity).__name__
            return log_and_format_error(
                "leave_chat",
                Exception(
                    f"Cannot leave chat ID {chat_id} of type {entity_type}. This function is for groups and channels only."
                ),
                chat_id=chat_id,
            )

    except Exception as e:
        logger.exception(f"leave_chat failed (chat_id={chat_id})")

        # Provide helpful hint for common errors
        error_str = str(e).lower()
        if "invalid" in error_str and "chat" in error_str:
            return log_and_format_error(
                "leave_chat",
                Exception(
                    f"Error leaving chat: This appears to be a channel/supergroup. Please check the chat ID and try again."
                ),
                chat_id=chat_id,
            )

        return log_and_format_error("leave_chat", e, chat_id=chat_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_participants(chat_id: Union[int, str]) -> str:
    """
    List all participants in a group or channel.
    Args:
        chat_id: The group or channel ID or username.
    """
    try:
        participants = await client.get_participants(chat_id)
        lines = [
            f"ID: {p.id}, Name: {getattr(p, 'first_name', '')} {getattr(p, 'last_name', '')}"
            for p in participants
        ]
        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error("get_participants", e, chat_id=chat_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
@validate_id("chat_id")
async def send_file(chat_id: Union[int, str], file_path: str, caption: str = None) -> str:
    """
    Send a file to a chat.
    Args:
        chat_id: The chat ID or username.
        file_path: Absolute path to the file to send (must exist and be readable).
        caption: Optional caption for the file.
    """
    try:
        if not os.path.isfile(file_path):
            return f"File not found: {file_path}"
        if not os.access(file_path, os.R_OK):
            return f"File is not readable: {file_path}"
        entity = await client.get_entity(chat_id)
        await client.send_file(entity, file_path, caption=caption)
        return f"File sent to chat {chat_id}."
    except Exception as e:
        return log_and_format_error(
            "send_file", e, chat_id=chat_id, file_path=file_path, caption=caption
        )


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def download_media(chat_id: Union[int, str], message_id: int, file_path: str) -> str:
    """
    Download media from a message in a chat.
    Args:
        chat_id: The chat ID or username.
        message_id: The message ID containing the media.
        file_path: Absolute path to save the downloaded file (must be writable).
    """
    try:
        entity = await client.get_entity(chat_id)
        msg = await client.raw.get_messages(entity, ids=message_id)
        if not msg or not msg.media:
            return "No media found in the specified message."
        # Check if directory is writable
        dir_path = os.path.dirname(file_path) or "."
        if not os.access(dir_path, os.W_OK):
            return f"Directory not writable: {dir_path}"
        await client.download_media(msg, file=file_path)
        if not os.path.isfile(file_path):
            return f"Download failed: file not created at {file_path}"
        return f"Media downloaded to {file_path}."
    except Exception as e:
        return log_and_format_error(
            "download_media",
            e,
            chat_id=chat_id,
            message_id=message_id,
            file_path=file_path,
        )


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
async def update_profile(first_name: str = None, last_name: str = None, about: str = None) -> str:
    """
    Update your profile information (name, bio).
    """
    try:
        await client(
            functions.account.UpdateProfileRequest(
                first_name=first_name, last_name=last_name, about=about
            )
        )
        return "Profile updated."
    except Exception as e:
        return log_and_format_error(
            "update_profile", e, first_name=first_name, last_name=last_name, about=about
        )


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
async def set_profile_photo(file_path: str) -> str:
    """
    Set a new profile photo.
    """
    try:
        await client(
            functions.photos.UploadProfilePhotoRequest(file=await client.upload_file(file_path))
        )
        return "Profile photo updated."
    except Exception as e:
        return log_and_format_error("set_profile_photo", e, file_path=file_path)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
async def delete_profile_photo() -> str:
    """
    Delete your current profile photo.
    """
    try:
        photos = await client(
            functions.photos.GetUserPhotosRequest(user_id="me", offset=0, max_id=0, limit=1)
        )
        if not photos.photos:
            return "No profile photo to delete."
        await client(functions.photos.DeletePhotosRequest(id=[photos.photos[0].id]))
        return "Profile photo deleted."
    except Exception as e:
        return log_and_format_error("delete_profile_photo", e)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def get_privacy_settings() -> str:
    """
    Get your privacy settings for last seen status.
    """
    try:
        # Import needed types directly
        from telethon.tl.types import InputPrivacyKeyStatusTimestamp

        try:
            settings = await client(
                functions.account.GetPrivacyRequest(key=InputPrivacyKeyStatusTimestamp())
            )
            return str(settings)
        except TypeError as e:
            if "TLObject was expected" in str(e):
                return "Error: Privacy settings API call failed due to type mismatch. This is likely a version compatibility issue with Telethon."
            else:
                raise
    except Exception as e:
        logger.exception("get_privacy_settings failed")
        return log_and_format_error("get_privacy_settings", e)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("allow_users", "disallow_users")
async def set_privacy_settings(
    key: str,
    allow_users: Optional[List[Union[int, str]]] = None,
    disallow_users: Optional[List[Union[int, str]]] = None,
) -> str:
    """
    Set privacy settings (e.g., last seen, phone, etc.).

    Args:
        key: The privacy setting to modify ('status' for last seen, 'phone', 'profile_photo', etc.)
        allow_users: List of user IDs or usernames to allow
        disallow_users: List of user IDs or usernames to disallow
    """
    try:
        # Import needed types
        from telethon.tl.types import (
            InputPrivacyKeyStatusTimestamp,
            InputPrivacyKeyPhoneNumber,
            InputPrivacyKeyProfilePhoto,
            InputPrivacyValueAllowUsers,
            InputPrivacyValueDisallowUsers,
            InputPrivacyValueAllowAll,
            InputPrivacyValueDisallowAll,
        )

        # Map the simplified keys to their corresponding input types
        key_mapping = {
            "status": InputPrivacyKeyStatusTimestamp,
            "phone": InputPrivacyKeyPhoneNumber,
            "profile_photo": InputPrivacyKeyProfilePhoto,
        }

        # Get the appropriate key class
        if key not in key_mapping:
            return f"Error: Unsupported privacy key '{key}'. Supported keys: {', '.join(key_mapping.keys())}"

        privacy_key = key_mapping[key]()

        # Prepare the rules
        rules = []

        # Process allow rules
        if allow_users is None or len(allow_users) == 0:
            # If no specific users to allow, allow everyone by default
            rules.append(InputPrivacyValueAllowAll())
        else:
            # Convert user IDs to InputUser entities
            try:
                allow_entities = []
                for user_id in allow_users:
                    try:
                        user = await client.get_entity(user_id)
                        allow_entities.append(user)
                    except Exception as user_err:
                        logger.warning(f"Could not get entity for user ID {user_id}: {user_err}")

                if allow_entities:
                    rules.append(InputPrivacyValueAllowUsers(users=allow_entities))
            except Exception as allow_err:
                logger.error(f"Error processing allowed users: {allow_err}")
                return log_and_format_error("set_privacy_settings", allow_err, key=key)

        # Process disallow rules
        if disallow_users and len(disallow_users) > 0:
            try:
                disallow_entities = []
                for user_id in disallow_users:
                    try:
                        user = await client.get_entity(user_id)
                        disallow_entities.append(user)
                    except Exception as user_err:
                        logger.warning(f"Could not get entity for user ID {user_id}: {user_err}")

                if disallow_entities:
                    rules.append(InputPrivacyValueDisallowUsers(users=disallow_entities))
            except Exception as disallow_err:
                logger.error(f"Error processing disallowed users: {disallow_err}")
                return log_and_format_error("set_privacy_settings", disallow_err, key=key)

        # Apply the privacy settings
        try:
            result = await client(
                functions.account.SetPrivacyRequest(key=privacy_key, rules=rules)
            )
            return f"Privacy settings for {key} updated successfully."
        except TypeError as type_err:
            if "TLObject was expected" in str(type_err):
                return "Error: Privacy settings API call failed due to type mismatch. This is likely a version compatibility issue with Telethon."
            else:
                raise
    except Exception as e:
        logger.exception(f"set_privacy_settings failed (key={key})")
        return log_and_format_error("set_privacy_settings", e, key=key)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
async def import_contacts(contacts: list) -> str:
    """
    Import a list of contacts. Each contact should be a dict with phone, first_name, last_name.
    """
    try:
        input_contacts = [
            functions.contacts.InputPhoneContact(
                client_id=i,
                phone=c["phone"],
                first_name=c["first_name"],
                last_name=c.get("last_name", ""),
            )
            for i, c in enumerate(contacts)
        ]
        result = await client(functions.contacts.ImportContactsRequest(contacts=input_contacts))
        return f"Imported {len(result.imported)} contacts."
    except Exception as e:
        return log_and_format_error("import_contacts", e, contacts=contacts)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def export_contacts() -> str:
    """
    Export all contacts as a JSON string.
    """
    try:
        result = await client(functions.contacts.GetContactsRequest(hash=0))
        users = result.users
        return json.dumps([format_entity(u) for u in users], indent=2)
    except Exception as e:
        return log_and_format_error("export_contacts", e)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def get_blocked_users() -> str:
    """
    Get a list of blocked users.
    """
    try:
        result = await client(functions.contacts.GetBlockedRequest(offset=0, limit=100))
        return json.dumps([format_entity(u) for u in result.users], indent=2)
    except Exception as e:
        return log_and_format_error("get_blocked_users", e)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
async def create_channel(title: str, about: str = "", megagroup: bool = False) -> str:
    """
    Create a new channel or supergroup.
    """
    try:
        result = await client(
            functions.channels.CreateChannelRequest(title=title, about=about, megagroup=megagroup)
        )
        return f"Channel '{title}' created with ID: {result.chats[0].id}"
    except Exception as e:
        return log_and_format_error(
            "create_channel", e, title=title, about=about, megagroup=megagroup
        )


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def edit_chat_title(chat_id: Union[int, str], title: str) -> str:
    """
    Edit the title of a chat, group, or channel.
    """
    try:
        entity = await client.get_entity(chat_id)
        if isinstance(entity, Channel):
            await client(functions.channels.EditTitleRequest(channel=entity, title=title))
        elif isinstance(entity, Chat):
            await client(functions.messages.EditChatTitleRequest(chat_id=chat_id, title=title))
        else:
            return f"Cannot edit title for this entity type ({type(entity)})."
        return f"Chat {chat_id} title updated to '{title}'."
    except Exception as e:
        logger.exception(f"edit_chat_title failed (chat_id={chat_id}, title='{title}')")
        return log_and_format_error("edit_chat_title", e, chat_id=chat_id, title=title)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def edit_chat_photo(chat_id: Union[int, str], file_path: str) -> str:
    """
    Edit the photo of a chat, group, or channel. Requires a file path to an image.
    """
    try:
        if not os.path.isfile(file_path):
            return f"Photo file not found: {file_path}"
        if not os.access(file_path, os.R_OK):
            return f"Photo file not readable: {file_path}"

        entity = await client.get_entity(chat_id)
        uploaded_file = await client.upload_file(file_path)

        if isinstance(entity, Channel):
            # For channels/supergroups, use EditPhotoRequest with InputChatUploadedPhoto
            input_photo = InputChatUploadedPhoto(file=uploaded_file)
            await client(functions.channels.EditPhotoRequest(channel=entity, photo=input_photo))
        elif isinstance(entity, Chat):
            # For basic groups, use EditChatPhotoRequest with InputChatUploadedPhoto
            input_photo = InputChatUploadedPhoto(file=uploaded_file)
            await client(
                functions.messages.EditChatPhotoRequest(chat_id=chat_id, photo=input_photo)
            )
        else:
            return f"Cannot edit photo for this entity type ({type(entity)})."

        return f"Chat {chat_id} photo updated."
    except Exception as e:
        logger.exception(f"edit_chat_photo failed (chat_id={chat_id}, file_path='{file_path}')")
        return log_and_format_error("edit_chat_photo", e, chat_id=chat_id, file_path=file_path)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def delete_chat_photo(chat_id: Union[int, str]) -> str:
    """
    Delete the photo of a chat, group, or channel.
    """
    try:
        entity = await client.get_entity(chat_id)
        if isinstance(entity, Channel):
            # Use InputChatPhotoEmpty for channels/supergroups
            await client(
                functions.channels.EditPhotoRequest(channel=entity, photo=InputChatPhotoEmpty())
            )
        elif isinstance(entity, Chat):
            # Use None (or InputChatPhotoEmpty) for basic groups
            await client(
                functions.messages.EditChatPhotoRequest(
                    chat_id=chat_id, photo=InputChatPhotoEmpty()
                )
            )
        else:
            return f"Cannot delete photo for this entity type ({type(entity)})."

        return f"Chat {chat_id} photo deleted."
    except Exception as e:
        logger.exception(f"delete_chat_photo failed (chat_id={chat_id})")
        return log_and_format_error("delete_chat_photo", e, chat_id=chat_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("group_id", "user_id")
async def promote_admin(
    group_id: Union[int, str], user_id: Union[int, str], rights: dict = None
) -> str:
    """
    Promote a user to admin in a group/channel.

    Args:
        group_id: ID or username of the group/channel
        user_id: User ID or username to promote
        rights: Admin rights to give (optional)
    """
    try:
        chat = await client.get_entity(group_id)
        user = await client.get_entity(user_id)

        # Set default admin rights if not provided
        if not rights:
            rights = {
                "change_info": True,
                "post_messages": True,
                "edit_messages": True,
                "delete_messages": True,
                "ban_users": True,
                "invite_users": True,
                "pin_messages": True,
                "add_admins": False,
                "anonymous": False,
                "manage_call": True,
                "other": True,
            }

        admin_rights = ChatAdminRights(
            change_info=rights.get("change_info", True),
            post_messages=rights.get("post_messages", True),
            edit_messages=rights.get("edit_messages", True),
            delete_messages=rights.get("delete_messages", True),
            ban_users=rights.get("ban_users", True),
            invite_users=rights.get("invite_users", True),
            pin_messages=rights.get("pin_messages", True),
            add_admins=rights.get("add_admins", False),
            anonymous=rights.get("anonymous", False),
            manage_call=rights.get("manage_call", True),
            other=rights.get("other", True),
        )

        try:
            result = await client(
                functions.channels.EditAdminRequest(
                    channel=chat, user_id=user, admin_rights=admin_rights, rank="Admin"
                )
            )
            return f"Successfully promoted user {user_id} to admin in {chat.title}"
        except telethon.errors.rpcerrorlist.UserNotMutualContactError:
            return "Error: Cannot promote users who are not mutual contacts. Please ensure the user is in your contacts and has added you back."
        except Exception as e:
            return log_and_format_error("promote_admin", e, group_id=group_id, user_id=user_id)

    except Exception as e:
        logger.error(
            f"telegram_mcp promote_admin failed (group_id={group_id}, user_id={user_id})",
            exc_info=True,
        )
        return log_and_format_error("promote_admin", e, group_id=group_id, user_id=user_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("group_id", "user_id")
async def demote_admin(group_id: Union[int, str], user_id: Union[int, str]) -> str:
    """
    Demote a user from admin in a group/channel.

    Args:
        group_id: ID or username of the group/channel
        user_id: User ID or username to demote
    """
    try:
        chat = await client.get_entity(group_id)
        user = await client.get_entity(user_id)

        # Create empty admin rights (regular user)
        admin_rights = ChatAdminRights(
            change_info=False,
            post_messages=False,
            edit_messages=False,
            delete_messages=False,
            ban_users=False,
            invite_users=False,
            pin_messages=False,
            add_admins=False,
            anonymous=False,
            manage_call=False,
            other=False,
        )

        try:
            result = await client(
                functions.channels.EditAdminRequest(
                    channel=chat, user_id=user, admin_rights=admin_rights, rank=""
                )
            )
            return f"Successfully demoted user {user_id} from admin in {chat.title}"
        except telethon.errors.rpcerrorlist.UserNotMutualContactError:
            return "Error: Cannot modify admin status of users who are not mutual contacts. Please ensure the user is in your contacts and has added you back."
        except Exception as e:
            return log_and_format_error("demote_admin", e, group_id=group_id, user_id=user_id)

    except Exception as e:
        logger.error(
            f"telegram_mcp demote_admin failed (group_id={group_id}, user_id={user_id})",
            exc_info=True,
        )
        return log_and_format_error("demote_admin", e, group_id=group_id, user_id=user_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id", "user_id")
async def ban_user(chat_id: Union[int, str], user_id: Union[int, str]) -> str:
    """
    Ban a user from a group or channel.

    Args:
        chat_id: ID or username of the group/channel
        user_id: User ID or username to ban
    """
    try:
        chat = await client.get_entity(chat_id)
        user = await client.get_entity(user_id)

        # Create banned rights (all restrictions enabled)
        banned_rights = ChatBannedRights(
            until_date=None,  # Ban forever
            view_messages=True,
            send_messages=True,
            send_media=True,
            send_stickers=True,
            send_gifs=True,
            send_games=True,
            send_inline=True,
            embed_links=True,
            send_polls=True,
            change_info=True,
            invite_users=True,
            pin_messages=True,
        )

        try:
            await client(
                functions.channels.EditBannedRequest(
                    channel=chat, participant=user, banned_rights=banned_rights
                )
            )
            return f"User {user_id} banned from chat {chat.title} (ID: {chat_id})."
        except telethon.errors.rpcerrorlist.UserNotMutualContactError:
            return "Error: Cannot ban users who are not mutual contacts. Please ensure the user is in your contacts and has added you back."
        except Exception as e:
            return log_and_format_error("ban_user", e, chat_id=chat_id, user_id=user_id)
    except Exception as e:
        logger.exception(f"ban_user failed (chat_id={chat_id}, user_id={user_id})")
        return log_and_format_error("ban_user", e, chat_id=chat_id, user_id=user_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id", "user_id")
async def unban_user(chat_id: Union[int, str], user_id: Union[int, str]) -> str:
    """
    Unban a user from a group or channel.

    Args:
        chat_id: ID or username of the group/channel
        user_id: User ID or username to unban
    """
    try:
        chat = await client.get_entity(chat_id)
        user = await client.get_entity(user_id)

        # Create unbanned rights (no restrictions)
        unbanned_rights = ChatBannedRights(
            until_date=None,
            view_messages=False,
            send_messages=False,
            send_media=False,
            send_stickers=False,
            send_gifs=False,
            send_games=False,
            send_inline=False,
            embed_links=False,
            send_polls=False,
            change_info=False,
            invite_users=False,
            pin_messages=False,
        )

        try:
            await client(
                functions.channels.EditBannedRequest(
                    channel=chat, participant=user, banned_rights=unbanned_rights
                )
            )
            return f"User {user_id} unbanned from chat {chat.title} (ID: {chat_id})."
        except telethon.errors.rpcerrorlist.UserNotMutualContactError:
            return "Error: Cannot modify status of users who are not mutual contacts. Please ensure the user is in your contacts and has added you back."
        except Exception as e:
            return log_and_format_error("unban_user", e, chat_id=chat_id, user_id=user_id)
    except Exception as e:
        logger.exception(f"unban_user failed (chat_id={chat_id}, user_id={user_id})")
        return log_and_format_error("unban_user", e, chat_id=chat_id, user_id=user_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_admins(chat_id: Union[int, str]) -> str:
    """
    Get all admins in a group or channel.
    """
    try:
        # Fix: Use the correct filter type ChannelParticipantsAdmins
        participants = await client.get_participants(chat_id, filter=ChannelParticipantsAdmins())
        lines = [
            f"ID: {p.id}, Name: {getattr(p, 'first_name', '')} {getattr(p, 'last_name', '')}".strip()
            for p in participants
        ]
        return "\n".join(lines) if lines else "No admins found."
    except Exception as e:
        logger.exception(f"get_admins failed (chat_id={chat_id})")
        return log_and_format_error("get_admins", e, chat_id=chat_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_banned_users(chat_id: Union[int, str]) -> str:
    """
    Get all banned users in a group or channel.
    """
    try:
        # Fix: Use the correct filter type ChannelParticipantsKicked
        participants = await client.get_participants(
            chat_id, filter=ChannelParticipantsKicked(q="")
        )
        lines = [
            f"ID: {p.id}, Name: {getattr(p, 'first_name', '')} {getattr(p, 'last_name', '')}".strip()
            for p in participants
        ]
        return "\n".join(lines) if lines else "No banned users found."
    except Exception as e:
        logger.exception(f"get_banned_users failed (chat_id={chat_id})")
        return log_and_format_error("get_banned_users", e, chat_id=chat_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_invite_link(chat_id: Union[int, str]) -> str:
    """
    Get the invite link for a group or channel.
    """
    try:
        entity = await client.get_entity(chat_id)

        # Try using ExportChatInviteRequest first
        try:
            from telethon.tl import functions

            result = await client(functions.messages.ExportChatInviteRequest(peer=entity))
            return result.link
        except AttributeError:
            # If the function doesn't exist in the current Telethon version
            logger.warning("ExportChatInviteRequest not available, using alternative method")
        except Exception as e1:
            # If that fails, log and try alternative approach
            logger.warning(f"ExportChatInviteRequest failed: {e1}")

        # Alternative approach using client.export_chat_invite_link
        try:
            invite_link = await client.export_chat_invite_link(entity)
            return invite_link
        except Exception as e2:
            logger.warning(f"export_chat_invite_link failed: {e2}")

        # Last resort: Try directly fetching chat info
        try:
            if isinstance(entity, (Chat, Channel)):
                full_chat = await client(functions.messages.GetFullChatRequest(chat_id=entity.id))
                if hasattr(full_chat, "full_chat") and hasattr(full_chat.full_chat, "invite_link"):
                    return full_chat.full_chat.invite_link or "No invite link available."
        except Exception as e3:
            logger.warning(f"GetFullChatRequest failed: {e3}")

        return "Could not retrieve invite link for this chat."
    except Exception as e:
        logger.exception(f"get_invite_link failed (chat_id={chat_id})")
        return log_and_format_error("get_invite_link", e, chat_id=chat_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
async def join_chat_by_link(link: str) -> str:
    """
    Join a chat by invite link.
    """
    try:
        # Extract the hash from the invite link
        if "/" in link:
            hash_part = link.split("/")[-1]
            if hash_part.startswith("+"):
                hash_part = hash_part[1:]  # Remove the '+' if present
        else:
            hash_part = link

        # Try checking the invite before joining
        try:
            # Try to check invite info first (will often fail if not a member)
            invite_info = await client(functions.messages.CheckChatInviteRequest(hash=hash_part))
            if hasattr(invite_info, "chat") and invite_info.chat:
                # If we got chat info, we're already a member
                chat_title = getattr(invite_info.chat, "title", "Unknown Chat")
                return f"You are already a member of this chat: {chat_title}"
        except Exception:
            # This often fails if not a member - just continue
            pass

        # Join the chat using the hash
        result = await client(functions.messages.ImportChatInviteRequest(hash=hash_part))
        if result and hasattr(result, "chats") and result.chats:
            chat_title = getattr(result.chats[0], "title", "Unknown Chat")
            return f"Successfully joined chat: {chat_title}"
        return f"Joined chat via invite hash."
    except Exception as e:
        err_str = str(e).lower()
        if "expired" in err_str:
            return "The invite hash has expired and is no longer valid."
        elif "invalid" in err_str:
            return "The invite hash is invalid or malformed."
        elif "already" in err_str and "participant" in err_str:
            return "You are already a member of this chat."
        logger.exception(f"join_chat_by_link failed (link={link})")
        return f"Error joining chat: {e}"


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def export_chat_invite(chat_id: Union[int, str]) -> str:
    """
    Export a chat invite link.
    """
    try:
        entity = await client.get_entity(chat_id)

        # Try using ExportChatInviteRequest first
        try:
            from telethon.tl import functions

            result = await client(functions.messages.ExportChatInviteRequest(peer=entity))
            return result.link
        except AttributeError:
            # If the function doesn't exist in the current Telethon version
            logger.warning("ExportChatInviteRequest not available, using alternative method")
        except Exception as e1:
            # If that fails, log and try alternative approach
            logger.warning(f"ExportChatInviteRequest failed: {e1}")

        # Alternative approach using client.export_chat_invite_link
        try:
            invite_link = await client.export_chat_invite_link(entity)
            return invite_link
        except Exception as e2:
            logger.warning(f"export_chat_invite_link failed: {e2}")
            return log_and_format_error("export_chat_invite", e2, chat_id=chat_id)

    except Exception as e:
        logger.exception(f"export_chat_invite failed (chat_id={chat_id})")
        return log_and_format_error("export_chat_invite", e, chat_id=chat_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
async def import_chat_invite(hash: str) -> str:
    """
    Import a chat invite by hash.
    """
    try:
        # Remove any prefixes like '+' if present
        if hash.startswith("+"):
            hash = hash[1:]

        # Try checking the invite before joining
        try:
            from telethon.errors import (
                InviteHashExpiredError,
                InviteHashInvalidError,
                UserAlreadyParticipantError,
                ChatAdminRequiredError,
                UsersTooMuchError,
            )

            # Try to check invite info first (will often fail if not a member)
            invite_info = await client(functions.messages.CheckChatInviteRequest(hash=hash))
            if hasattr(invite_info, "chat") and invite_info.chat:
                # If we got chat info, we're already a member
                chat_title = getattr(invite_info.chat, "title", "Unknown Chat")
                return f"You are already a member of this chat: {chat_title}"
        except Exception as check_err:
            # This often fails if not a member - just continue
            pass

        # Join the chat using the hash
        try:
            result = await client(functions.messages.ImportChatInviteRequest(hash=hash))
            if result and hasattr(result, "chats") and result.chats:
                chat_title = getattr(result.chats[0], "title", "Unknown Chat")
                return f"Successfully joined chat: {chat_title}"
            return f"Joined chat via invite hash."
        except Exception as join_err:
            err_str = str(join_err).lower()
            if "expired" in err_str:
                return "The invite hash has expired and is no longer valid."
            elif "invalid" in err_str:
                return "The invite hash is invalid or malformed."
            elif "already" in err_str and "participant" in err_str:
                return "You are already a member of this chat."
            elif "admin" in err_str:
                return "Cannot join this chat - requires admin approval."
            elif "too much" in err_str or "too many" in err_str:
                return "Cannot join this chat - it has reached maximum number of participants."
            else:
                raise  # Re-raise to be caught by the outer exception handler

    except Exception as e:
        logger.exception(f"import_chat_invite failed (hash={hash})")
        return log_and_format_error("import_chat_invite", e, hash=hash)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
@validate_id("chat_id")
async def send_voice(chat_id: Union[int, str], file_path: str) -> str:
    """
    Send a voice message to a chat. File must be an OGG/OPUS voice note.

    Args:
        chat_id: The chat ID or username.
        file_path: Absolute path to the OGG/OPUS file.
    """
    try:
        if not os.path.isfile(file_path):
            return f"File not found: {file_path}"
        if not os.access(file_path, os.R_OK):
            return f"File is not readable: {file_path}"

        mime, _ = mimetypes.guess_type(file_path)
        if not (
            mime
            and (
                mime == "audio/ogg"
                or file_path.lower().endswith(".ogg")
                or file_path.lower().endswith(".opus")
            )
        ):
            return "Voice file must be .ogg or .opus format."

        entity = await client.get_entity(chat_id)
        await client.send_file(entity, file_path, voice_note=True)
        return f"Voice message sent to chat {chat_id}."
    except Exception as e:
        return log_and_format_error("send_voice", e, chat_id=chat_id, file_path=file_path)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
@validate_id("from_chat_id", "to_chat_id")
async def forward_message(
    from_chat_id: Union[int, str], message_id: int, to_chat_id: Union[int, str]
) -> str:
    """
    Forward a message from one chat to another.
    """
    try:
        from_entity = await client.get_entity(from_chat_id)
        to_entity = await client.get_entity(to_chat_id)
        await client.forward_messages(to_entity, message_id, from_entity)
        return f"Message {message_id} forwarded from {from_chat_id} to {to_chat_id}."
    except Exception as e:
        return log_and_format_error(
            "forward_message",
            e,
            from_chat_id=from_chat_id,
            message_id=message_id,
            to_chat_id=to_chat_id,
        )


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def edit_message(chat_id: Union[int, str], message_id: int, new_text: str) -> str:
    """
    Edit a message you sent.
    """
    try:
        entity = await client.get_entity(chat_id)
        edited = await client.edit_message(entity, message_id, new_text)
        if edited:
            record = build_message_record(edited)
            if record.get("chat_id") is None:
                resolved_id = resolve_chat_identifier(chat_id)
                if resolved_id is not None:
                    record["chat_id"] = resolved_id
            store.upsert_message_record(record)
        return f"Message {message_id} edited."
    except Exception as e:
        return log_and_format_error(
            "edit_message", e, chat_id=chat_id, message_id=message_id, new_text=new_text
        )


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def delete_message(chat_id: Union[int, str], message_id: int) -> str:
    """
    Delete a message by ID.
    """
    try:
        entity = await client.get_entity(chat_id)
        await client.delete_messages(entity, message_id)
        resolved_id = resolve_chat_identifier(chat_id)
        if resolved_id is not None:
            store.delete_messages(resolved_id, [message_id])
        return f"Message {message_id} deleted."
    except Exception as e:
        return log_and_format_error("delete_message", e, chat_id=chat_id, message_id=message_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def pin_message(chat_id: Union[int, str], message_id: int) -> str:
    """
    Pin a message in a chat.
    """
    try:
        entity = await client.get_entity(chat_id)
        await client.pin_message(entity, message_id)
        resolved_id = resolve_chat_identifier(chat_id)
        if resolved_id is not None:
            store.set_message_pinned(resolved_id, message_id, True)
        return f"Message {message_id} pinned in chat {chat_id}."
    except Exception as e:
        return log_and_format_error("pin_message", e, chat_id=chat_id, message_id=message_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def unpin_message(chat_id: Union[int, str], message_id: int) -> str:
    """
    Unpin a message in a chat.
    """
    try:
        entity = await client.get_entity(chat_id)
        await client.unpin_message(entity, message_id)
        resolved_id = resolve_chat_identifier(chat_id)
        if resolved_id is not None:
            store.set_message_pinned(resolved_id, message_id, False)
        return f"Message {message_id} unpinned in chat {chat_id}."
    except Exception as e:
        return log_and_format_error("unpin_message", e, chat_id=chat_id, message_id=message_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def mark_as_read(chat_id: Union[int, str]) -> str:
    """
    Mark all messages as read in a chat.
    """
    try:
        entity = await client.get_entity(chat_id)
        await client.send_read_acknowledge(entity)
        resolved_id = resolve_chat_identifier(chat_id)
        if resolved_id is not None:
            store.update_unread_count(resolved_id, 0)
        return f"Marked all messages as read in chat {chat_id}."
    except Exception as e:
        return log_and_format_error("mark_as_read", e, chat_id=chat_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
@validate_id("chat_id")
async def reply_to_message(chat_id: Union[int, str], message_id: int, text: str) -> str:
    """
    Reply to a specific message in a chat.
    """
    try:
        entity = await client.get_entity(chat_id)
        sent = await client.send_message(entity, text, reply_to=message_id)
        if sent:
            record = build_message_record(sent)
            if record.get("chat_id") is None:
                resolved_id = resolve_chat_identifier(chat_id)
                if resolved_id is not None:
                    record["chat_id"] = resolved_id
            store.upsert_message_record(record)
        return f"Replied to message {message_id} in chat {chat_id}."
    except Exception as e:
        return log_and_format_error(
            "reply_to_message", e, chat_id=chat_id, message_id=message_id, text=text
        )


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_media_info(chat_id: Union[int, str], message_id: int) -> str:
    """
    Get info about media in a message.

    Args:
        chat_id: The chat ID or username.
        message_id: The message ID.
    """
    try:
        entity = await client.get_entity(chat_id)
        msg = await client.get_messages(entity, ids=message_id)

        if not msg or not msg.media:
            return "No media found in the specified message."

        return str(msg.media)
    except Exception as e:
        return log_and_format_error("get_media_info", e, chat_id=chat_id, message_id=message_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def search_public_chats(query: str) -> str:
    """
    Search for public chats, channels, or bots by username or title.
    """
    try:
        result = await client(functions.contacts.SearchRequest(q=query, limit=20))
        return json.dumps([format_entity(u) for u in result.users], indent=2)
    except Exception as e:
        return log_and_format_error("search_public_chats", e, query=query)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def search_messages(chat_id: Union[int, str], query: str, limit: int = 20) -> str:
    """
    Search for messages in a chat by text.
    """
    try:
        entity = await client.get_entity(chat_id)
        messages = await client.get_messages(entity, limit=limit, search=query)

        lines = []
        for msg in messages:
            sender_name = get_sender_name(msg)
            reply_info = ""
            if msg.reply_to and msg.reply_to.reply_to_msg_id:
                reply_info = f" | reply to {msg.reply_to.reply_to_msg_id}"
            lines.append(
                f"ID: {msg.id} | {sender_name} | Date: {msg.date}{reply_info} | Message: {msg.message}"
            )
        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error(
            "search_messages", e, chat_id=chat_id, query=query, limit=limit
        )


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def resolve_username(username: str) -> str:
    """
    Resolve a username to a user or chat ID.
    """
    try:
        result = await client(functions.contacts.ResolveUsernameRequest(username=username))
        return str(result)
    except Exception as e:
        return log_and_format_error("resolve_username", e, username=username)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def mute_chat(chat_id: Union[int, str]) -> str:
    """
    Mute notifications for a chat.
    """
    try:
        from telethon.tl.types import InputPeerNotifySettings

        peer = await client.get_entity(chat_id)
        await client(
            functions.account.UpdateNotifySettingsRequest(
                peer=peer, settings=InputPeerNotifySettings(mute_until=2**31 - 1)
            )
        )
        return f"Chat {chat_id} muted."
    except (ImportError, AttributeError) as type_err:
        try:
            # Alternative approach directly using raw API
            peer = await client.get_input_entity(chat_id)
            await client(
                functions.account.UpdateNotifySettingsRequest(
                    peer=peer,
                    settings={
                        "mute_until": 2**31 - 1,  # Far future
                        "show_previews": False,
                        "silent": True,
                    },
                )
            )
            return f"Chat {chat_id} muted (using alternative method)."
        except Exception as alt_e:
            logger.exception(f"mute_chat (alt method) failed (chat_id={chat_id})")
            return log_and_format_error("mute_chat", alt_e, chat_id=chat_id)
    except Exception as e:
        logger.exception(f"mute_chat failed (chat_id={chat_id})")
        return log_and_format_error("mute_chat", e, chat_id=chat_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def unmute_chat(chat_id: Union[int, str]) -> str:
    """
    Unmute notifications for a chat.
    """
    try:
        from telethon.tl.types import InputPeerNotifySettings

        peer = await client.get_entity(chat_id)
        await client(
            functions.account.UpdateNotifySettingsRequest(
                peer=peer, settings=InputPeerNotifySettings(mute_until=0)
            )
        )
        return f"Chat {chat_id} unmuted."
    except (ImportError, AttributeError) as type_err:
        try:
            # Alternative approach directly using raw API
            peer = await client.get_input_entity(chat_id)
            await client(
                functions.account.UpdateNotifySettingsRequest(
                    peer=peer,
                    settings={
                        "mute_until": 0,  # Unmute (current time)
                        "show_previews": True,
                        "silent": False,
                    },
                )
            )
            return f"Chat {chat_id} unmuted (using alternative method)."
        except Exception as alt_e:
            logger.exception(f"unmute_chat (alt method) failed (chat_id={chat_id})")
            return log_and_format_error("unmute_chat", alt_e, chat_id=chat_id)
    except Exception as e:
        logger.exception(f"unmute_chat failed (chat_id={chat_id})")
        return log_and_format_error("unmute_chat", e, chat_id=chat_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def archive_chat(chat_id: Union[int, str]) -> str:
    """
    Archive a chat.
    """
    try:
        await client(
            functions.messages.ToggleDialogPinRequest(
                peer=await client.get_entity(chat_id), pinned=True
            )
        )
        return f"Chat {chat_id} archived."
    except Exception as e:
        return log_and_format_error("archive_chat", e, chat_id=chat_id)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
@validate_id("chat_id")
async def unarchive_chat(chat_id: Union[int, str]) -> str:
    """
    Unarchive a chat.
    """
    try:
        await client(
            functions.messages.ToggleDialogPinRequest(
                peer=await client.get_entity(chat_id), pinned=False
            )
        )
        return f"Chat {chat_id} unarchived."
    except Exception as e:
        return log_and_format_error("unarchive_chat", e, chat_id=chat_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def get_sticker_sets() -> str:
    """
    Get all sticker sets.
    """
    try:
        result = await client(functions.messages.GetAllStickersRequest(hash=0))
        return json.dumps([s.title for s in result.sets], indent=2)
    except Exception as e:
        return log_and_format_error("get_sticker_sets", e)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
@validate_id("chat_id")
async def send_sticker(chat_id: Union[int, str], file_path: str) -> str:
    """
    Send a sticker to a chat. File must be a valid .webp sticker file.

    Args:
        chat_id: The chat ID or username.
        file_path: Absolute path to the .webp sticker file.
    """
    try:
        if not os.path.isfile(file_path):
            return f"Sticker file not found: {file_path}"
        if not os.access(file_path, os.R_OK):
            return f"Sticker file is not readable: {file_path}"
        if not file_path.lower().endswith(".webp"):
            return "Sticker file must be a .webp file."

        entity = await client.get_entity(chat_id)
        await client.send_file(entity, file_path, force_document=False)
        return f"Sticker sent to chat {chat_id}."
    except Exception as e:
        return log_and_format_error("send_sticker", e, chat_id=chat_id, file_path=file_path)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def get_gif_search(query: str, limit: int = 10) -> str:
    """
    Search for GIFs by query. Returns a list of Telegram document IDs (not file paths).

    Args:
        query: Search term for GIFs.
        limit: Max number of GIFs to return.
    """
    try:
        # Try approach 1: SearchGifsRequest
        try:
            result = await client(
                functions.messages.SearchGifsRequest(q=query, offset_id=0, limit=limit)
            )
            if not result.gifs:
                return "[]"
            return json.dumps(
                [g.document.id for g in result.gifs], indent=2, default=json_serializer
            )
        except (AttributeError, ImportError):
            # Fallback approach: Use SearchRequest with GIF filter
            try:
                from telethon.tl.types import InputMessagesFilterGif

                result = await client(
                    functions.messages.SearchRequest(
                        peer="gif",
                        q=query,
                        filter=InputMessagesFilterGif(),
                        min_date=None,
                        max_date=None,
                        offset_id=0,
                        add_offset=0,
                        limit=limit,
                        max_id=0,
                        min_id=0,
                        hash=0,
                    )
                )
                if not result or not hasattr(result, "messages") or not result.messages:
                    return "[]"
                # Extract document IDs from any messages with media
                gif_ids = []
                for msg in result.messages:
                    if hasattr(msg, "media") and msg.media and hasattr(msg.media, "document"):
                        gif_ids.append(msg.media.document.id)
                return json.dumps(gif_ids, default=json_serializer)
            except Exception as inner_e:
                # Last resort: Try to fetch from a public bot
                return f"Could not search GIFs using available methods: {inner_e}"
    except Exception as e:
        logger.exception(f"get_gif_search failed (query={query}, limit={limit})")
        return log_and_format_error("get_gif_search", e, query=query, limit=limit)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
@validate_id("chat_id")
async def send_gif(chat_id: Union[int, str], gif_id: int) -> str:
    """
    Send a GIF to a chat by Telegram GIF document ID (not a file path).

    Args:
        chat_id: The chat ID or username.
        gif_id: Telegram document ID for the GIF (from get_gif_search).
    """
    try:
        if not isinstance(gif_id, int):
            return "gif_id must be a Telegram document ID (integer), not a file path. Use get_gif_search to find IDs."
        entity = await client.get_entity(chat_id)
        await client.send_file(entity, gif_id)
        return f"GIF sent to chat {chat_id}."
    except Exception as e:
        return log_and_format_error("send_gif", e, chat_id=chat_id, gif_id=gif_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
async def get_bot_info(bot_username: str) -> str:
    """
    Get information about a bot by username.
    """
    try:
        entity = await client.get_entity(bot_username)
        if not entity:
            return f"Bot with username {bot_username} not found."

        result = await client(functions.users.GetFullUserRequest(id=entity))

        # Create a more structured, serializable response
        if hasattr(result, "to_dict"):
            # Use custom serializer to handle non-serializable types
            return json.dumps(result.to_dict(), indent=2, default=json_serializer)
        else:
            # Fallback if to_dict is not available
            info = {
                "bot_info": {
                    "id": entity.id,
                    "username": entity.username,
                    "first_name": entity.first_name,
                    "last_name": getattr(entity, "last_name", ""),
                    "is_bot": getattr(entity, "bot", False),
                    "verified": getattr(entity, "verified", False),
                }
            }
            if hasattr(result, "full_user") and hasattr(result.full_user, "about"):
                info["bot_info"]["about"] = result.full_user.about
            return json.dumps(info, indent=2)
    except Exception as e:
        logger.exception(f"get_bot_info failed (bot_username={bot_username})")
        return log_and_format_error("get_bot_info", e, bot_username=bot_username)


@mcp.tool(
    annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True, idempotentHint=True)
)
async def set_bot_commands(bot_username: str, commands: list) -> str:
    """
    Set bot commands for a bot you own.
    Note: This function can only be used if the Telegram client is a bot account.
    Regular user accounts cannot set bot commands.

    Args:
        bot_username: The username of the bot to set commands for.
        commands: List of command dictionaries with 'command' and 'description' keys.
    """
    try:
        # First check if the current client is a bot
        me = await client.get_me()
        if not getattr(me, "bot", False):
            return "Error: This function can only be used by bot accounts. Your current Telegram account is a regular user account, not a bot."

        # Import required types
        from telethon.tl.types import BotCommand, BotCommandScopeDefault
        from telethon.tl.functions.bots import SetBotCommandsRequest

        # Create BotCommand objects from the command dictionaries
        bot_commands = [
            BotCommand(command=c["command"], description=c["description"]) for c in commands
        ]

        # Get the bot entity
        bot = await client.get_entity(bot_username)

        # Set the commands with proper scope
        await client(
            SetBotCommandsRequest(
                scope=BotCommandScopeDefault(),
                lang_code="en",  # Default language code
                commands=bot_commands,
            )
        )

        return f"Bot commands set for {bot_username}."
    except ImportError as ie:
        logger.exception(f"set_bot_commands failed - ImportError: {ie}")
        return log_and_format_error("set_bot_commands", ie)
    except Exception as e:
        logger.exception(f"set_bot_commands failed (bot_username={bot_username})")
        return log_and_format_error("set_bot_commands", e, bot_username=bot_username)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_history(chat_id: Union[int, str], limit: int = 100) -> str:
    """
    Get full chat history (up to limit).
    """
    try:
        entity = await client.get_entity(chat_id)
        messages = await client.get_messages(entity, limit=limit)

        lines = []
        for msg in messages:
            sender_name = get_sender_name(msg)
            reply_info = ""
            if msg.reply_to and msg.reply_to.reply_to_msg_id:
                reply_info = f" | reply to {msg.reply_to.reply_to_msg_id}"
            lines.append(
                f"ID: {msg.id} | {sender_name} | Date: {msg.date}{reply_info} | Message: {msg.message}"
            )
        return "\n".join(lines)
    except Exception as e:
        return log_and_format_error("get_history", e, chat_id=chat_id, limit=limit)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("user_id")
async def get_user_photos(user_id: Union[int, str], limit: int = 10) -> str:
    """
    Get profile photos of a user.
    """
    try:
        user = await client.get_entity(user_id)
        photos = await client(
            functions.photos.GetUserPhotosRequest(user_id=user, offset=0, max_id=0, limit=limit)
        )
        return json.dumps([p.id for p in photos.photos], indent=2)
    except Exception as e:
        return log_and_format_error("get_user_photos", e, user_id=user_id, limit=limit)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("user_id")
async def get_user_status(user_id: Union[int, str]) -> str:
    """
    Get the online status of a user.
    """
    try:
        user = await client.get_entity(user_id)
        return str(user.status)
    except Exception as e:
        return log_and_format_error("get_user_status", e, user_id=user_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_recent_actions(chat_id: Union[int, str]) -> str:
    """
    Get recent admin actions (admin log) in a group or channel.
    """
    try:
        result = await client(
            functions.channels.GetAdminLogRequest(
                channel=chat_id,
                q="",
                events_filter=None,
                admins=[],
                max_id=0,
                min_id=0,
                limit=20,
            )
        )

        if not result or not result.events:
            return "No recent admin actions found."

        # Use the custom serializer to handle datetime objects
        return json.dumps([e.to_dict() for e in result.events], indent=2, default=json_serializer)
    except Exception as e:
        logger.exception(f"get_recent_actions failed (chat_id={chat_id})")
        return log_and_format_error("get_recent_actions", e, chat_id=chat_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, readOnlyHint=True))
@validate_id("chat_id")
async def get_pinned_messages(chat_id: Union[int, str]) -> str:
    """
    Get all pinned messages in a chat.
    """
    try:
        entity = await client.get_entity(chat_id)

        # Use correct filter based on Telethon version
        try:
            # Try newer Telethon approach
            from telethon.tl.types import InputMessagesFilterPinned

            messages = await client.get_messages(entity, filter=InputMessagesFilterPinned())
        except (ImportError, AttributeError):
            # Fallback - try without filter and manually filter pinned
            all_messages = await client.get_messages(entity, limit=50)
            messages = [m for m in all_messages if getattr(m, "pinned", False)]

        if not messages:
            return "No pinned messages found in this chat."

        lines = []
        for msg in messages:
            sender_name = get_sender_name(msg)
            reply_info = ""
            if msg.reply_to and msg.reply_to.reply_to_msg_id:
                reply_info = f" | reply to {msg.reply_to.reply_to_msg_id}"
            lines.append(
                f"ID: {msg.id} | {sender_name} | Date: {msg.date}{reply_info} | Message: {msg.message or '[Media/No text]'}"
            )

        return "\n".join(lines)
    except Exception as e:
        logger.exception(f"get_pinned_messages failed (chat_id={chat_id})")
        return log_and_format_error("get_pinned_messages", e, chat_id=chat_id)


@mcp.tool(annotations=ToolAnnotations(openWorldHint=True, destructiveHint=True))
async def create_poll(
    chat_id: int,
    question: str,
    options: list,
    multiple_choice: bool = False,
    quiz_mode: bool = False,
    public_votes: bool = True,
    close_date: str = None,
) -> str:
    """
    Create a poll in a chat using Telegram's native poll feature.

    Args:
        chat_id: The ID of the chat to send the poll to
        question: The poll question
        options: List of answer options (2-10 options)
        multiple_choice: Whether users can select multiple answers
        quiz_mode: Whether this is a quiz (has correct answer)
        public_votes: Whether votes are public
        close_date: Optional close date in ISO format (YYYY-MM-DD HH:MM:SS)
    """
    try:
        entity = await client.get_entity(chat_id)

        # Validate options
        if len(options) < 2:
            return "Error: Poll must have at least 2 options."
        if len(options) > 10:
            return "Error: Poll can have at most 10 options."

        # Parse close date if provided
        close_date_obj = None
        if close_date:
            try:
                close_date_obj = datetime.fromisoformat(close_date.replace("Z", "+00:00"))
            except ValueError:
                return f"Invalid close_date format. Use YYYY-MM-DD HH:MM:SS format."

        # Create the poll using InputMediaPoll with SendMediaRequest
        from telethon.tl.types import InputMediaPoll, Poll, PollAnswer, TextWithEntities
        import random

        poll = Poll(
            id=random.randint(0, 2**63 - 1),
            question=TextWithEntities(text=question, entities=[]),
            answers=[
                PollAnswer(text=TextWithEntities(text=option, entities=[]), option=bytes([i]))
                for i, option in enumerate(options)
            ],
            multiple_choice=multiple_choice,
            quiz=quiz_mode,
            public_voters=public_votes,
            close_date=close_date_obj,
        )

        result = await client(
            functions.messages.SendMediaRequest(
                peer=entity,
                media=InputMediaPoll(poll=poll),
                message="",
                random_id=random.randint(0, 2**63 - 1),
            )
        )

        return f"Poll created successfully in chat {chat_id}."
    except Exception as e:
        logger.exception(f"create_poll failed (chat_id={chat_id}, question='{question}')")
        return log_and_format_error(
            "create_poll", e, chat_id=chat_id, question=question, options=options
        )


async def _main() -> None:
    try:
        # Start the Telethon client non-interactively
        print("Starting Telegram client...")
        await client.start()

        print("Telegram client started. Syncing chats...")
        await sync_chats_from_telegram()
        print("Syncing new messages...")
        await sync_new_messages()

        print("Telegram client started. Running MCP server...")
        # Use the asynchronous entrypoint instead of mcp.run()
        await mcp.run_stdio_async()
    except Exception as e:
        print(f"Error starting client: {e}", file=sys.stderr)
        if isinstance(e, sqlite3.OperationalError) and "database is locked" in str(e):
            print(
                "Database lock detected. Please ensure no other instances are running.",
                file=sys.stderr,
            )
        sys.exit(1)


def main() -> None:
    nest_asyncio.apply()
    asyncio.run(_main())


if __name__ == "__main__":
    main()
