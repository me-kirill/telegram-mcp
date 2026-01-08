import base64
import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from telethon import utils
from telethon.tl.types import Channel, Chat, User


def _now_ts() -> int:
    return int(time.time())


def _to_timestamp(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def _normalize_username(username: Optional[str]) -> Optional[str]:
    if not username:
        return None
    return username.lstrip("@").lower()


def db_json_serializer(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return "base64:" + base64.b64encode(obj).decode("ascii")
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return str(obj)


def decode_db_bytes(value: Any) -> Optional[bytes]:
    if isinstance(value, str) and value.startswith("base64:"):
        encoded = value[len("base64:") :]
        try:
            return base64.b64decode(encoded)
        except (ValueError, TypeError):
            return None
    if isinstance(value, list):
        try:
            return bytes(value)
        except (ValueError, TypeError):
            return None
    return None


def _extract_sender_name(message: Any) -> str:
    sender = getattr(message, "sender", None)
    if sender is None:
        post_author = getattr(message, "post_author", None)
        if post_author:
            return post_author
        return "Unknown"
    if hasattr(sender, "title") and sender.title:
        return sender.title
    if hasattr(sender, "first_name"):
        first = getattr(sender, "first_name", "") or ""
        last = getattr(sender, "last_name", "") or ""
        full = f"{first} {last}".strip()
        return full or "Unknown"
    return "Unknown"


def build_chat_record(entity: Any, dialog: Optional[Any] = None) -> Dict[str, Any]:
    chat_type = "unknown"
    title = None
    username = None
    phone = None
    is_bot = None
    is_verified = None

    if isinstance(entity, User):
        chat_type = "user"
        name_parts = []
        if entity.first_name:
            name_parts.append(entity.first_name)
        if getattr(entity, "last_name", None):
            name_parts.append(entity.last_name)
        title = " ".join(name_parts) if name_parts else "Unknown"
        username = _normalize_username(getattr(entity, "username", None))
        phone = getattr(entity, "phone", None)
        is_bot = int(bool(getattr(entity, "bot", False)))
        is_verified = int(bool(getattr(entity, "verified", False)))
    elif isinstance(entity, Chat):
        chat_type = "group"
        title = getattr(entity, "title", None) or "Unknown"
    elif isinstance(entity, Channel):
        if getattr(entity, "broadcast", False):
            chat_type = "channel"
        else:
            chat_type = "group"
        title = getattr(entity, "title", None) or "Unknown"
        username = _normalize_username(getattr(entity, "username", None))

    peer_id = utils.get_peer_id(entity)
    record = {
        "id": peer_id,
        "type": chat_type,
        "title": title,
        "username": username,
        "phone": phone,
        "is_bot": is_bot,
        "is_verified": is_verified,
        "unread_count": None,
        "last_message_id": None,
        "last_message_date": None,
        "raw_json": json.dumps(entity.to_dict(), default=db_json_serializer),
        "updated_at": _now_ts(),
    }

    if dialog is not None:
        if hasattr(dialog, "unread_count"):
            record["unread_count"] = dialog.unread_count
        if getattr(dialog, "message", None) is not None:
            last_message = dialog.message
            record["last_message_id"] = getattr(last_message, "id", None)
            record["last_message_date"] = _to_timestamp(getattr(last_message, "date", None))

    return record


def build_message_record(message: Any) -> Dict[str, Any]:
    reply_to_msg_id = None
    if getattr(message, "reply_to", None) and getattr(message.reply_to, "reply_to_msg_id", None):
        reply_to_msg_id = message.reply_to.reply_to_msg_id

    chat_id = getattr(message, "chat_id", None)
    if chat_id is None and getattr(message, "peer_id", None):
        chat_id = utils.get_peer_id(message.peer_id)

    return {
        "chat_id": chat_id,
        "message_id": message.id,
        "date": _to_timestamp(getattr(message, "date", None)),
        "sender_id": getattr(message, "sender_id", None),
        "sender_name": _extract_sender_name(message),
        "text": message.message or "",
        "reply_to_msg_id": reply_to_msg_id,
        "has_media": int(bool(getattr(message, "media", None))),
        "media_type": type(message.media).__name__ if getattr(message, "media", None) else None,
        "pinned": int(bool(getattr(message, "pinned", False))),
        "edit_date": _to_timestamp(getattr(message, "edit_date", None)),
        "out": int(bool(getattr(message, "out", False))),
        "raw_json": json.dumps(message.to_dict(), default=db_json_serializer),
    }


class SQLiteStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._apply_pragmas()

    def _apply_pragmas(self) -> None:
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY,
                type TEXT,
                title TEXT,
                username TEXT,
                phone TEXT,
                is_bot INTEGER,
                is_verified INTEGER,
                unread_count INTEGER,
                last_message_id INTEGER,
                last_message_date INTEGER,
                raw_json TEXT,
                updated_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                date INTEGER NOT NULL,
                sender_id INTEGER,
                sender_name TEXT,
                text TEXT,
                reply_to_msg_id INTEGER,
                has_media INTEGER,
                media_type TEXT,
                pinned INTEGER,
                edit_date INTEGER,
                out INTEGER,
                raw_json TEXT,
                PRIMARY KEY (chat_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_messages_chat_date
                ON messages (chat_id, date);
            CREATE INDEX IF NOT EXISTS idx_messages_chat_text
                ON messages (chat_id, text);
            CREATE INDEX IF NOT EXISTS idx_messages_chat_id
                ON messages (chat_id, message_id);
            CREATE INDEX IF NOT EXISTS idx_messages_sender_id
                ON messages (sender_id);
            CREATE INDEX IF NOT EXISTS idx_messages_sender_date
                ON messages (sender_id, date);
            """
        )
        self.conn.commit()

    def upsert_chat_record(self, record: Dict[str, Any]) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO chats (
                    id, type, title, username, phone, is_bot, is_verified,
                    unread_count, last_message_id, last_message_date, raw_json, updated_at
                ) VALUES (
                    :id, :type, :title, :username, :phone, :is_bot, :is_verified,
                    :unread_count, :last_message_id, :last_message_date, :raw_json, :updated_at
                )
                ON CONFLICT(id) DO UPDATE SET
                    type = COALESCE(excluded.type, chats.type),
                    title = COALESCE(excluded.title, chats.title),
                    username = COALESCE(excluded.username, chats.username),
                    phone = COALESCE(excluded.phone, chats.phone),
                    is_bot = COALESCE(excluded.is_bot, chats.is_bot),
                    is_verified = COALESCE(excluded.is_verified, chats.is_verified),
                    unread_count = COALESCE(excluded.unread_count, chats.unread_count),
                    last_message_id = CASE
                        WHEN excluded.last_message_id IS NOT NULL
                             AND (chats.last_message_id IS NULL
                                  OR excluded.last_message_id > chats.last_message_id)
                        THEN excluded.last_message_id
                        ELSE chats.last_message_id
                    END,
                    last_message_date = CASE
                        WHEN excluded.last_message_date IS NOT NULL
                             AND (chats.last_message_date IS NULL
                                  OR excluded.last_message_date > chats.last_message_date)
                        THEN excluded.last_message_date
                        ELSE chats.last_message_date
                    END,
                    raw_json = COALESCE(excluded.raw_json, chats.raw_json),
                    updated_at = COALESCE(excluded.updated_at, chats.updated_at)
                """,
                record,
            )

    def ensure_chat_placeholder(self, chat_id: int) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO chats (id, type, title, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (chat_id, "unknown", None, _now_ts()),
            )

    def upsert_message_record(self, record: Dict[str, Any]) -> None:
        chat_id = record.get("chat_id")
        if chat_id is not None:
            self.ensure_chat_placeholder(chat_id)

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO messages (
                    chat_id, message_id, date, sender_id, sender_name, text,
                    reply_to_msg_id, has_media, media_type, pinned, edit_date, out, raw_json
                ) VALUES (
                    :chat_id, :message_id, :date, :sender_id, :sender_name, :text,
                    :reply_to_msg_id, :has_media, :media_type, :pinned, :edit_date, :out, :raw_json
                )
                ON CONFLICT(chat_id, message_id) DO UPDATE SET
                    date = excluded.date,
                    sender_id = excluded.sender_id,
                    sender_name = excluded.sender_name,
                    text = excluded.text,
                    reply_to_msg_id = excluded.reply_to_msg_id,
                    has_media = excluded.has_media,
                    media_type = excluded.media_type,
                    pinned = excluded.pinned,
                    edit_date = excluded.edit_date,
                    out = excluded.out,
                    raw_json = excluded.raw_json
                """,
                record,
            )

            if chat_id is not None and record.get("message_id") is not None:
                self.conn.execute(
                    """
                    UPDATE chats
                    SET last_message_id = CASE
                            WHEN last_message_id IS NULL OR ? > last_message_id THEN ?
                            ELSE last_message_id
                        END,
                        last_message_date = CASE
                            WHEN last_message_date IS NULL OR ? > last_message_date THEN ?
                            ELSE last_message_date
                        END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        record["message_id"],
                        record["message_id"],
                        record["date"],
                        record["date"],
                        _now_ts(),
                        chat_id,
                    ),
                )

    def upsert_messages(self, records: Iterable[Dict[str, Any]]) -> None:
        records = list(records)
        if not records:
            return
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO messages (
                    chat_id, message_id, date, sender_id, sender_name, text,
                    reply_to_msg_id, has_media, media_type, pinned, edit_date, out, raw_json
                ) VALUES (
                    :chat_id, :message_id, :date, :sender_id, :sender_name, :text,
                    :reply_to_msg_id, :has_media, :media_type, :pinned, :edit_date, :out, :raw_json
                )
                ON CONFLICT(chat_id, message_id) DO UPDATE SET
                    date = excluded.date,
                    sender_id = excluded.sender_id,
                    sender_name = excluded.sender_name,
                    text = excluded.text,
                    reply_to_msg_id = excluded.reply_to_msg_id,
                    has_media = excluded.has_media,
                    media_type = excluded.media_type,
                    pinned = excluded.pinned,
                    edit_date = excluded.edit_date,
                    out = excluded.out,
                    raw_json = excluded.raw_json
                """,
                records,
            )

        for record in records:
            chat_id = record.get("chat_id")
            if chat_id is None:
                continue
            self.ensure_chat_placeholder(chat_id)
            with self.conn:
                self.conn.execute(
                    """
                    UPDATE chats
                    SET last_message_id = CASE
                            WHEN last_message_id IS NULL OR ? > last_message_id THEN ?
                            ELSE last_message_id
                        END,
                        last_message_date = CASE
                            WHEN last_message_date IS NULL OR ? > last_message_date THEN ?
                            ELSE last_message_date
                        END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        record["message_id"],
                        record["message_id"],
                        record["date"],
                        record["date"],
                        _now_ts(),
                        chat_id,
                    ),
                )

    def delete_messages(self, chat_id: int, message_ids: Iterable[int]) -> int:
        message_ids = list(message_ids)
        if not message_ids:
            return 0
        placeholders = ",".join("?" for _ in message_ids)
        with self.conn:
            cursor = self.conn.execute(
                f"DELETE FROM messages WHERE chat_id = ? AND message_id IN ({placeholders})",
                [chat_id, *message_ids],
            )
        return cursor.rowcount

    def find_chat_ids_by_message_id(self, message_id: int) -> List[int]:
        rows = self.conn.execute(
            "SELECT chat_id FROM messages WHERE message_id = ?",
            (message_id,),
        ).fetchall()
        return [row[0] for row in rows]

    def set_message_pinned(self, chat_id: int, message_id: int, pinned: bool) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE messages SET pinned = ? WHERE chat_id = ? AND message_id = ?",
                (1 if pinned else 0, chat_id, message_id),
            )

    def get_meta(self, key: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            (key,),
        ).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO meta (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def get_meta_int(self, key: str) -> Optional[int]:
        value = self.get_meta(key)
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def set_meta_int(self, key: str, value: int) -> None:
        self.set_meta(key, str(value))

    def update_unread_count(self, chat_id: int, unread_count: int) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE chats SET unread_count = ?, updated_at = ? WHERE id = ?",
                (unread_count, _now_ts(), chat_id),
            )

    def increment_unread(self, chat_id: int) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE chats
                SET unread_count = COALESCE(unread_count, 0) + 1,
                    updated_at = ?
                WHERE id = ?
                """,
                (_now_ts(), chat_id),
            )

    def get_chat_by_id(self, chat_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute("SELECT * FROM chats WHERE id = ?", (chat_id,)).fetchone()
        return dict(row) if row else None

    def get_chat_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        normalized = _normalize_username(username)
        if not normalized:
            return None
        row = self.conn.execute("SELECT * FROM chats WHERE username = ?", (normalized,)).fetchone()
        return dict(row) if row else None

    def get_chats(
        self,
        page: int,
        page_size: int,
        chat_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        offset = (page - 1) * page_size
        if chat_type:
            rows = self.conn.execute(
                """
                SELECT * FROM chats
                WHERE type = ?
                ORDER BY COALESCE(last_message_date, 0) DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (chat_type, page_size, offset),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT * FROM chats
                ORDER BY COALESCE(last_message_date, 0) DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (page_size, offset),
            ).fetchall()
        return [dict(row) for row in rows]

    def search_chats(self, query: str, chat_type: Optional[str] = None) -> List[Dict[str, Any]]:
        pattern = f"%{query}%"
        if chat_type:
            rows = self.conn.execute(
                """
                SELECT * FROM chats
                WHERE type = ? AND (
                    title LIKE ? COLLATE NOCASE
                    OR username LIKE ? COLLATE NOCASE
                    OR phone LIKE ?
                )
                ORDER BY COALESCE(last_message_date, 0) DESC, id DESC
                """,
                (chat_type, pattern, pattern, pattern),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT * FROM chats
                WHERE title LIKE ? COLLATE NOCASE
                   OR username LIKE ? COLLATE NOCASE
                   OR phone LIKE ?
                ORDER BY COALESCE(last_message_date, 0) DESC, id DESC
                """,
                (pattern, pattern, pattern),
            ).fetchall()
        return [dict(row) for row in rows]

    def find_senders_by_identifier(self, identifier: Any) -> List[Dict[str, Any]]:
        """
        Find potential sender IDs based on identifier.

        Args:
            identifier: Can be:
                - Integer sender_id
                - String numeric ID ("123456")
                - Username ("@username" or "username")
                - Name ("Владлен") - may match multiple people

        Returns:
            List of matching chats (users) with id, title, username
        """
        import re

        # Case 1: Integer - direct lookup
        if isinstance(identifier, int):
            chat = self.get_chat_by_id(identifier)
            return [chat] if chat else []

        if isinstance(identifier, str):
            trimmed = identifier.strip()

            # Numeric string
            if trimmed.lstrip("-").isdigit():
                numeric = int(trimmed)
                chat = self.get_chat_by_id(numeric)
                return [chat] if chat else []

            # Username (with or without @)
            if trimmed.startswith("@") or re.match(r"^[a-zA-Z][a-zA-Z0-9_]{4,}$", trimmed):
                username = trimmed.lstrip("@").lower()
                chat = self.get_chat_by_username(username)
                return [chat] if chat else []

            # Name search - fuzzy match on title for users only
            pattern = f"%{trimmed}%"
            rows = self.conn.execute(
                """
                SELECT * FROM chats
                WHERE type = 'user' AND title LIKE ? COLLATE NOCASE
                ORDER BY COALESCE(last_message_date, 0) DESC
                LIMIT 20
                """,
                (pattern,),
            ).fetchall()
            return [dict(row) for row in rows]

        return []

    def get_messages(self, chat_id: int, limit: int, offset: int = 0) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM messages
            WHERE chat_id = ?
            ORDER BY message_id DESC
            LIMIT ? OFFSET ?
            """,
            (chat_id, limit, offset),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_messages(
        self,
        chat_id: int,
        limit: int,
        search_query: Optional[str] = None,
        from_ts: Optional[int] = None,
        to_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        conditions = ["chat_id = ?"]
        params: List[Any] = [chat_id]

        if search_query:
            conditions.append("text LIKE ? COLLATE NOCASE")
            params.append(f"%{search_query}%")
        if from_ts is not None:
            conditions.append("date >= ?")
            params.append(from_ts)
        if to_ts is not None:
            conditions.append("date <= ?")
            params.append(to_ts)

        where_clause = " AND ".join(conditions)
        query = (
            "SELECT * FROM messages "
            f"WHERE {where_clause} "
            "ORDER BY date DESC, message_id DESC "
            "LIMIT ?"
        )
        params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def search_messages_by_sender(
        self,
        sender_ids: List[int],
        limit: int,
        offset: int = 0,
        search_query: Optional[str] = None,
        from_ts: Optional[int] = None,
        to_ts: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search messages from specific sender(s) across all chats.

        Args:
            sender_ids: List of sender IDs to filter by
            limit: Maximum number of messages to return
            offset: Number of messages to skip (for pagination)
            search_query: Optional text to search in message content
            from_ts: Filter messages from this timestamp
            to_ts: Filter messages until this timestamp

        Returns:
            List of message records with chat info
        """
        if not sender_ids:
            return []

        conditions: List[str] = []
        params: List[Any] = []

        # Sender filter
        placeholders = ",".join("?" for _ in sender_ids)
        conditions.append(f"m.sender_id IN ({placeholders})")
        params.extend(sender_ids)

        # Text search
        if search_query:
            conditions.append("m.text LIKE ? COLLATE NOCASE")
            params.append(f"%{search_query}%")

        # Date range
        if from_ts is not None:
            conditions.append("m.date >= ?")
            params.append(from_ts)
        if to_ts is not None:
            conditions.append("m.date <= ?")
            params.append(to_ts)

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                m.*,
                c.title as chat_title,
                c.type as chat_type,
                c.username as chat_username
            FROM messages m
            LEFT JOIN chats c ON m.chat_id = c.id
            WHERE {where_clause}
            ORDER BY m.date DESC, m.message_id DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        rows = self.conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def count_messages_by_sender(
        self,
        sender_ids: List[int],
        search_query: Optional[str] = None,
        from_ts: Optional[int] = None,
        to_ts: Optional[int] = None,
    ) -> int:
        """
        Count messages from specific sender(s) - useful for pagination info.
        """
        if not sender_ids:
            return 0

        conditions: List[str] = []
        params: List[Any] = []

        placeholders = ",".join("?" for _ in sender_ids)
        conditions.append(f"sender_id IN ({placeholders})")
        params.extend(sender_ids)

        if search_query:
            conditions.append("text LIKE ? COLLATE NOCASE")
            params.append(f"%{search_query}%")

        if from_ts is not None:
            conditions.append("date >= ?")
            params.append(from_ts)
        if to_ts is not None:
            conditions.append("date <= ?")
            params.append(to_ts)

        where_clause = " AND ".join(conditions)

        row = self.conn.execute(
            f"SELECT COUNT(*) FROM messages WHERE {where_clause}",
            params,
        ).fetchone()
        return row[0] if row else 0

    def get_message(self, chat_id: int, message_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM messages WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id),
        ).fetchone()
        return dict(row) if row else None

    def get_messages_before(
        self, chat_id: int, message_id: int, limit: int
    ) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM messages
            WHERE chat_id = ? AND message_id < ?
            ORDER BY message_id DESC
            LIMIT ?
            """,
            (chat_id, message_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_messages_after(
        self, chat_id: int, message_id: int, limit: int
    ) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM messages
            WHERE chat_id = ? AND message_id > ?
            ORDER BY message_id ASC
            LIMIT ?
            """,
            (chat_id, message_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_last_message(self, chat_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT * FROM messages
            WHERE chat_id = ?
            ORDER BY message_id DESC
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_pinned_messages(
        self, chat_id: int, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        if limit is None:
            rows = self.conn.execute(
                """
                SELECT * FROM messages
                WHERE chat_id = ? AND pinned = 1
                ORDER BY message_id DESC
                """,
                (chat_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT * FROM messages
                WHERE chat_id = ? AND pinned = 1
                ORDER BY message_id DESC
                LIMIT ?
                """,
                (chat_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_recent_with_buttons(self, chat_id: int, limit: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM messages
            WHERE chat_id = ?
            ORDER BY message_id DESC
            LIMIT ?
            """,
            (chat_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_chat_ids(self) -> List[int]:
        rows = self.conn.execute("SELECT id FROM chats").fetchall()
        return [row[0] for row in rows]

    def get_last_message_id(self, chat_id: int) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MAX(message_id) FROM messages WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
        return row[0] if row else None
