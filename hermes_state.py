#!/usr/bin/env python3
"""
SQLite State Store for Hermes Agent.

Provides persistent session storage with FTS5 full-text search, replacing
the per-session JSONL file approach. Stores session metadata, full message
history, and model configuration for CLI and gateway sessions.

Key design decisions:
- WAL mode for concurrent readers + one writer (gateway multi-platform)
- FTS5 virtual table for fast text search across all session messages
- Compression-triggered session splitting via parent_session_id chains
- Batch runner and RL trajectories are NOT stored here (separate systems)
- Session source tagging ('cli', 'telegram', 'discord', etc.) for filtering
"""

import json
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, Any, List, Optional


DEFAULT_DB_PATH = Path(os.getenv("HERMES_HOME", Path.home() / ".hermes")) / "state.db"

SCHEMA_VERSION = 5

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    user_id TEXT,
    model TEXT,
    model_config TEXT,
    system_prompt TEXT,
    parent_session_id TEXT,
    started_at REAL NOT NULL,
    ended_at REAL,
    end_reason TEXT,
    message_count INTEGER DEFAULT 0,
    tool_call_count INTEGER DEFAULT 0,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    cache_read_tokens INTEGER DEFAULT 0,
    cache_write_tokens INTEGER DEFAULT 0,
    reasoning_tokens INTEGER DEFAULT 0,
    billing_provider TEXT,
    billing_base_url TEXT,
    billing_mode TEXT,
    estimated_cost_usd REAL,
    actual_cost_usd REAL,
    cost_status TEXT,
    cost_source TEXT,
    pricing_version TEXT,
    title TEXT,
    FOREIGN KEY (parent_session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    role TEXT NOT NULL,
    content TEXT,
    tool_call_id TEXT,
    tool_calls TEXT,
    tool_name TEXT,
    timestamp REAL NOT NULL,
    token_count INTEGER,
    finish_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_sessions_source ON sessions(source);
CREATE INDEX IF NOT EXISTS idx_sessions_parent ON sessions(parent_session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, timestamp);
"""

FTS_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    content,
    content=messages,
    content_rowid=id
);

CREATE TRIGGER IF NOT EXISTS messages_fts_insert AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, content) VALUES (new.id, new.content);
END;

CREATE TRIGGER IF NOT EXISTS messages_fts_delete AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.id, old.content);
END;

CREATE TRIGGER IF NOT EXISTS messages_fts_update AFTER UPDATE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.id, old.content);
    INSERT INTO messages_fts(rowid, content) VALUES (new.id, new.content);
END;
"""


class SessionDB:
    """
    SQLite-backed session storage with FTS5 search.

    Thread-safe for the common gateway pattern (multiple reader threads,
    single writer via WAL mode). Each method opens its own cursor.
    """

    def __init__(self, db_path: Path = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            timeout=10.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

        self._init_schema()

    def _init_schema(self):
        """Create tables and FTS if they don't exist, run migrations."""
        cursor = self._conn.cursor()

        cursor.executescript(SCHEMA_SQL)

        # Check schema version and run migrations
        cursor.execute("SELECT version FROM schema_version LIMIT 1")
        row = cursor.fetchone()
        if row is None:
            cursor.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
        else:
            current_version = row["version"] if isinstance(row, sqlite3.Row) else row[0]
            if current_version < 2:
                # v2: add finish_reason column to messages
                try:
                    cursor.execute("ALTER TABLE messages ADD COLUMN finish_reason TEXT")
                except sqlite3.OperationalError:
                    pass  # Column already exists
                cursor.execute("UPDATE schema_version SET version = 2")
            if current_version < 3:
                # v3: add title column to sessions
                try:
                    cursor.execute("ALTER TABLE sessions ADD COLUMN title TEXT")
                except sqlite3.OperationalError:
                    pass  # Column already exists
                cursor.execute("UPDATE schema_version SET version = 3")
            if current_version < 4:
                # v4: add unique index on title (NULLs allowed, only non-NULL must be unique)
                try:
                    cursor.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_title_unique "
                        "ON sessions(title) WHERE title IS NOT NULL"
                    )
                except sqlite3.OperationalError:
                    pass  # Index already exists
                cursor.execute("UPDATE schema_version SET version = 4")
            if current_version < 5:
                new_columns = [
                    ("cache_read_tokens", "INTEGER DEFAULT 0"),
                    ("cache_write_tokens", "INTEGER DEFAULT 0"),
                    ("reasoning_tokens", "INTEGER DEFAULT 0"),
                    ("billing_provider", "TEXT"),
                    ("billing_base_url", "TEXT"),
                    ("billing_mode", "TEXT"),
                    ("estimated_cost_usd", "REAL"),
                    ("actual_cost_usd", "REAL"),
                    ("cost_status", "TEXT"),
                    ("cost_source", "TEXT"),
                    ("pricing_version", "TEXT"),
                ]
                for name, column_type in new_columns:
                    try:
                        # name and column_type come from the hardcoded tuple above,
                        # not user input. Double-quote identifier escaping is applied
                        # as defense-in-depth; SQLite DDL cannot be parameterized.
                        safe_name = name.replace('"', '""')
                        cursor.execute(f'ALTER TABLE sessions ADD COLUMN "{safe_name}" {column_type}')
                    except sqlite3.OperationalError:
                        pass
                cursor.execute("UPDATE schema_version SET version = 5")

        # Unique title index — always ensure it exists (safe to run after migrations
        # since the title column is guaranteed to exist at this point)
        try:
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_title_unique "
                "ON sessions(title) WHERE title IS NOT NULL"
            )
        except sqlite3.OperationalError:
            pass  # Index already exists

        # FTS5 setup (separate because CREATE VIRTUAL TABLE can't be in executescript with IF NOT EXISTS reliably)
        try:
            cursor.execute("SELECT * FROM messages_fts LIMIT 0")
        except sqlite3.OperationalError:
            cursor.executescript(FTS_SQL)

        self._conn.commit()

    def close(self):
        """Close the database connection."""
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None

    # =========================================================================
    # Session lifecycle
    # =========================================================================

    def create_session(
        self,
        session_id: str,
        source: str,
        model: str = None,
        model_config: Dict[str, Any] = None,
        system_prompt: str = None,
        user_id: str = None,
        parent_session_id: str = None,
    ) -> str:
        """Create a new session record. Returns the session_id."""
        with self._lock:
            self._conn.execute(
                """INSERT INTO sessions (id, source, user_id, model, model_config,
                   system_prompt, parent_session_id, started_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    source,
                    user_id,
                    model,
                    json.dumps(model_config) if model_config else None,
                    system_prompt,
                    parent_session_id,
                    time.time(),
                ),
            )
            self._conn.commit()
        return session_id

    def end_session(self, session_id: str, end_reason: str) -> None:
        """Mark a session as ended."""
        with self._lock:
            self._conn.execute(
                "UPDATE sessions SET ended_at = ?, end_reason = ? WHERE id = ?",
                (time.time(), end_reason, session_id),
            )
            self._conn.commit()

    def update_system_prompt(self, session_id: str, system_prompt: str) -> None:
        """Store the full assembled system prompt snapshot."""
        with self._lock:
            self._conn.execute(
                "UPDATE sessions SET system_prompt = ? WHERE id = ?",
                (system_prompt, session_id),
            )
            self._conn.commit()

    def update_token_counts(
        self,
        session_id: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        model: str = None,
        cache_read_tokens: int = 0,
        cache_write_tokens: int = 0,
        reasoning_tokens: int = 0,
        estimated_cost_usd: Optional[float] = None,
        actual_cost_usd: Optional[float] = None,
        cost_status: Optional[str] = None,
        cost_source: Optional[str] = None,
        pricing_version: Optional[str] = None,
        billing_provider: Optional[str] = None,
        billing_base_url: Optional[str] = None,
        billing_mode: Optional[str] = None,
    ) -> None:
        """Increment token counters and backfill model if not already set."""
        with self._lock:
            self._conn.execute(
                """UPDATE sessions SET
                   input_tokens = input_tokens + ?,
                   output_tokens = output_tokens + ?,
                   cache_read_tokens = cache_read_tokens + ?,
                   cache_write_tokens = cache_write_tokens + ?,
                   reasoning_tokens = reasoning_tokens + ?,
                   estimated_cost_usd = COALESCE(estimated_cost_usd, 0) + COALESCE(?, 0),
                   actual_cost_usd = CASE
                       WHEN ? IS NULL THEN actual_cost_usd
                       ELSE COALESCE(actual_cost_usd, 0) + ?
                   END,
                   cost_status = COALESCE(?, cost_status),
                   cost_source = COALESCE(?, cost_source),
                   pricing_version = COALESCE(?, pricing_version),
                   billing_provider = COALESCE(billing_provider, ?),
                   billing_base_url = COALESCE(billing_base_url, ?),
                   billing_mode = COALESCE(billing_mode, ?),
                   model = COALESCE(model, ?)
                   WHERE id = ?""",
                (
                    input_tokens,
                    output_tokens,
                    cache_read_tokens,
                    cache_write_tokens,
                    reasoning_tokens,
                    estimated_cost_usd,
                    actual_cost_usd,
                    actual_cost_usd,
                    cost_status,
                    cost_source,
                    pricing_version,
                    billing_provider,
                    billing_base_url,
                    billing_mode,
                    model,
                    session_id,
                ),
            )
            self._conn.commit()

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get a session by ID."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM sessions WHERE id = ?", (session_id,)
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def resolve_session_id(self, session_id_or_prefix: str) -> Optional[str]:
        """Resolve an exact or uniquely prefixed session ID to the full ID.

        Returns the exact ID when it exists. Otherwise treats the input as a
        prefix and returns the single matching session ID if the prefix is
        unambiguous. Returns None for no matches or ambiguous prefixes.
        """
        exact = self.get_session(session_id_or_prefix)
        if exact:
            return exact["id"]

        escaped = (
            session_id_or_prefix
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        with self._lock:
            cursor = self._conn.execute(
                "SELECT id FROM sessions WHERE id LIKE ? ESCAPE '\\' ORDER BY started_at DESC LIMIT 2",
                (f"{escaped}%",),
            )
            matches = [row["id"] for row in cursor.fetchall()]
        if len(matches) == 1:
            return matches[0]
        return None

    # Maximum length for session titles
    MAX_TITLE_LENGTH = 100

    @staticmethod
    def sanitize_title(title: Optional[str]) -> Optional[str]:
        """Validate and sanitize a session title.

        - Strips leading/trailing whitespace
        - Removes ASCII control characters (0x00-0x1F, 0x7F) and problematic
          Unicode control chars (zero-width, RTL/LTR overrides, etc.)
        - Collapses internal whitespace runs to single spaces
        - Normalizes empty/whitespace-only strings to None
        - Enforces MAX_TITLE_LENGTH

        Returns the cleaned title string or None.
        Raises ValueError if the title exceeds MAX_TITLE_LENGTH after cleaning.
        """
        if not title:
            return None

        # Remove ASCII control characters (0x00-0x1F, 0x7F) but keep
        # whitespace chars (\t=0x09, \n=0x0A, \r=0x0D) so they can be
        # normalized to spaces by the whitespace collapsing step below
        cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', title)

        # Remove problematic Unicode control characters:
        # - Zero-width chars (U+200B-U+200F, U+FEFF)
        # - Directional overrides (U+202A-U+202E, U+2066-U+2069)
        # - Object replacement (U+FFFC), interlinear annotation (U+FFF9-U+FFFB)
        cleaned = re.sub(
            r'[\u200b-\u200f\u2028-\u202e\u2060-\u2069\ufeff\ufffc\ufff9-\ufffb]',
            '', cleaned,
        )

        # Collapse internal whitespace runs and strip
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        if not cleaned:
            return None

        if len(cleaned) > SessionDB.MAX_TITLE_LENGTH:
            raise ValueError(
                f"Title too long ({len(cleaned)} chars, max {SessionDB.MAX_TITLE_LENGTH})"
            )

        return cleaned

    def set_session_title(self, session_id: str, title: str) -> bool:
        """Set or update a session's title.

        Returns True if session was found and title was set.
        Raises ValueError if title is already in use by another session,
        or if the title fails validation (too long, invalid characters).
        Empty/whitespace-only strings are normalized to None (clearing the title).
        """
        title = self.sanitize_title(title)
        with self._lock:
            if title:
                # Check uniqueness (allow the same session to keep its own title)
                cursor = self._conn.execute(
                    "SELECT id FROM sessions WHERE title = ? AND id != ?",
                    (title, session_id),
                )
                conflict = cursor.fetchone()
                if conflict:
                    raise ValueError(
                        f"Title '{title}' is already in use by session {conflict['id']}"
                    )
            cursor = self._conn.execute(
                "UPDATE sessions SET title = ? WHERE id = ?",
                (title, session_id),
            )
            self._conn.commit()
            rowcount = cursor.rowcount
        return rowcount > 0

    def get_session_title(self, session_id: str) -> Optional[str]:
        """Get the title for a session, or None."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT title FROM sessions WHERE id = ?", (session_id,)
            )
            row = cursor.fetchone()
        return row["title"] if row else None

    def get_session_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """Look up a session by exact title. Returns session dict or None."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM sessions WHERE title = ?", (title,)
            )
            row = cursor.fetchone()
        return dict(row) if row else None

    def resolve_session_by_title(self, title: str) -> Optional[str]:
        """Resolve a title to a session ID, preferring the latest in a lineage.

        If the exact title exists, returns that session's ID.
        If not, searches for "title #N" variants and returns the latest one.
        If the exact title exists AND numbered variants exist, returns the
        latest numbered variant (the most recent continuation).
        """
        # First try exact match
        exact = self.get_session_by_title(title)

        # Also search for numbered variants: "title #2", "title #3", etc.
        # Escape SQL LIKE wildcards (%, _) in the title to prevent false matches
        escaped = title.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self._lock:
            cursor = self._conn.execute(
                "SELECT id, title, started_at FROM sessions "
                "WHERE title LIKE ? ESCAPE '\\' ORDER BY started_at DESC",
                (f"{escaped} #%",),
            )
            numbered = cursor.fetchall()

        if numbered:
            # Return the most recent numbered variant
            return numbered[0]["id"]
        elif exact:
            return exact["id"]
        return None

    def get_next_title_in_lineage(self, base_title: str) -> str:
        """Generate the next title in a lineage (e.g., "my session" → "my session #2").

        Strips any existing " #N" suffix to find the base name, then finds
        the highest existing number and increments.
        """
        # Strip existing #N suffix to find the true base
        match = re.match(r'^(.*?) #(\d+)$', base_title)
        if match:
            base = match.group(1)
        else:
            base = base_title

        # Find all existing numbered variants
        # Escape SQL LIKE wildcards (%, _) in the base to prevent false matches
        escaped = base.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self._lock:
            cursor = self._conn.execute(
                "SELECT title FROM sessions WHERE title = ? OR title LIKE ? ESCAPE '\\'",
                (base, f"{escaped} #%"),
            )
            existing = [row["title"] for row in cursor.fetchall()]

        if not existing:
            return base  # No conflict, use the base name as-is

        # Find the highest number
        max_num = 1  # The unnumbered original counts as #1
        for t in existing:
            m = re.match(r'^.* #(\d+)$', t)
            if m:
                max_num = max(max_num, int(m.group(1)))

        return f"{base} #{max_num + 1}"

    def list_sessions_rich(
        self,
        source: str = None,
        limit: int = 20,
        offset: int = 0,
        include_tool_errors: bool = True,
        exclude_sources: List[str] = None,
    ) -> List[Dict[str, Any]]:
        """List sessions with preview (first user message) and last active timestamp.

        Returns dicts with keys: id, source, model, title, started_at, ended_at,
        message_count, tool_call_count, input_tokens, output_tokens, preview (first
        60 chars of first user message), last_active (timestamp of last message),
        duration (seconds, computed from started_at/ended_at), and optionally
        tool_errors (count of messages with error finish_reason).

        Uses a single query with correlated subqueries instead of N+2 queries.
        """
        source_clause = "WHERE s.source = ?" if source else ""
        tool_errors_subquery = ""
        if include_tool_errors:
            # Detect tool errors by content patterns — _detect_tool_failure flags
            # results starting with "Error", "Error executing tool", or containing
            # "[error]" / "[exit N]" (N != 0) / "[full]" tags.
            tool_errors_subquery = """,
                COALESCE(
                    (SELECT COUNT(*) FROM messages m3
                     WHERE m3.session_id = s.id AND m3.role = 'tool'
                     AND (
                         m3.content LIKE 'Error executing tool%'
                         OR m3.content LIKE 'Error: %'
                         OR m3.content LIKE '[error]%'
                     )),
                     0
                ) AS _tool_errors"""
        exclude_clause = ""
        exclude_params = []
        if exclude_sources:
            placeholders = ",".join(["?"] * len(exclude_sources))
            if source:
                # Already have a WHERE, add AND
                exclude_clause = f"AND s.source NOT IN ({placeholders})"
            else:
                exclude_clause = f"WHERE s.source NOT IN ({placeholders})"
            exclude_params = list(exclude_sources)
        query = f"""
            SELECT s.*,
                COALESCE(
                    (SELECT SUBSTR(REPLACE(REPLACE(m.content, X'0A', ' '), X'0D', ' '), 1, 63)
                     FROM messages m
                     WHERE m.session_id = s.id AND m.role = 'user' AND m.content IS NOT NULL
                     ORDER BY m.timestamp, m.id LIMIT 1),
                    ''
                ) AS _preview_raw,
                COALESCE(
                    (SELECT MAX(m2.timestamp) FROM messages m2 WHERE m2.session_id = s.id),
                    s.started_at
                ) AS last_active
                {tool_errors_subquery}
            FROM sessions s
            {source_clause} {exclude_clause}
            ORDER BY s.started_at DESC
            LIMIT ? OFFSET ?
        """
        # Build params: source (if any), then exclude_sources, then limit, offset
        if source:
            params = [source] + exclude_params + [limit, offset]
        else:
            params = exclude_params + [limit, offset]
        with self._lock:
            cursor = self._conn.execute(query, params)
            rows = cursor.fetchall()
        sessions = []
        for row in rows:
            s = dict(row)
            # Build the preview from the raw substring
            raw = s.pop("_preview_raw", "").strip()
            if raw:
                text = raw[:60]
                s["preview"] = text + ("..." if len(raw) > 60 else "")
            else:
                s["preview"] = ""
            # Compute duration
            started = s.get("started_at")
            ended = s.get("ended_at")
            if started and ended:
                s["duration"] = ended - started
            elif started:
                s["duration"] = time.time() - started
            else:
                s["duration"] = 0
            # Include tool error count
            if include_tool_errors and "_tool_errors" in s:
                s["tool_errors"] = s.pop("_tool_errors")
            sessions.append(s)

        return sessions

    # =========================================================================
    # Message storage
    # =========================================================================

    def append_message(
        self,
        session_id: str,
        role: str,
        content: str = None,
        tool_name: str = None,
        tool_calls: Any = None,
        tool_call_id: str = None,
        token_count: int = None,
        finish_reason: str = None,
    ) -> int:
        """
        Append a message to a session. Returns the message row ID.

        Also increments the session's message_count (and tool_call_count
        if role is 'tool' or tool_calls is present).
        """
        with self._lock:
            cursor = self._conn.execute(
                """INSERT INTO messages (session_id, role, content, tool_call_id,
                   tool_calls, tool_name, timestamp, token_count, finish_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    role,
                    content,
                    tool_call_id,
                    json.dumps(tool_calls) if tool_calls else None,
                    tool_name,
                    time.time(),
                    token_count,
                    finish_reason,
                ),
            )
            msg_id = cursor.lastrowid

            # Update counters
            # Count actual tool calls from the tool_calls list (not from tool responses).
            # A single assistant message can contain multiple parallel tool calls.
            num_tool_calls = 0
            if tool_calls is not None:
                num_tool_calls = len(tool_calls) if isinstance(tool_calls, list) else 1
            if num_tool_calls > 0:
                self._conn.execute(
                    """UPDATE sessions SET message_count = message_count + 1,
                       tool_call_count = tool_call_count + ? WHERE id = ?""",
                    (num_tool_calls, session_id),
                )
            else:
                self._conn.execute(
                    "UPDATE sessions SET message_count = message_count + 1 WHERE id = ?",
                    (session_id,),
                )

            self._conn.commit()
        return msg_id

    def get_messages(self, session_id: str) -> List[Dict[str, Any]]:
        """Load all messages for a session, ordered by timestamp."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM messages WHERE session_id = ? ORDER BY timestamp, id",
                (session_id,),
            )
            rows = cursor.fetchall()
        result = []
        for row in rows:
            msg = dict(row)
            if msg.get("tool_calls"):
                try:
                    msg["tool_calls"] = json.loads(msg["tool_calls"])
                except (json.JSONDecodeError, TypeError):
                    pass
            result.append(msg)
        return result

    def get_messages_as_conversation(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Load messages in the OpenAI conversation format (role + content dicts).
        Used by the gateway to restore conversation history.
        """
        with self._lock:
            cursor = self._conn.execute(
                "SELECT role, content, tool_call_id, tool_calls, tool_name "
                "FROM messages WHERE session_id = ? ORDER BY timestamp, id",
                (session_id,),
            )
            rows = cursor.fetchall()
        messages = []
        for row in rows:
            msg = {"role": row["role"], "content": row["content"]}
            if row["tool_call_id"]:
                msg["tool_call_id"] = row["tool_call_id"]
            if row["tool_name"]:
                msg["tool_name"] = row["tool_name"]
            if row["tool_calls"]:
                try:
                    msg["tool_calls"] = json.loads(row["tool_calls"])
                except (json.JSONDecodeError, TypeError):
                    pass
            messages.append(msg)
        return messages

    # =========================================================================
    # Search
    # =========================================================================

    @staticmethod
    def _sanitize_fts5_query(query: str) -> str:
        """Sanitize user input for safe use in FTS5 MATCH queries.

        FTS5 has its own query syntax where characters like ``"``, ``(``, ``)``,
        ``+``, ``*``, ``{``, ``}`` and bare boolean operators (``AND``, ``OR``,
        ``NOT``) have special meaning.  Passing raw user input directly to
        MATCH can cause ``sqlite3.OperationalError``.

        Strategy:
        - Preserve properly paired quoted phrases (``"exact phrase"``)
        - Strip unmatched FTS5-special characters that would cause errors
        - Wrap unquoted hyphenated terms in quotes so FTS5 matches them
          as exact phrases instead of splitting on the hyphen
        """
        # Step 1: Extract balanced double-quoted phrases and protect them
        # from further processing via numbered placeholders.
        _quoted_parts: list = []

        def _preserve_quoted(m: re.Match) -> str:
            _quoted_parts.append(m.group(0))
            return f"\x00Q{len(_quoted_parts) - 1}\x00"

        sanitized = re.sub(r'"[^"]*"', _preserve_quoted, query)

        # Step 2: Strip remaining (unmatched) FTS5-special characters
        sanitized = re.sub(r'[+{}()\"^]', " ", sanitized)

        # Step 3: Collapse repeated * (e.g. "***") into a single one,
        # and remove leading * (prefix-only needs at least one char before *)
        sanitized = re.sub(r"\*+", "*", sanitized)
        sanitized = re.sub(r"(^|\s)\*", r"\1", sanitized)

        # Step 4: Remove dangling boolean operators at start/end that would
        # cause syntax errors (e.g. "hello AND" or "OR world")
        sanitized = re.sub(r"(?i)^(AND|OR|NOT)\b\s*", "", sanitized.strip())
        sanitized = re.sub(r"(?i)\s+(AND|OR|NOT)\s*$", "", sanitized.strip())

        # Step 5: Wrap unquoted hyphenated terms (e.g. ``chat-send``) in
        # double quotes.  FTS5's tokenizer splits on hyphens, turning
        # ``chat-send`` into ``chat AND send``.  Quoting preserves the
        # intended phrase match.
        sanitized = re.sub(r"\b(\w+(?:-\w+)+)\b", r'"\1"', sanitized)

        # Step 6: Restore preserved quoted phrases
        for i, quoted in enumerate(_quoted_parts):
            sanitized = sanitized.replace(f"\x00Q{i}\x00", quoted)

        return sanitized.strip()

    def search_messages(
        self,
        query: str,
        source_filter: List[str] = None,
        role_filter: List[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Full-text search across session messages using FTS5.

        Supports FTS5 query syntax:
          - Simple keywords: "docker deployment"
          - Phrases: '"exact phrase"'
          - Boolean: "docker OR kubernetes", "python NOT java"
          - Prefix: "deploy*"

        Returns matching messages with session metadata, content snippet,
        and surrounding context (1 message before and after the match).
        """
        if not query or not query.strip():
            return []

        query = self._sanitize_fts5_query(query)
        if not query:
            return []

        # Build WHERE clauses dynamically
        where_clauses = ["messages_fts MATCH ?"]
        params: list = [query]

        if source_filter is not None:
            source_placeholders = ",".join("?" for _ in source_filter)
            where_clauses.append(f"s.source IN ({source_placeholders})")
            params.extend(source_filter)

        if role_filter:
            role_placeholders = ",".join("?" for _ in role_filter)
            where_clauses.append(f"m.role IN ({role_placeholders})")
            params.extend(role_filter)

        where_sql = " AND ".join(where_clauses)
        params.extend([limit, offset])

        sql = f"""
            SELECT
                m.id,
                m.session_id,
                m.role,
                snippet(messages_fts, 0, '>>>', '<<<', '...', 40) AS snippet,
                m.content,
                m.timestamp,
                m.tool_name,
                s.source,
                s.model,
                s.started_at AS session_started
            FROM messages_fts
            JOIN messages m ON m.id = messages_fts.rowid
            JOIN sessions s ON s.id = m.session_id
            WHERE {where_sql}
            ORDER BY rank
            LIMIT ? OFFSET ?
        """

        with self._lock:
            try:
                cursor = self._conn.execute(sql, params)
            except sqlite3.OperationalError:
                # FTS5 query syntax error despite sanitization — return empty
                return []
            matches = [dict(row) for row in cursor.fetchall()]

            # Add surrounding context (1 message before + after each match)
            for match in matches:
                try:
                    ctx_cursor = self._conn.execute(
                        """SELECT role, content FROM messages
                           WHERE session_id = ? AND id >= ? - 1 AND id <= ? + 1
                           ORDER BY id""",
                        (match["session_id"], match["id"], match["id"]),
                    )
                    context_msgs = [
                        {"role": r["role"], "content": (r["content"] or "")[:200]}
                        for r in ctx_cursor.fetchall()
                    ]
                    match["context"] = context_msgs
                except Exception:
                    match["context"] = []

        # Remove full content from result (snippet is enough, saves tokens)
        for match in matches:
            match.pop("content", None)

        return matches

    def search_sessions(
        self,
        source: str = None,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List sessions, optionally filtered by source."""
        with self._lock:
            if source:
                cursor = self._conn.execute(
                    "SELECT * FROM sessions WHERE source = ? ORDER BY started_at DESC LIMIT ? OFFSET ?",
                    (source, limit, offset),
                )
            else:
                cursor = self._conn.execute(
                    "SELECT * FROM sessions ORDER BY started_at DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                )
            return [dict(row) for row in cursor.fetchall()]

    # =========================================================================
    # Utility
    # =========================================================================

    def session_count(self, source: str = None) -> int:
        """Count sessions, optionally filtered by source."""
        with self._lock:
            if source:
                cursor = self._conn.execute(
                    "SELECT COUNT(*) FROM sessions WHERE source = ?", (source,)
                )
            else:
                cursor = self._conn.execute("SELECT COUNT(*) FROM sessions")
            return cursor.fetchone()[0]

    def message_count(self, session_id: str = None) -> int:
        """Count messages, optionally for a specific session."""
        with self._lock:
            if session_id:
                cursor = self._conn.execute(
                    "SELECT COUNT(*) FROM messages WHERE session_id = ?", (session_id,)
                )
            else:
                cursor = self._conn.execute("SELECT COUNT(*) FROM messages")
            return cursor.fetchone()[0]

    # =========================================================================
    # Export and cleanup
    # =========================================================================

    def export_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Export a single session with all its messages as a dict."""
        session = self.get_session(session_id)
        if not session:
            return None
        messages = self.get_messages(session_id)
        return {**session, "messages": messages}

    def export_all(self, source: str = None) -> List[Dict[str, Any]]:
        """
        Export all sessions (with messages) as a list of dicts.
        Suitable for writing to a JSONL file for backup/analysis.
        """
        sessions = self.search_sessions(source=source, limit=100000)
        results = []
        for session in sessions:
            messages = self.get_messages(session["id"])
            results.append({**session, "messages": messages})
        return results

    def clear_messages(self, session_id: str) -> None:
        """Delete all messages for a session and reset its counters."""
        with self._lock:
            self._conn.execute(
                "DELETE FROM messages WHERE session_id = ?", (session_id,)
            )
            self._conn.execute(
                "UPDATE sessions SET message_count = 0, tool_call_count = 0 WHERE id = ?",
                (session_id,),
            )
            self._conn.commit()

    def delete_session(self, session_id: str) -> bool:
        """Delete a session and all its messages. Returns True if found."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE id = ?", (session_id,)
            )
            if cursor.fetchone()[0] == 0:
                return False
            self._conn.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
            self._conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
            self._conn.commit()
            return True

    def prune_sessions(self, older_than_days: int = 90, source: str = None) -> int:
        """
        Delete sessions older than N days. Returns count of deleted sessions.
        Only prunes ended sessions (not active ones).
        """
        import time as _time
        cutoff = _time.time() - (older_than_days * 86400)

        with self._lock:
            if source:
                cursor = self._conn.execute(
                    """SELECT id FROM sessions
                       WHERE started_at < ? AND ended_at IS NOT NULL AND source = ?""",
                    (cutoff, source),
                )
            else:
                cursor = self._conn.execute(
                    "SELECT id FROM sessions WHERE started_at < ? AND ended_at IS NOT NULL",
                    (cutoff,),
                )
            session_ids = [row["id"] for row in cursor.fetchall()]

            for sid in session_ids:
                self._conn.execute("DELETE FROM messages WHERE session_id = ?", (sid,))
                self._conn.execute("DELETE FROM sessions WHERE id = ?", (sid,))

            self._conn.commit()
        return len(session_ids)

    # =========================================================================
    # User Prompt Database (B6)
    # =========================================================================

    @staticmethod
    def _expand_pastes_in_text(text: str, pastes_dir: str = "~/.hermes/pastes") -> str:
        """Resolve [Pasted text #N: N lines -> /path/to/file] references inline.

        Returns the text with each paste reference replaced by its file contents,
        prefixed with the filename so the receiving agent knows the source.
        """
        import os
        import textwrap

        resolved_dir = os.path.expanduser(pastes_dir)
        pattern = r"\[Pasted text #\\d+: \d+ lines \xe2\x86\x92 (.+?)\]"

        def _replacer(m: re.Match) -> str:
            file_path = m.group(1).strip()
            if os.path.exists(file_path):
                try:
                    file_content = Path(file_path).read_text(encoding="utf-8")
                    fname = os.path.basename(file_path)
                    return f"[--- Paste from {fname} ({file_path}) ---]\n{file_content}\n[--- End paste ---]"
                except Exception:
                    return f"[Paste file not readable: {file_path}]"
            elif os.path.exists(resolved_dir) and os.path.exists(os.path.join(resolved_dir, os.path.basename(file_path))):
                # Try relative to pastes_dir
                full = os.path.join(resolved_dir, os.path.basename(file_path))
                try:
                    file_content = Path(full).read_text(encoding="utf-8")
                    fname = os.path.basename(full)
                    return f"[--- Paste from {fname} ({full}) ---]\n{file_content}\n[--- End paste ---]"
                except Exception:
                    return f"[Paste file not readable: {full}]"
            else:
                return f"[Paste file not found: {file_path}]"

        return re.sub(pattern, _replacer, text)

    def get_user_prompts(
        self,
        source_filter: str = None,
        session_filter: str = None,
        date_from: float = None,
        date_to: float = None,
        limit: int = 100,
        offset: int = 0,
        expand_pastes: bool = True,
        pastes_dir: str = "~/.hermes/pastes",
    ) -> List[Dict[str, Any]]:
        """Query all user prompts with full session metadata.

        Each returned dict includes:
        - prompt_id: message id
        - session_id: session id
        - session_title: title from sessions table
        - session_source: cli, telegram, discord, etc.
        - model: model used for this session
        - prompt: the raw user message content
        - prompt_tokens: token count for this message
        - prompt_timestamp: when the message was sent
        - session_started_at: when the session started
        - session_ended_at: when the session ended
        - session_duration_s: duration in seconds (if ended)
        - session_input_tokens: total input tokens for session
        - session_output_tokens: total output tokens for session
        - session_tool_calls: total tool call count
        - session_estimated_cost: cost estimate
        - prompt_expanded: the content with paste references resolved

        Args:
            source_filter: filter by session source (cli, telegram, etc.)
            session_filter: filter by session_id
            date_from: unix timestamp, prompts after this time
            date_to: unix timestamp, prompts before this time
            limit: max results
            offset: skip N results
            expand_pastes: if True, resolve paste refs inline in prompt_expanded
        """
        query = """
            SELECT
                m.id as prompt_id,
                m.session_id,
                s.title as session_title,
                s.source as session_source,
                s.model,
                s.started_at as session_started_at,
                s.ended_at as session_ended_at,
                CASE WHEN s.ended_at IS NOT NULL
                     THEN CAST(s.ended_at - s.started_at AS INTEGER)
                     ELSE NULL END as session_duration_s,
                s.input_tokens as session_input_tokens,
                s.output_tokens as session_output_tokens,
                s.tool_call_count as session_tool_calls,
                s.estimated_cost_usd as session_estimated_cost,
                s.billing_provider,
                m.content as prompt_raw,
                m.token_count as prompt_tokens,
                m.timestamp as prompt_timestamp
            FROM messages m
            JOIN sessions s ON m.session_id = s.id
            WHERE m.role = 'user'
        """
        params = []

        if source_filter:
            query += " AND s.source = ?"
            params.append(source_filter)
        if session_filter:
            query += " AND m.session_id = ?"
            params.append(session_filter)
        if date_from:
            query += " AND m.timestamp >= ?"
            params.append(date_from)
        if date_to:
            query += " AND m.timestamp <= ?"
            params.append(date_to)

        query += " ORDER BY m.timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._lock:
            cursor = self._conn.execute(query, params)
            rows = cursor.fetchall()

        results = []
        for row in rows:
            d = dict(row)
            d["prompt"] = d.pop("prompt_raw")
            if expand_pastes and d["prompt"]:
                d["prompt_expanded"] = self._expand_pastes_in_text(
                    d["prompt"], pastes_dir
                )
            else:
                d["prompt_expanded"] = d["prompt"]
            results.append(d)

        return results

    def get_user_prompts_count(
        self,
        source_filter: str = None,
        date_from: float = None,
        date_to: float = None,
    ) -> int:
        """Count total user prompts matching filters."""
        query = """
            SELECT COUNT(*) FROM messages m
            JOIN sessions s ON m.session_id = s.id
            WHERE m.role = 'user'
        """
        params = []
        if source_filter:
            query += " AND s.source = ?"
            params.append(source_filter)
        if date_from:
            query += " AND m.timestamp >= ?"
            params.append(date_from)
        if date_to:
            query += " AND m.timestamp <= ?"
            params.append(date_to)

        with self._lock:
            cursor = self._conn.execute(query, params)
            return cursor.fetchone()[0]

    def search_user_prompts(
        self,
        query: str,
        source_filter: str = None,
        limit: int = 20,
        offset: int = 0,
        expand_pastes: bool = True,
    ) -> List[Dict[str, Any]]:
        """Full-text search user prompts using FTS5 with metadata."""
        if not query or not query.strip():
            return []

        safe_query = self._sanitize_fts5_query(query)
        if not safe_query:
            return []
        # Wrap in double-quotes for safe FTS5 matching of special chars
        # This treats the entire query as an exact phrase match unless it contains operators
        if not (safe_query.startswith('"') and safe_query.endswith('"')):
            if ' AND ' not in safe_query and ' OR ' not in safe_query:
                safe_query = '"' + safe_query.replace('"', '""') + '"'

        sql = """
            SELECT
                m.id as prompt_id,
                m.session_id,
                s.title as session_title,
                s.source as session_source,
                s.model,
                s.started_at as session_started_at,
                s.ended_at as session_ended_at,
                CASE WHEN s.ended_at IS NOT NULL
                     THEN CAST(s.ended_at - s.started_at AS INTEGER)
                     ELSE NULL END as session_duration_s,
                s.input_tokens as session_input_tokens,
                s.output_tokens as session_output_tokens,
                s.tool_call_count as session_tool_calls,
                s.estimated_cost_usd as session_estimated_cost,
                s.billing_provider,
                m.content as prompt_raw,
                m.token_count as prompt_tokens,
                m.timestamp as prompt_timestamp,
                fts.rank as fts_rank
            FROM messages m
            JOIN sessions s ON m.session_id = s.id
            JOIN messages_fts fts ON fts.rowid = m.id
            WHERE m.role = 'user' AND messages_fts MATCH ?
            ORDER BY fts.rank
        """
        params = [safe_query]
        if source_filter:
            sql += " AND s.source = ?"
            params.append(source_filter)

        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._lock:
            cursor = self._conn.execute(sql, params)
            rows = cursor.fetchall()

        results = []
        for row in rows:
            d = dict(row)
            d["prompt"] = d.pop("prompt_raw")
            if expand_pastes and d["prompt"]:
                d["prompt_expanded"] = self._expand_pastes_in_text(d["prompt"])
            else:
                d["prompt_expanded"] = d["prompt"]
            results.append(d)

        return results



# =========================================================================
# Cross-Tool Prompt Parsers (B6 Phase 1 — Option C: Query-on-Demand)
# =========================================================================
# Query wrappers that parse external CLI tool session logs on-the-fly.
# No data import, no duplication — normalized format with tool metadata.
# =========================================================================

class ClaudeCodeParser:
    """Parse Claude Code JSONL session logs from ~/.claude/projects/."""

    def __init__(self, projects_dir: str = "~/.claude/projects"):
        self.projects_dir = Path(projects_dir).expanduser()
        self._tool = "claude-code"

    def _iter_sessions(self):
        """Yield (session_path, project_name) for all Claude Code sessions."""
        if not self.projects_dir.exists():
            return
        for project in self.projects_dir.iterdir():
            if project.is_dir():
                for session_file in project.glob("*.jsonl"):
                    yield session_file, project.name

    def get_user_prompts(
        self,
        project_filter: str = None,
        query: str = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Extract user prompts from Claude Code sessions.

        Returns normalized format compatible with SessionDB.get_user_prompts().
        Keys: prompt_id, source_tool, session_source, session_title, model,
        prompt, prompt_expanded, prompt_timestamp, tool_calls_estimate, etc.
        """
        results = []
        count = 0
        for session_path, project_name in self._iter_sessions():
            if project_filter and project_filter.lower() not in project_name.lower():
                continue
            try:
                with open(session_path, "r", encoding="utf-8") as f:
                    session_lines = f.readlines()
                session_ts = session_path.stat().st_mtime
                session_started_at = session_ts
                has_user_prompt = False
                # We cannot reliably reconstruct token counts from Claude Code JSONL
                # (they may be in metadata, but we'd need to check)
                for line in session_lines:
                    try:
                        data = json.loads(line)
                        msg = data.get("message", {})
                        if msg.get("role") == "user":
                            content = msg.get("content", "")
                            # Skip tool results (they're lists, not strings)
                            if isinstance(content, str) and not content.startswith("["):
                                has_user_prompt = True
                                count += 1
                                if count > offset and len(results) < limit:
                                    results.append(
                                        {
                                            "prompt_id": None,
                                            "source_tool": self._tool,
                                            "session_source": f"claude-{project_name}",
                                            "session_title": f"{project_name}",
                                            "model": "claude-code (unknown)",
                                            "prompt": content,
                                            "prompt_expanded": content,  # No pastes in this context
                                            "prompt_timestamp": session_ts,  # approximate per-session
                                            "session_started_at": session_started_at,
                                            "session_ended_at": None,
                                            "session_duration_s": None,
                                            "session_input_tokens": None,
                                            "session_output_tokens": None,
                                            "session_tool_calls": None,
                                            "session_estimated_cost": None,
                                            "billing_provider": None,
                                        }
                                    )
                    except json.JSONDecodeError:
                        continue
                # If this session had no user prompts at all, we can ignore it
            except Exception:
                continue
        return results

    def count_user_prompts(self, project_filter: str = None) -> int:
        count = 0
        for session_path, project_name in self._iter_sessions():
            if project_filter and project_filter.lower() not in project_name.lower():
                continue
            try:
                with open(session_path, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            if data.get("message", {}).get("role") == "user":
                                content = data["message"].get("content", "")
                                if isinstance(content, str) and not content.startswith("["):
                                    count += 1
                        except:
                            continue
            except:
                continue
        return count


class CodexParser:
    """Parse Codex CLI session logs from ~/.codex/sessions/."""

    def __init__(self, sessions_dir: str = "~/.codex/sessions"):
        self.sessions_dir = Path(sessions_dir).expanduser()
        self._tool = "codex"

    def _iter_sessions(self):
        """Yield session files from the dated hierarchy."""
        if not self.sessions_dir.exists():
            return
        for session_file in self.sessions_dir.rglob("*.jsonl"):
            yield session_file

    def get_user_prompts(
        self,
        query: str = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Extract user prompts from Codex sessions."""
        results = []
        count = 0
        for session_path in self._iter_sessions():
            try:
                with open(session_path, "r", encoding="utf-8") as f:
                    session_lines = f.readlines()
                session_ts = session_path.stat().st_mtime
                has_user_prompt = False
                for line in session_lines:
                    try:
                        data = json.loads(line)
                        msg = data.get("message", {})
                        if msg.get("role") == "user":
                            content = msg.get("content", "")
                            if isinstance(content, str) and not content.startswith("["):
                                has_user_prompt = True
                                count += 1
                                if count > offset and len(results) < limit:
                                    results.append(
                                        {
                                            "prompt_id": None,
                                            "source_tool": self._tool,
                                            "session_source": f"codex-{session_path.parent.name}",
                                            "session_title": session_path.name,
                                            "model": "codex (unknown)",
                                            "prompt": content,
                                            "prompt_expanded": content,
                                            "prompt_timestamp": session_ts,
                                            "session_started_at": session_ts,
                                            "session_ended_at": None,
                                            "session_duration_s": None,
                                            "session_input_tokens": None,
                                            "session_output_tokens": None,
                                            "session_tool_calls": None,
                                            "session_estimated_cost": None,
                                            "billing_provider": None,
                                        }
                                    )
                            elif isinstance(content, list):
                                # multimodal: may contain text parts
                                for part in content:
                                    if isinstance(part, dict) and part.get("type") == "text":
                                        count += 1
                                        if count > offset and len(results) < limit:
                                            results.append(
                                                {
                                                    "prompt_id": None,
                                                    "source_tool": self._tool,
                                                    "session_source": f"codex-{session_path.parent.name}",
                                                    "session_title": session_path.name,
                                                    "model": "codex (unknown)",
                                                    "prompt": part.get("text", ""),
                                                    "prompt_expanded": part.get("text", ""),
                                                    "prompt_timestamp": session_ts,
                                                    "session_started_at": session_ts,
                                                    "session_ended_at": None,
                                                    "session_duration_s": None,
                                                    "session_input_tokens": None,
                                                    "session_output_tokens": None,
                                                    "session_tool_calls": None,
                                                    "session_estimated_cost": None,
                                                    "billing_provider": None,
                                                }
                                            )
                                        break
                    except json.JSONDecodeError:
                        continue
            except Exception:
                continue
        return results

    def count_user_prompts(self) -> int:
        count = 0
        for session_path in self._iter_sessions():
            try:
                with open(session_path, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            if data.get("message", {}).get("role") == "user":
                                count += 1
                        except:
                            continue
            except:
                continue
        return count


class QwenParser:
    """Parse Qwen Code chat logs from ~/.qwen/projects/*/chats/*.jsonl."""

    def __init__(self, projects_dir: str = "~/.qwen/projects"):
        self.projects_dir = Path(projects_dir).expanduser()
        self._tool = "qwen-code"

    def _iter_sessions(self):
        """Yield (session_path, project_name) for all Qwen Code sessions."""
        if not self.projects_dir.exists():
            return
        for project in self.projects_dir.iterdir():
            if project.is_dir():
                chats_dir = project / "chats"
                if chats_dir.exists():
                    for session_file in chats_dir.glob("*.jsonl"):
                        yield session_file, project.name

    def get_user_prompts(
        self,
        project_filter: str = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Extract user prompts from Qwen Code sessions."""
        results = []
        count = 0
        for session_path, project_name in self._iter_sessions():
            if project_filter and project_filter.lower() not in project_name.lower():
                continue
            try:
                with open(session_path, "r", encoding="utf-8") as f:
                    session_lines = f.readlines()
                session_ts = session_path.stat().st_mtime
                for line in session_lines:
                    try:
                        data = json.loads(line)
                        if data.get("type") == "user":
                            msg = data.get("message", {})
                            parts = msg.get("parts", [])
                            for part in parts:
                                if isinstance(part, dict) and part.get("type") == "text":
                                    content = part.get("text", "")
                                    count += 1
                                    if count > offset and len(results) < limit:
                                        results.append(
                                            {
                                                "prompt_id": None,
                                                "source_tool": self._tool,
                                                "session_source": f"qwen-{project_name}",
                                                "session_title": session_path.name,
                                                "model": "qwen-code (unknown)",
                                                "prompt": content,
                                                "prompt_expanded": content,
                                                "prompt_timestamp": session_ts,
                                                "session_started_at": session_ts,
                                                "session_ended_at": None,
                                                "session_duration_s": None,
                                                "session_input_tokens": None,
                                                "session_output_tokens": None,
                                                "session_tool_calls": None,
                                                "session_estimated_cost": None,
                                                "billing_provider": None,
                                            }
                                        )
                                    break  # Only count each user message once
                    except json.JSONDecodeError:
                        continue
            except Exception:
                continue
        return results

    def count_user_prompts(self, project_filter: str = None) -> int:
        count = 0
        for session_path, project_name in self._iter_sessions():
            if project_filter and project_filter.lower() not in project_name.lower():
                continue
            try:
                with open(session_path, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            if data.get("type") == "user":
                                count += 1
                        except:
                            continue
            except:
                continue
        return count


class CrossToolPrompts:
    """Unified query interface for all supported CLI tools."""

    def __init__(self):
        self.claude_parser = ClaudeCodeParser()
        self.codex_parser = CodexParser()
        self.qwen_parser = QwenParser()

    def get_all_user_prompts(
        self,
        sources: List[str] = None,
        tools: List[str] = None,
        query: str = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Aggregate prompts from Hermes DB + external tools.

        Args:
            sources: List of source tools to include (e.g., ['hermes', 'claude', 'codex', 'qwen'])
            query: Full-text search (only applies to Hermes DB for now; future: tool FTS)
            limit: Max total results
            offset: Pagination offset

        Returns unified list with same schema as SessionDB.get_user_prompts().
        """
        all_results = []
        remaining = limit

        # 1. Hermes DB if requested
        if sources is None or "hermes" in sources:
            db = SessionDB(Path("~/.hermes/state.db").expanduser())
            # get_user_prompts already handles FTS search if query is provided
            if tools is None or "hermes" in tools:
                results = db.get_user_prompts(
                    source_filter=None,
                    limit=remaining,
                    offset=offset,
                    expand_pastes=False,
                )
                all_results.extend(results)
                remaining = limit - len(all_results)
                if remaining <= 0:
                    return all_results[:limit]

        # 2. Claude Code
        if (sources is None or "claude" in sources) and (tools is None or "claude-code" in tools):
            results = self.claude_parser.get_user_prompts(limit=remaining)
            all_results.extend(results)
            remaining = limit - len(all_results)
            if remaining <= 0:
                return all_results[:limit]

        # 3. Codex
        if (sources is None or "codex" in sources) and (tools is None or "codex" in tools):
            results = self.codex_parser.get_user_prompts(limit=remaining)
            all_results.extend(results)
            remaining = limit - len(all_results)
            if remaining <= 0:
                return all_results[:limit]

        # 4. Qwen Code
        if (sources is None or "qwen" in sources) and (tools is None or "qwen-code" in tools):
            results = self.qwen_parser.get_user_prompts(limit=remaining)
            all_results.extend(results)
            remaining = limit - len(all_results)
            if remaining <= 0:
                return all_results[:limit]

        # Note: full-text search across tools would require a separate indexing pass
        # For now, if query is provided, we filter locally on the combined results
        if query:
            q_lower = query.lower()
            all_results = [
                r for r in all_results if q_lower in r.get("prompt", "").lower() or q_lower in r.get("session_title", "").lower()
            ]

        return all_results[:limit]

    def count_all(self, tools: List[str] = None) -> Dict[str, int]:
        """Return counts for each tool and total."""
        counts = {}
        # Hermes
        db = SessionDB(Path("~/.hermes/state.db").expanduser())
        hermes_total = db.get_user_prompts_count()
        counts["hermes"] = hermes_total
        # Claude Code
        counts["claude-code"] = self.claude_parser.count_user_prompts()
        # Codex
        counts["codex"] = self.codex_parser.count_user_prompts()
        # Qwen Code
        counts["qwen-code"] = self.qwen_parser.count_user_prompts()
        return counts

