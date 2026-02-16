from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .models import DiffRecord
from .text_utils import normalize_text


@dataclass(frozen=True)
class ScanStateSummary:
    local_root: str
    remote_address: str
    local_scan_seconds: float
    remote_scan_seconds: float
    local_files: int
    remote_files: int
    compared_paths: int
    only_local: int
    only_remote: int
    different_content: int
    uncertain: int
    metadata_only: int


@dataclass(frozen=True)
class StateContext:
    local_root: str
    remote_address: str


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS state_meta (
            singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            local_root TEXT NOT NULL,
            remote_address TEXT NOT NULL,
            local_scan_seconds REAL NOT NULL,
            remote_scan_seconds REAL NOT NULL,
            local_files INTEGER NOT NULL,
            remote_files INTEGER NOT NULL,
            compared_paths INTEGER NOT NULL,
            only_local INTEGER NOT NULL,
            only_remote INTEGER NOT NULL,
            different_content INTEGER NOT NULL,
            uncertain INTEGER NOT NULL,
            metadata_only INTEGER NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS current_diffs (
            relpath TEXT PRIMARY KEY,
            content_state TEXT NOT NULL,
            metadata_state TEXT NOT NULL,
            metadata_diff_json TEXT NOT NULL,
            metadata_detail_json TEXT NOT NULL DEFAULT '[]',
            metadata_source TEXT,
            local_size INTEGER,
            remote_size INTEGER
        )
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_current_diffs_content
        ON current_diffs(content_state, metadata_state)
        """
    )

    cols = conn.execute("PRAGMA table_info(current_diffs)").fetchall()
    col_names = {str(col["name"]) for col in cols}
    if "metadata_detail_json" not in col_names:
        conn.execute(
            "ALTER TABLE current_diffs ADD COLUMN metadata_detail_json TEXT NOT NULL DEFAULT '[]'"
        )
    if "metadata_source" not in col_names:
        conn.execute("ALTER TABLE current_diffs ADD COLUMN metadata_source TEXT")
    if "local_size" not in col_names:
        conn.execute("ALTER TABLE current_diffs ADD COLUMN local_size INTEGER")
    if "remote_size" not in col_names:
        conn.execute("ALTER TABLE current_diffs ADD COLUMN remote_size INTEGER")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ui_prefs (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_actions (
            relpath TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    action_cols = conn.execute("PRAGMA table_info(scan_actions)").fetchall()
    action_col_names = {str(col["name"]) for col in action_cols}
    if "run_id" in action_col_names:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scan_actions_new (
                relpath TEXT PRIMARY KEY,
                action TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO scan_actions_new(relpath, action, updated_at)
            SELECT relpath, action, updated_at FROM scan_actions
            """
        )
        conn.execute("DROP TABLE scan_actions")
        conn.execute("ALTER TABLE scan_actions_new RENAME TO scan_actions")


def save_current_state(
    db_path: Path,
    summary: ScanStateSummary,
    diffs: list[DiffRecord],
) -> None:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        with conn:
            conn.execute(
                """
                INSERT INTO state_meta (
                    singleton_id,
                    local_root,
                    remote_address,
                    local_scan_seconds,
                    remote_scan_seconds,
                    local_files,
                    remote_files,
                    compared_paths,
                    only_local,
                    only_remote,
                    different_content,
                    uncertain,
                    metadata_only,
                    updated_at
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(singleton_id) DO UPDATE SET
                    local_root = excluded.local_root,
                    remote_address = excluded.remote_address,
                    local_scan_seconds = excluded.local_scan_seconds,
                    remote_scan_seconds = excluded.remote_scan_seconds,
                    local_files = excluded.local_files,
                    remote_files = excluded.remote_files,
                    compared_paths = excluded.compared_paths,
                    only_local = excluded.only_local,
                    only_remote = excluded.only_remote,
                    different_content = excluded.different_content,
                    uncertain = excluded.uncertain,
                    metadata_only = excluded.metadata_only,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalize_text(summary.local_root),
                    normalize_text(summary.remote_address),
                    summary.local_scan_seconds,
                    summary.remote_scan_seconds,
                    summary.local_files,
                    summary.remote_files,
                    summary.compared_paths,
                    summary.only_local,
                    summary.only_remote,
                    summary.different_content,
                    summary.uncertain,
                    summary.metadata_only,
                ),
            )

            conn.execute("CREATE TEMP TABLE _seen_paths(relpath TEXT PRIMARY KEY)")
            conn.executemany(
                """
                INSERT INTO current_diffs (
                    relpath,
                    content_state,
                    metadata_state,
                    metadata_diff_json,
                    metadata_detail_json,
                    metadata_source,
                    local_size,
                    remote_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(relpath) DO UPDATE SET
                    content_state = excluded.content_state,
                    metadata_state = excluded.metadata_state,
                    metadata_diff_json = excluded.metadata_diff_json,
                    metadata_detail_json = excluded.metadata_detail_json,
                    metadata_source = excluded.metadata_source,
                    local_size = excluded.local_size,
                    remote_size = excluded.remote_size
                """,
                [
                    (
                        normalize_text(diff.relpath),
                        diff.content_state.value,
                        diff.metadata_state.value,
                        json.dumps(list(diff.metadata_diff), ensure_ascii=True),
                        json.dumps(list(diff.metadata_details), ensure_ascii=True),
                        normalize_text(diff.metadata_source)
                        if diff.metadata_source is not None
                        else None,
                        diff.local_size,
                        diff.remote_size,
                    )
                    for diff in diffs
                ],
            )
            conn.executemany(
                "INSERT OR IGNORE INTO _seen_paths(relpath) VALUES (?)",
                ((normalize_text(diff.relpath),) for diff in diffs),
            )
            conn.execute(
                "DELETE FROM current_diffs WHERE relpath NOT IN (SELECT relpath FROM _seen_paths)"
            )
            conn.execute(
                "DELETE FROM scan_actions WHERE relpath NOT IN (SELECT relpath FROM _seen_paths)"
            )
            conn.execute("DROP TABLE _seen_paths")
    finally:
        conn.close()


def get_state_context(db_path: Path) -> StateContext | None:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        row = conn.execute(
            "SELECT local_root, remote_address FROM state_meta WHERE singleton_id = 1"
        ).fetchone()
        if row is None:
            return None
        return StateContext(
            local_root=str(row["local_root"]),
            remote_address=str(row["remote_address"]),
        )
    finally:
        conn.close()


def load_current_diffs(db_path: Path) -> list[dict[str, object]]:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        rows = conn.execute(
            """
            SELECT relpath, content_state, metadata_state, metadata_diff_json, metadata_detail_json, metadata_source, local_size, remote_size
            FROM current_diffs
            ORDER BY relpath
            """
        ).fetchall()
        return [
            {
                "relpath": row["relpath"],
                "content_state": row["content_state"],
                "metadata_state": row["metadata_state"],
                "metadata_diff": json.loads(row["metadata_diff_json"]),
                "metadata_details": json.loads(row["metadata_detail_json"] or "[]"),
                "metadata_source": row["metadata_source"],
                "local_size": row["local_size"],
                "remote_size": row["remote_size"],
            }
            for row in rows
        ]
    finally:
        conn.close()


def get_ui_pref(db_path: Path, key: str, default: str) -> str:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        row = conn.execute(
            "SELECT value FROM ui_prefs WHERE key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return default
        return str(row["value"])
    finally:
        conn.close()


def set_ui_pref(db_path: Path, key: str, value: str) -> None:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        with conn:
            conn.execute(
                """
                INSERT INTO ui_prefs(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
    finally:
        conn.close()


def load_action_overrides(db_path: Path) -> dict[str, str]:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        rows = conn.execute("SELECT relpath, action FROM scan_actions").fetchall()
        return {str(row["relpath"]): str(row["action"]) for row in rows}
    finally:
        conn.close()


def upsert_action_overrides(db_path: Path, updates: dict[str, str]) -> None:
    if not updates:
        return
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        with conn:
            conn.executemany(
                """
                INSERT INTO scan_actions (relpath, action)
                VALUES (?, ?)
                ON CONFLICT(relpath) DO UPDATE SET
                    action = excluded.action,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    (normalize_text(relpath), action)
                    for relpath, action in updates.items()
                ),
            )
    finally:
        conn.close()


def mark_paths_identical(db_path: Path, relpaths: set[str]) -> None:
    if not relpaths:
        return
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        with conn:
            conn.executemany(
                """
                UPDATE current_diffs
                SET
                    content_state = 'identical',
                    metadata_state = 'identical',
                    metadata_diff_json = '[]',
                    metadata_detail_json = '[]'
                WHERE relpath = ?
                """,
                ((normalize_text(relpath),) for relpath in relpaths),
            )
    finally:
        conn.close()


def delete_paths_from_current_state(db_path: Path, relpaths: set[str]) -> None:
    if not relpaths:
        return
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        with conn:
            conn.executemany(
                "DELETE FROM current_diffs WHERE relpath = ?",
                ((normalize_text(relpath),) for relpath in relpaths),
            )
            conn.executemany(
                "DELETE FROM scan_actions WHERE relpath = ?",
                ((normalize_text(relpath),) for relpath in relpaths),
            )
    finally:
        conn.close()


def clear_action_overrides(db_path: Path) -> None:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        with conn:
            conn.execute("DELETE FROM scan_actions")
    finally:
        conn.close()
