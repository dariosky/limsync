from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .models import DiffRecord
from .text_utils import normalize_text


@dataclass(frozen=True)
class ScanStateSummary:
    source_endpoint: str
    destination_endpoint: str
    source_scan_seconds: float
    destination_scan_seconds: float
    source_files: int
    destination_files: int
    compared_paths: int
    only_source: int
    only_destination: int
    different_content: int
    uncertain: int
    metadata_only: int


@dataclass(frozen=True)
class StateContext:
    source_endpoint: str
    destination_endpoint: str


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
            source_endpoint TEXT,
            destination_endpoint TEXT,
            local_scan_seconds REAL NOT NULL,
            remote_scan_seconds REAL NOT NULL,
            local_files INTEGER NOT NULL,
            remote_files INTEGER NOT NULL,
            compared_paths INTEGER NOT NULL,
            only_left INTEGER NOT NULL,
            only_right INTEGER NOT NULL,
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
            left_size INTEGER,
            right_size INTEGER
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
    if "left_size" not in col_names:
        conn.execute("ALTER TABLE current_diffs ADD COLUMN left_size INTEGER")
    if "right_size" not in col_names:
        conn.execute("ALTER TABLE current_diffs ADD COLUMN right_size INTEGER")

    meta_cols = conn.execute("PRAGMA table_info(state_meta)").fetchall()
    meta_col_names = {str(col["name"]) for col in meta_cols}
    if "source_endpoint" not in meta_col_names:
        conn.execute("ALTER TABLE state_meta ADD COLUMN source_endpoint TEXT")
    if "destination_endpoint" not in meta_col_names:
        conn.execute("ALTER TABLE state_meta ADD COLUMN destination_endpoint TEXT")
    if "only_left" not in meta_col_names:
        conn.execute("ALTER TABLE state_meta ADD COLUMN only_left INTEGER")
        if "only_local" in meta_col_names:
            conn.execute(
                "UPDATE state_meta SET only_left = only_local WHERE only_left IS NULL"
            )
    if "only_right" not in meta_col_names:
        conn.execute("ALTER TABLE state_meta ADD COLUMN only_right INTEGER")
        if "only_remote" in meta_col_names:
            conn.execute(
                "UPDATE state_meta SET only_right = only_remote WHERE only_right IS NULL"
            )

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
                    source_endpoint,
                    destination_endpoint,
                    local_scan_seconds,
                    remote_scan_seconds,
                    local_files,
                    remote_files,
                    compared_paths,
                    only_left,
                    only_right,
                    different_content,
                    uncertain,
                    metadata_only,
                    updated_at
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(singleton_id) DO UPDATE SET
                    local_root = excluded.local_root,
                    remote_address = excluded.remote_address,
                    source_endpoint = excluded.source_endpoint,
                    destination_endpoint = excluded.destination_endpoint,
                    local_scan_seconds = excluded.local_scan_seconds,
                    remote_scan_seconds = excluded.remote_scan_seconds,
                    local_files = excluded.local_files,
                    remote_files = excluded.remote_files,
                    compared_paths = excluded.compared_paths,
                    only_left = excluded.only_left,
                    only_right = excluded.only_right,
                    different_content = excluded.different_content,
                    uncertain = excluded.uncertain,
                    metadata_only = excluded.metadata_only,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    normalize_text(summary.source_endpoint),
                    normalize_text(summary.destination_endpoint),
                    normalize_text(summary.source_endpoint),
                    normalize_text(summary.destination_endpoint),
                    summary.source_scan_seconds,
                    summary.destination_scan_seconds,
                    summary.source_files,
                    summary.destination_files,
                    summary.compared_paths,
                    summary.only_source,
                    summary.only_destination,
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
                    left_size,
                    right_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(relpath) DO UPDATE SET
                    content_state = excluded.content_state,
                    metadata_state = excluded.metadata_state,
                    metadata_diff_json = excluded.metadata_diff_json,
                    metadata_detail_json = excluded.metadata_detail_json,
                    metadata_source = excluded.metadata_source,
                    left_size = excluded.left_size,
                    right_size = excluded.right_size
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
                        diff.left_size,
                        diff.right_size,
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
            """
            SELECT
                COALESCE(source_endpoint, local_root) AS source_endpoint,
                COALESCE(destination_endpoint, remote_address) AS destination_endpoint
            FROM state_meta
            WHERE singleton_id = 1
            """
        ).fetchone()
        if row is None:
            return None
        return StateContext(
            source_endpoint=str(row["source_endpoint"]),
            destination_endpoint=str(row["destination_endpoint"]),
        )
    finally:
        conn.close()


def load_current_diffs(db_path: Path) -> list[dict[str, object]]:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        cols = conn.execute("PRAGMA table_info(current_diffs)").fetchall()
        col_names = {str(col["name"]) for col in cols}
        left_expr = "left_size"
        right_expr = "right_size"
        if "local_size" in col_names:
            left_expr = "COALESCE(left_size, local_size) AS left_size"
        if "remote_size" in col_names:
            right_expr = "COALESCE(right_size, remote_size) AS right_size"
        rows = conn.execute(
            "SELECT relpath, content_state, metadata_state, metadata_diff_json, "
            "metadata_detail_json, metadata_source, "
            f"{left_expr}, {right_expr} "
            "FROM current_diffs ORDER BY relpath"
        ).fetchall()
        return [
            {
                "relpath": row["relpath"],
                "content_state": row["content_state"],
                "metadata_state": row["metadata_state"],
                "metadata_diff": json.loads(row["metadata_diff_json"]),
                "metadata_details": json.loads(row["metadata_detail_json"] or "[]"),
                "metadata_source": row["metadata_source"],
                "left_size": row["left_size"],
                "right_size": row["right_size"],
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


def replace_diffs_in_scope(
    db_path: Path,
    diffs: list[DiffRecord],
    *,
    scope_relpath: str,
    scope_is_dir: bool,
) -> None:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        with conn:
            if scope_is_dir:
                scope_like = f"{normalize_text(scope_relpath).rstrip('/')}/%"
                conn.execute(
                    "DELETE FROM current_diffs WHERE relpath = ? OR relpath LIKE ?",
                    (normalize_text(scope_relpath), scope_like),
                )
                conn.execute(
                    "DELETE FROM scan_actions WHERE relpath = ? OR relpath LIKE ?",
                    (normalize_text(scope_relpath), scope_like),
                )
            else:
                conn.execute(
                    "DELETE FROM current_diffs WHERE relpath = ?",
                    (normalize_text(scope_relpath),),
                )
                conn.execute(
                    "DELETE FROM scan_actions WHERE relpath = ?",
                    (normalize_text(scope_relpath),),
                )

            conn.executemany(
                """
                INSERT INTO current_diffs (
                    relpath,
                    content_state,
                    metadata_state,
                    metadata_diff_json,
                    metadata_detail_json,
                    metadata_source,
                    left_size,
                    right_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(relpath) DO UPDATE SET
                    content_state = excluded.content_state,
                    metadata_state = excluded.metadata_state,
                    metadata_diff_json = excluded.metadata_diff_json,
                    metadata_detail_json = excluded.metadata_detail_json,
                    metadata_source = excluded.metadata_source,
                    left_size = excluded.left_size,
                    right_size = excluded.right_size
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
                        diff.left_size,
                        diff.right_size,
                    )
                    for diff in diffs
                ],
            )
    finally:
        conn.close()
