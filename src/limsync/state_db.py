from __future__ import annotations

import json
import sqlite3
import tomllib
from dataclasses import dataclass
from functools import lru_cache
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


@lru_cache(maxsize=1)
def _project_version() -> str:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    version = str(data["project"]["version"]).strip()
    if not version:
        raise RuntimeError("project.version in pyproject.toml is empty")
    return version


def _drop_all_user_objects(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT type, name
        FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%'
        ORDER BY CASE type WHEN 'view' THEN 0 WHEN 'trigger' THEN 1 WHEN 'index' THEN 2 WHEN 'table' THEN 3 ELSE 4 END
        """
    ).fetchall()
    for row in rows:
        obj_type = str(row["type"])
        name = str(row["name"])
        quoted = f'"{name}"'
        if obj_type == "table":
            conn.execute(f"DROP TABLE IF EXISTS {quoted}")
        elif obj_type == "index":
            conn.execute(f"DROP INDEX IF EXISTS {quoted}")
        elif obj_type == "trigger":
            conn.execute(f"DROP TRIGGER IF EXISTS {quoted}")
        elif obj_type == "view":
            conn.execute(f"DROP VIEW IF EXISTS {quoted}")


def _ensure_versioned_db(conn: sqlite3.Connection) -> None:
    expected_version = _project_version()
    try:
        row = conn.execute(
            """
            SELECT value
            FROM limsync
            WHERE key = 'version'
            """
        ).fetchone()
    except sqlite3.Error:
        row = None
    if row is None or str(row["value"]) != expected_version:
        _drop_all_user_objects(conn)
        conn.execute(
            """
            CREATE TABLE limsync (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO limsync(key, value) VALUES ('version', ?)",
            (expected_version,),
        )


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS limsync (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    _ensure_versioned_db(conn)

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
    conn.commit()


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
            SELECT source_endpoint, destination_endpoint
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
        rows = conn.execute(
            """
            SELECT relpath, content_state, metadata_state, metadata_diff_json, metadata_detail_json, metadata_source, left_size, right_size
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
