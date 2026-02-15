from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .models import DiffRecord
from .text_utils import normalize_text


@dataclass(frozen=True)
class ScanRunSummary:
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


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
        CREATE TABLE IF NOT EXISTS scan_diffs (
            run_id INTEGER NOT NULL,
            relpath TEXT NOT NULL,
            content_state TEXT NOT NULL,
            metadata_state TEXT NOT NULL,
            metadata_diff_json TEXT NOT NULL,
            PRIMARY KEY (run_id, relpath),
            FOREIGN KEY (run_id) REFERENCES scan_runs(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scan_diffs_run_content
        ON scan_diffs(run_id, content_state, metadata_state)
        """
    )


def save_scan_run(
    db_path: Path, summary: ScanRunSummary, diffs: list[DiffRecord]
) -> int:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        with conn:
            cursor = conn.execute(
                """
                INSERT INTO scan_runs (
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
                    metadata_only
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            run_id = int(cursor.lastrowid)
            conn.executemany(
                """
                INSERT INTO scan_diffs (
                    run_id,
                    relpath,
                    content_state,
                    metadata_state,
                    metadata_diff_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        normalize_text(diff.relpath),
                        diff.content_state.value,
                        diff.metadata_state.value,
                        json.dumps(list(diff.metadata_diff), ensure_ascii=True),
                    )
                    for diff in diffs
                ],
            )
        return run_id
    finally:
        conn.close()


def get_latest_run_id(db_path: Path) -> int | None:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        row = conn.execute(
            "SELECT id FROM scan_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return int(row["id"]) if row else None
    finally:
        conn.close()


def query_diffs(
    db_path: Path,
    run_id: int,
    content: str,
    metadata_only: bool,
    offset: int,
    limit: int,
) -> tuple[int, list[dict[str, object]]]:
    conn = _connect(db_path)
    try:
        _init_schema(conn)
        where = ["run_id = ?"]
        params: list[object] = [run_id]

        if content != "all":
            where.append("content_state = ?")
            params.append(content)

        if metadata_only:
            where.append("content_state = 'identical' AND metadata_state = 'different'")

        clause = " AND ".join(where)

        total_row = conn.execute(
            f"SELECT COUNT(*) AS c FROM scan_diffs WHERE {clause}",
            params,
        ).fetchone()
        total = int(total_row["c"] if total_row else 0)

        rows = conn.execute(
            f"""
            SELECT relpath, content_state, metadata_state, metadata_diff_json
            FROM scan_diffs
            WHERE {clause}
            ORDER BY relpath
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()

        data = [
            {
                "relpath": row["relpath"],
                "content_state": row["content_state"],
                "metadata_state": row["metadata_state"],
                "metadata_diff": json.loads(row["metadata_diff_json"]),
            }
            for row in rows
        ]
        return total, data
    finally:
        conn.close()
