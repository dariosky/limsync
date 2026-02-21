from __future__ import annotations

import sqlite3

from limsync.models import ContentState, MetadataState
from limsync.state_db import (
    ScanStateSummary,
    _project_version,
    load_current_diffs,
    save_current_state,
)

from conftest import mk_diff


def _summary() -> ScanStateSummary:
    return ScanStateSummary(
        source_endpoint="local:/left",
        destination_endpoint="local:/right",
        source_scan_seconds=0.1,
        destination_scan_seconds=0.2,
        source_files=1,
        destination_files=0,
        compared_paths=1,
        only_source=1,
        only_destination=0,
        different_content=0,
        uncertain=0,
        metadata_only=0,
    )


def _diffs():
    return [
        mk_diff(
            "a.txt",
            content_state=ContentState.ONLY_LEFT,
            metadata_state=MetadataState.NOT_APPLICABLE,
            left_size=5,
            right_size=None,
        )
    ]


def test_save_current_state_bootstraps_versioned_db(tmp_path) -> None:
    db_path = tmp_path / "state.sqlite3"
    save_current_state(db_path, _summary(), _diffs())

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute(
            "SELECT value FROM limsync WHERE key = 'version'"
        ).fetchone()
        assert version is not None
        assert str(version["value"]) == _project_version()
    finally:
        conn.close()


def test_save_current_state_reinitializes_when_version_missing(tmp_path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE state_meta (singleton_id INTEGER PRIMARY KEY, only_local INTEGER NOT NULL)"
        )
        conn.commit()
    finally:
        conn.close()

    save_current_state(db_path, _summary(), _diffs())

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute(
            "SELECT value FROM limsync WHERE key = 'version'"
        ).fetchone()
        assert version is not None
        assert str(version["value"]) == _project_version()
        cols = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(state_meta)").fetchall()
        }
        assert "only_left" in cols
        assert "only_local" not in cols
    finally:
        conn.close()


def test_load_current_diffs_reinitializes_on_version_mismatch(tmp_path) -> None:
    db_path = tmp_path / "state.sqlite3"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE limsync (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute(
            "INSERT INTO limsync(key, value) VALUES ('version', '0.0.0-test')"
        )
        conn.execute("CREATE TABLE current_diffs (relpath TEXT PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()

    rows = load_current_diffs(db_path)
    assert rows == []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute(
            "SELECT value FROM limsync WHERE key = 'version'"
        ).fetchone()
        assert version is not None
        assert str(version["value"]) == _project_version()
        cols = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(current_diffs)").fetchall()
        }
        assert "left_size" in cols
        assert "right_size" in cols
    finally:
        conn.close()
