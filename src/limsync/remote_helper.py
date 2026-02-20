from __future__ import annotations

import argparse
import json
import os
import sqlite3
import stat
import sys
import time
from pathlib import PurePosixPath

try:
    from limsync.ignore_rules_shared import IgnoreRules
except Exception:
    # [[IGNORE_RULES_SHARED]]
    pass

CACHE_FOLDERS = {"__pycache__", ".pytest_cache", ".cache", ".ruff_cache"}
EXCLUDED_FOLDERS = {"node_modules", ".tox", ".venv", ".limsync"} | CACHE_FOLDERS
EXCLUDED_FILE_NAMES = {".DS_Store", "Icon\r"}


def emit(event: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=True) + "\n")
    sys.stdout.flush()


def node_type(st_mode: int) -> str:
    if stat.S_ISDIR(st_mode):
        return "dir"
    if stat.S_ISLNK(st_mode):
        return "symlink"
    return "file"


def _symlink_target_compare_key(
    root: str, home: str, relpath: str, target: str | None
) -> str | None:
    if target is None:
        return None

    normalized = PurePosixPath(target).as_posix()
    if os.path.isabs(normalized):
        abs_target = os.path.normpath(normalized)
    else:
        abs_target = os.path.normpath(
            os.path.join(root, os.path.dirname(relpath), normalized)
        )

    rel_to_root = os.path.relpath(abs_target, root)
    if rel_to_root == ".":
        return "inroot:."
    if not rel_to_root.startswith("../"):
        return f"inroot:{PurePosixPath(rel_to_root).as_posix()}"

    if os.path.isabs(normalized):
        rel_to_home = os.path.relpath(abs_target, home)
        if rel_to_home == ".":
            return "home:."
        if not rel_to_home.startswith("../"):
            return f"home:{PurePosixPath(rel_to_home).as_posix()}"
        return f"abs:{PurePosixPath(abs_target).as_posix()}"

    return f"rel:{normalized}"


def update_state_db(
    state_db: str,
    root: str,
    records: list[tuple[str, str, int, int, int]],
    dirs_scanned: int,
    files_seen: int,
) -> None:
    expanded_state_db = os.path.expanduser(state_db)
    db_path = (
        expanded_state_db
        if os.path.isabs(expanded_state_db)
        else os.path.join(root, expanded_state_db)
    )
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    now = int(time.time())
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                root TEXT NOT NULL,
                relpath TEXT NOT NULL,
                node_type TEXT NOT NULL,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                mode INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (root, relpath)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scan_meta (
                root TEXT PRIMARY KEY,
                scanned_at INTEGER NOT NULL,
                dirs_scanned INTEGER NOT NULL,
                files_seen INTEGER NOT NULL
            )
            """
        )

        with conn:
            conn.execute("CREATE TEMP TABLE seen(relpath TEXT PRIMARY KEY)")
            conn.executemany(
                """
                INSERT OR REPLACE INTO records
                (root, relpath, node_type, size, mtime_ns, mode, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (root, relpath, ntype, size, mtime_ns, mode, now)
                    for relpath, ntype, size, mtime_ns, mode in records
                ),
            )
            conn.executemany(
                "INSERT OR IGNORE INTO seen(relpath) VALUES (?)",
                ((relpath,) for relpath, _ntype, _size, _mtime_ns, _mode in records),
            )
            conn.execute(
                """
                DELETE FROM records
                WHERE root = ?
                  AND relpath NOT IN (SELECT relpath FROM seen)
                """,
                (root,),
            )
            conn.execute("DROP TABLE seen")
            conn.execute(
                """
                INSERT INTO scan_meta (root, scanned_at, dirs_scanned, files_seen)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(root) DO UPDATE SET
                    scanned_at = excluded.scanned_at,
                    dirs_scanned = excluded.dirs_scanned,
                    files_seen = excluded.files_seen
                """,
                (root, now, dirs_scanned, files_seen),
            )
    finally:
        conn.close()


def _normalize_subtree(subtree: str | None) -> PurePosixPath:
    if not subtree or subtree in {"", "."}:
        return PurePosixPath(".")
    return PurePosixPath(subtree)


def _prime_rules_for_subtree(
    rules: IgnoreRules, root: str, subtree_rel: PurePosixPath
) -> None:
    rules.load_if_exists(root, PurePosixPath("."))
    if subtree_rel == PurePosixPath("."):
        return
    current = PurePosixPath(".")
    for part in subtree_rel.parts[:-1]:
        current = (
            PurePosixPath(part) if current == PurePosixPath(".") else current / part
        )
        rules.load_if_exists(root, current)


def run_scan(
    root_arg: str, state_db: str, progress_interval: float, subtree: str | None = None
) -> int:
    root = os.path.expanduser(root_arg)
    root = os.path.abspath(root)
    home = os.path.abspath(os.path.expanduser("~"))
    if not os.path.isdir(root):
        emit({"event": "error", "message": f"Root not found: {root}"})
        return 2

    rules = IgnoreRules()
    subtree_rel = _normalize_subtree(subtree)
    _prime_rules_for_subtree(rules, root, subtree_rel)
    records_for_db: list[tuple[str, str, int, int, int]] = []

    dirs_scanned = 0
    files_seen = 0
    errors = 0
    last_progress = 0.0

    def on_walk_error(exc: OSError) -> None:
        nonlocal errors
        errors += 1
        emit(
            {
                "event": "error",
                "message": str(exc),
                "path": getattr(exc, "filename", None),
            }
        )

    start_root = root
    if subtree_rel != PurePosixPath("."):
        start_candidate = os.path.join(root, subtree_rel.as_posix())
        if os.path.isfile(start_candidate) or os.path.islink(start_candidate):
            start_root = os.path.dirname(start_candidate)
        elif os.path.isdir(start_candidate):
            start_root = start_candidate
        else:
            emit(
                {
                    "event": "done",
                    "root": root,
                    "dirs_scanned": 0,
                    "files_seen": 0,
                    "errors": 0,
                    "state_db": os.path.expanduser(state_db),
                }
            )
            return 0

    for current_dir, dirs, files in os.walk(
        start_root, topdown=True, onerror=on_walk_error, followlinks=False
    ):
        current_abs = os.path.abspath(current_dir)
        rel_dir = os.path.relpath(current_abs, root)
        rel_posix = PurePosixPath(
            "." if rel_dir == "." else rel_dir.replace(os.sep, "/")
        )

        dirs_scanned += 1
        now = time.monotonic()
        if (now - last_progress) >= progress_interval:
            emit(
                {
                    "event": "progress",
                    "relpath": rel_posix.as_posix(),
                    "dirs_scanned": dirs_scanned,
                    "files_seen": files_seen,
                }
            )
            last_progress = now

        rules.load_if_exists(root, rel_posix)

        kept_dirs: list[str] = []
        for dirname in dirs:
            if dirname in EXCLUDED_FOLDERS:
                continue
            child_rel = (
                PurePosixPath(dirname)
                if rel_posix == PurePosixPath(".")
                else rel_posix / dirname
            )
            if rules.is_ignored(child_rel, is_dir=True):
                continue
            kept_dirs.append(dirname)
        dirs[:] = kept_dirs

        for filename in files:
            if filename in EXCLUDED_FILE_NAMES:
                continue
            child_rel = (
                PurePosixPath(filename)
                if rel_posix == PurePosixPath(".")
                else rel_posix / filename
            )
            if rules.is_ignored(child_rel, is_dir=False):
                continue

            full_path = os.path.join(current_abs, filename)
            try:
                st = os.lstat(full_path)
            except OSError as exc:
                errors += 1
                emit({"event": "error", "message": str(exc), "path": full_path})
                continue

            ntype = node_type(st.st_mode)
            if ntype == "dir":
                continue

            relpath = child_rel.as_posix()
            files_seen += 1
            link_target = None
            link_target_key = None
            if ntype == "symlink":
                try:
                    link_target = PurePosixPath(os.readlink(full_path)).as_posix()
                    link_target_key = _symlink_target_compare_key(
                        root, home, relpath, link_target
                    )
                except OSError:
                    link_target = None
                    link_target_key = None
            record = {
                "event": "record",
                "relpath": relpath,
                "node_type": ntype,
                "size": int(st.st_size),
                "mtime_ns": int(st.st_mtime_ns),
                "mode": int(stat.S_IMODE(st.st_mode)),
                "link_target": link_target,
                "link_target_key": link_target_key,
                "owner": None,
                "group": None,
            }
            emit(record)
            records_for_db.append(
                (
                    relpath,
                    ntype,
                    int(st.st_size),
                    int(st.st_mtime_ns),
                    int(stat.S_IMODE(st.st_mode)),
                )
            )

    if subtree_rel == PurePosixPath("."):
        try:
            update_state_db(state_db, root, records_for_db, dirs_scanned, files_seen)
        except Exception as exc:
            emit({"event": "error", "message": f"state_db_update_failed: {exc}"})
            errors += 1

    emit(
        {
            "event": "done",
            "root": root,
            "dirs_scanned": dirs_scanned,
            "files_seen": files_seen,
            "errors": errors,
            "state_db": os.path.expanduser(state_db),
        }
    )
    # Permission/transient walk errors are reported in-stream but do not fail the scan.
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remote helper for limsync")
    parser.add_argument("--root", required=True)
    parser.add_argument("--state-db", default=".limsync/state.sqlite3")
    parser.add_argument("--progress-interval", type=float, default=0.25)
    parser.add_argument("--subtree", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_scan(args.root, args.state_db, args.progress_interval, args.subtree)


if __name__ == "__main__":
    raise SystemExit(main())
