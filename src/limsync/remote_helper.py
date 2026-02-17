from __future__ import annotations

import argparse
import fnmatch
import json
import os
import sqlite3
import stat
import sys
import time
from pathlib import PurePosixPath

CACHE_FOLDERS = {"__pycache__", ".pytest_cache", ".cache", ".ruff_cache"}
EXCLUDED_FOLDERS = {"node_modules", ".tox", ".venv", ".limsync"} | CACHE_FOLDERS
EXCLUDED_FILE_NAMES = {".DS_Store", "Icon\r"}


def emit(event: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=True) + "\n")
    sys.stdout.flush()


def _to_posix(path: PurePosixPath) -> str:
    return "." if str(path) == "." else path.as_posix()


class IgnoreRules:
    def __init__(self) -> None:
        self._patterns: dict[str, list[str]] = {}

    def add(self, base_relpath: PurePosixPath, lines: list[str]) -> None:
        patterns = []
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            patterns.append(line)
        if patterns:
            self._patterns[_to_posix(base_relpath)] = patterns

    def load_if_exists(self, root: str, dir_relpath: PurePosixPath) -> None:
        rel = "" if str(dir_relpath) == "." else dir_relpath.as_posix()
        candidate = os.path.join(root, rel, ".dropboxignore")
        if not os.path.isfile(candidate):
            return
        try:
            with open(candidate, encoding="utf-8", errors="replace") as f:
                lines = f.read().splitlines()
        except OSError:
            return
        self.add(dir_relpath, lines)

    def _pattern_matches(self, local_target: str, pattern: str, anchored: bool) -> bool:
        target = local_target.rstrip("/")
        if anchored:
            return fnmatch.fnmatch(target, pattern)

        if "/" not in pattern:
            if fnmatch.fnmatch(target, pattern):
                return True
            parts = [p for p in target.split("/") if p]
            return any(fnmatch.fnmatch(part, pattern) for part in parts)

        if fnmatch.fnmatch(target, pattern):
            return True
        parts = [p for p in target.split("/") if p]
        for idx in range(1, len(parts)):
            suffix = "/".join(parts[idx:])
            if fnmatch.fnmatch(suffix, pattern):
                return True
        return False

    def _match_patterns(
        self, local_target: str, is_dir: bool, patterns: list[str]
    ) -> bool | None:
        result: bool | None = None
        for raw in patterns:
            negate = raw.startswith("!")
            pattern = raw[1:] if negate else raw
            if not pattern:
                continue

            dir_only = pattern.endswith("/")
            if dir_only and not is_dir:
                continue
            if dir_only:
                pattern = pattern.rstrip("/")

            anchored = pattern.startswith("/")
            if anchored:
                pattern = pattern.lstrip("/")

            if self._pattern_matches(local_target, pattern, anchored):
                result = not negate
        return result

    def is_ignored(self, relpath: PurePosixPath, is_dir: bool) -> bool:
        target = relpath.as_posix()
        if is_dir and not target.endswith("/"):
            target = f"{target}/"

        ancestors = [PurePosixPath(".")]
        parts = relpath.parts
        for idx in range(len(parts) - 1):
            ancestors.append(PurePosixPath(*parts[: idx + 1]))

        ignored = False
        for ancestor in ancestors:
            anc_key = _to_posix(ancestor)
            patterns = self._patterns.get(anc_key)
            if not patterns:
                continue

            if anc_key == ".":
                local_target = target
            else:
                prefix = f"{anc_key}/"
                if not target.startswith(prefix):
                    continue
                local_target = target[len(prefix) :]

            matched = self._match_patterns(local_target, is_dir, patterns)
            if matched is not None:
                ignored = matched

        return ignored


def node_type(st_mode: int) -> str:
    if stat.S_ISDIR(st_mode):
        return "dir"
    if stat.S_ISLNK(st_mode):
        return "symlink"
    return "file"


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
            record = {
                "event": "record",
                "relpath": relpath,
                "node_type": ntype,
                "size": int(st.st_size),
                "mtime_ns": int(st.st_mtime_ns),
                "mode": int(stat.S_IMODE(st.st_mode)),
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
