from __future__ import annotations

import os
import stat
import time
from collections.abc import Callable
from pathlib import Path, PurePosixPath

from .excludes import IgnoreRules, is_excluded_file_name, is_excluded_folder_name
from .models import FileRecord, NodeType
from .text_utils import normalize_text


def _node_type(st_mode: int) -> NodeType:
    if stat.S_ISDIR(st_mode):
        return NodeType.DIR
    if stat.S_ISLNK(st_mode):
        return NodeType.SYMLINK
    return NodeType.FILE


class LocalScanner:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def scan(
        self,
        progress_cb: Callable[[PurePosixPath, int, int], None] | None = None,
    ) -> dict[str, FileRecord]:
        if not self.root.exists() or not self.root.is_dir():
            raise FileNotFoundError(f"Local root not found: {self.root}")

        records: dict[str, FileRecord] = {}
        rules = IgnoreRules()
        dirs_scanned = 0
        files_seen = 0
        last_progress = 0.0

        for current_dir, dirs, files in os.walk(self.root, topdown=True):
            current_path = Path(current_dir)
            rel_dir = PurePosixPath(".")
            if current_path != self.root:
                rel_dir = PurePosixPath(current_path.relative_to(self.root).as_posix())
            dirs_scanned += 1

            now = time.monotonic()
            if progress_cb is not None and (now - last_progress) >= 0.2:
                progress_cb(rel_dir, dirs_scanned, files_seen)
                last_progress = now

            rules.load_if_exists(self.root, rel_dir)

            kept_dirs: list[str] = []
            for dir_name in dirs:
                if is_excluded_folder_name(dir_name):
                    continue
                child_rel = (
                    PurePosixPath(dir_name)
                    if rel_dir == PurePosixPath(".")
                    else rel_dir / dir_name
                )
                if rules.is_ignored(child_rel, is_dir=True):
                    continue
                kept_dirs.append(dir_name)
            dirs[:] = kept_dirs

            for filename in files:
                if is_excluded_file_name(filename):
                    continue
                child_rel = (
                    PurePosixPath(filename)
                    if rel_dir == PurePosixPath(".")
                    else rel_dir / filename
                )
                full_path = self.root / child_rel.as_posix()
                if rules.is_ignored(child_rel, is_dir=False):
                    continue

                st = full_path.lstat()
                node_type = _node_type(st.st_mode)
                if node_type == NodeType.DIR:
                    continue
                files_seen += 1

                relpath = normalize_text(child_rel.as_posix())
                records[relpath] = FileRecord(
                    relpath=relpath,
                    node_type=node_type,
                    size=st.st_size,
                    mtime_ns=st.st_mtime_ns,
                    mode=stat.S_IMODE(st.st_mode),
                    owner=None,
                    group=None,
                )

        if progress_cb is not None:
            progress_cb(PurePosixPath("."), dirs_scanned, files_seen)

        return records
