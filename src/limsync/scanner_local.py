from __future__ import annotations

import os
import stat
import time
from collections.abc import Callable
from pathlib import Path, PurePosixPath

from .excludes import IgnoreRules, is_excluded_file_name, is_excluded_folder_name
from .models import FileRecord, NodeType
from .symlink_utils import symlink_target_compare_key
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
        self.home = Path.home().expanduser().resolve()

    def scan(
        self,
        progress_cb: Callable[[PurePosixPath, int, int], None] | None = None,
        subtree: PurePosixPath | None = None,
    ) -> dict[str, FileRecord]:
        if not self.root.exists() or not self.root.is_dir():
            raise FileNotFoundError(f"Local root not found: {self.root}")

        records: dict[str, FileRecord] = {}
        rules = IgnoreRules()
        subtree_rel = self._normalize_subtree(subtree)
        self._prime_rules_for_subtree(rules, subtree_rel)
        dirs_scanned = 0
        files_seen = 0
        last_progress = 0.0

        start_root = self._scan_start_path(subtree_rel)
        if start_root is None:
            return records

        for current_dir, dirs, files in os.walk(start_root, topdown=True):
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
                link_target = None
                link_target_key = None
                if node_type == NodeType.SYMLINK:
                    link_target = normalize_text(os.readlink(full_path))
                    link_target_key = symlink_target_compare_key(
                        relpath=relpath,
                        target=link_target,
                        root=self.root,
                        home=self.home,
                    )
                records[relpath] = FileRecord(
                    relpath=relpath,
                    node_type=node_type,
                    size=st.st_size,
                    mtime_ns=st.st_mtime_ns,
                    mode=stat.S_IMODE(st.st_mode),
                    link_target=link_target,
                    link_target_key=link_target_key,
                    owner=None,
                    group=None,
                )

        if progress_cb is not None:
            progress_cb(PurePosixPath("."), dirs_scanned, files_seen)

        return records

    def _normalize_subtree(self, subtree: PurePosixPath | None) -> PurePosixPath:
        if subtree is None:
            return PurePosixPath(".")
        text = (
            subtree.as_posix() if isinstance(subtree, PurePosixPath) else str(subtree)
        )
        normalized = PurePosixPath(text)
        if str(normalized) in {"", "."}:
            return PurePosixPath(".")
        return normalized

    def _scan_start_path(self, subtree: PurePosixPath) -> Path | None:
        if subtree == PurePosixPath("."):
            return self.root
        candidate = self.root / subtree.as_posix()
        if candidate.is_file() or candidate.is_symlink():
            return candidate.parent
        if candidate.is_dir():
            return candidate
        return None

    def _prime_rules_for_subtree(
        self, rules: IgnoreRules, subtree: PurePosixPath
    ) -> None:
        rules.load_if_exists(self.root, PurePosixPath("."))
        if subtree == PurePosixPath("."):
            return
        current = PurePosixPath(".")
        for part in subtree.parts[:-1]:
            current = (
                PurePosixPath(part) if current == PurePosixPath(".") else current / part
            )
            rules.load_if_exists(self.root, current)
