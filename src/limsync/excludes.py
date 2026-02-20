from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

from .config import EXCLUDED_FILE_NAMES, EXCLUDED_FOLDERS
from .ignore_rules_shared import IgnoreRules


def is_excluded_folder_name(name: str) -> bool:
    return name in EXCLUDED_FOLDERS


def is_excluded_file_name(name: str) -> bool:
    return name in EXCLUDED_FILE_NAMES


def load_ignore_rules_tree(root: Path) -> IgnoreRules:
    """Load all nested `.dropboxignore` files under `root`."""
    resolved = root.expanduser().resolve()
    rules = IgnoreRules()
    if not resolved.exists() or not resolved.is_dir():
        return rules

    for current_dir, dirs, _files in os.walk(resolved, topdown=True):
        current_path = Path(current_dir)
        rel_dir = PurePosixPath(".")
        if current_path != resolved:
            rel_dir = PurePosixPath(current_path.relative_to(resolved).as_posix())
        rules.load_if_exists(str(resolved), rel_dir)
        dirs[:] = [name for name in dirs if not is_excluded_folder_name(name)]
    return rules
