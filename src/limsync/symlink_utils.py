from __future__ import annotations

import os
from pathlib import Path, PurePosixPath


def _normalize_target_text(target: str) -> str:
    return PurePosixPath(target).as_posix()


def symlink_target_compare_key(
    *,
    relpath: str,
    target: str | None,
    root: Path,
    home: Path,
) -> str | None:
    if target is None:
        return None

    normalized = _normalize_target_text(target)
    link_path = root / relpath
    target_path = Path(normalized)
    if target_path.is_absolute():
        abs_target = target_path
    else:
        abs_target = (link_path.parent / target_path).resolve(strict=False)

    try:
        rel_to_root = abs_target.relative_to(root)
        return f"inroot:{PurePosixPath(rel_to_root.as_posix()).as_posix()}"
    except ValueError:
        pass

    if target_path.is_absolute():
        try:
            rel_to_home = abs_target.relative_to(home)
            return f"home:{PurePosixPath(rel_to_home.as_posix()).as_posix()}"
        except ValueError:
            return f"abs:{PurePosixPath(abs_target.as_posix()).as_posix()}"

    return f"rel:{normalized}"


def map_symlink_target_for_destination(
    *,
    source_root: Path,
    source_home: Path,
    source_relpath: str,
    source_target: str,
    destination_root: Path,
    destination_home: Path,
    destination_relpath: str,
) -> str:
    normalized_target = _normalize_target_text(source_target)
    source_link_path = source_root / source_relpath
    destination_link_path = destination_root / destination_relpath
    target_path = Path(normalized_target)
    if target_path.is_absolute():
        abs_target = target_path
    else:
        abs_target = (source_link_path.parent / target_path).resolve(strict=False)

    try:
        rel_to_source_root = abs_target.relative_to(source_root)
        mapped_abs = destination_root / rel_to_source_root
        mapped_rel = os.path.relpath(mapped_abs, destination_link_path.parent)
        return PurePosixPath(mapped_rel).as_posix()
    except ValueError:
        pass

    if target_path.is_absolute():
        try:
            rel_to_source_home = abs_target.relative_to(source_home)
            mapped_home_abs = destination_home / rel_to_source_home
            return PurePosixPath(mapped_home_abs.as_posix()).as_posix()
        except ValueError:
            return normalized_target

    return normalized_target
