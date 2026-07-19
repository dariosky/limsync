from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath

from rich.text import Text

from .models import ContentState, DiffRecord, MetadataState


@dataclass
class FolderCounts:
    only_left: int = 0
    only_right: int = 0
    identical: int = 0
    metadata_only: int = 0
    different: int = 0
    uncertain: int = 0
    metadata_fields: dict[str, int] = field(default_factory=dict)


@dataclass
class ActionCounts:
    left: int = 0
    right: int = 0
    suggested: int = 0
    ignored: int = 0

    @property
    def total(self) -> int:
        return self.left + self.right + self.suggested + self.ignored


@dataclass
class FileEntry:
    relpath: str
    name: str
    content_state: str
    metadata_state: str
    metadata_diff: list[str]
    metadata_details: list[str]
    left_size: int | None
    right_size: int | None


@dataclass
class DirEntry:
    name: str
    relpath: str
    dirs: dict[str, DirEntry] = field(default_factory=dict)
    files: list[FileEntry] = field(default_factory=list)
    counts: FolderCounts = field(default_factory=FolderCounts)


def _file_counts(file_entry: FileEntry) -> FolderCounts:
    counts = FolderCounts()
    if file_entry.content_state == "only_left":
        counts.only_left = 1
    elif file_entry.content_state == "only_right":
        counts.only_right = 1
    elif file_entry.content_state == "different":
        counts.different = 1
    elif file_entry.content_state == "unknown":
        counts.uncertain = 1
    elif file_entry.content_state == "identical":
        if file_entry.metadata_state == "different":
            counts.metadata_only = 1
            for field_name in file_entry.metadata_diff:
                counts.metadata_fields[field_name] = (
                    counts.metadata_fields.get(field_name, 0) + 1
                )
        else:
            counts.identical = 1
    return counts


def _apply_counts(target: FolderCounts, increment: FolderCounts) -> None:
    target.only_left += increment.only_left
    target.only_right += increment.only_right
    target.identical += increment.identical
    target.metadata_only += increment.metadata_only
    target.different += increment.different
    target.uncertain += increment.uncertain
    for key, value in increment.metadata_fields.items():
        target.metadata_fields[key] = target.metadata_fields.get(key, 0) + value


def _is_identical_folder(entry: DirEntry) -> bool:
    c = entry.counts
    return (
        c.only_left == 0
        and c.only_right == 0
        and c.metadata_only == 0
        and c.different == 0
        and c.uncertain == 0
    )


def _is_changed(entry: FileEntry) -> bool:
    return not (
        entry.content_state == "identical" and entry.metadata_state == "identical"
    )


def _action_summary_parts(action_counts: ActionCounts | None) -> list[str]:
    if action_counts is None or action_counts.total == 0:
        return []

    parts: list[str] = []
    if action_counts.left:
        parts.append(f"Left {action_counts.left}")
    if action_counts.right:
        parts.append(f"Right {action_counts.right}")
    if action_counts.suggested:
        parts.append(f"Suggested {action_counts.suggested}")
    if action_counts.ignored and not parts:
        parts.append(f"Ignored {action_counts.ignored}")
    return parts


def _folder_label(
    entry: DirEntry,
    *,
    include_identical: bool = True,
    action_counts: ActionCounts | None = None,
    counts: FolderCounts | None = None,
) -> Text:
    action_parts = _action_summary_parts(action_counts)
    if action_parts:
        return Text.assemble(
            (entry.name, "bold"), "  ", (" | ".join(action_parts), "cyan")
        )

    c = counts or entry.counts
    only_left = c.only_left
    only_right = c.only_right
    parts: list[str] = []
    if only_left:
        parts.append(f"Left {only_left}")
    if only_right:
        parts.append(f"Right {only_right}")
    if c.different:
        parts.append(f"Conflict {c.different}")
    if c.uncertain:
        parts.append(f"Uncertain {c.uncertain}")
    if c.metadata_only:
        parts.append(f"Metadata {c.metadata_only}")
    if include_identical and c.identical:
        parts.append(f"Identical {c.identical}")
    summary = " | ".join(parts) if parts else "No changes"
    return Text.assemble((entry.name, "bold"), "  ", (summary, "cyan"))


def _action_counts_for_files(
    relpaths: list[str],
    files_by_relpath: dict[str, FileEntry],
    action_overrides: dict[str, str],
) -> ActionCounts:
    counts = ActionCounts()
    for relpath in relpaths:
        entry = files_by_relpath.get(relpath)
        if entry is None or not _is_changed(entry):
            continue
        action = action_overrides.get(relpath)
        if action is None:
            continue
        if action == "left_wins":
            counts.left += 1
        elif action == "right_wins":
            counts.right += 1
        elif action == "suggested":
            counts.suggested += 1
        elif action == "ignore":
            counts.ignored += 1
    return counts


def _folder_action_counts_by_relpath(
    dir_files_map: dict[str, list[str]],
    files_by_relpath: dict[str, FileEntry],
    action_overrides: dict[str, str],
    included_relpaths: set[str] | None = None,
) -> dict[str, ActionCounts]:
    return {
        relpath: _action_counts_for_files(
            (
                file_relpaths
                if included_relpaths is None
                else [path for path in file_relpaths if path in included_relpaths]
            ),
            files_by_relpath,
            action_overrides,
        )
        for relpath, file_relpaths in dir_files_map.items()
    }


def _folder_counts_by_relpath(
    dir_files_map: dict[str, list[str]],
    files_by_relpath: dict[str, FileEntry],
    included_changed_relpaths: set[str],
) -> dict[str, FolderCounts]:
    counts_by_dir: dict[str, FolderCounts] = {}
    for dir_relpath, file_relpaths in dir_files_map.items():
        counts = FolderCounts()
        for file_relpath in file_relpaths:
            entry = files_by_relpath.get(file_relpath)
            if entry is None:
                continue
            if _is_changed(entry) and file_relpath not in included_changed_relpaths:
                continue
            _apply_counts(counts, _file_counts(entry))
        counts_by_dir[dir_relpath] = counts
    return counts_by_dir


def _file_reason(file_entry: FileEntry) -> str:
    if (
        file_entry.content_state == "different"
        and file_entry.left_size is not None
        and file_entry.right_size is not None
        and file_entry.left_size != file_entry.right_size
    ):
        return "size"
    return ",".join(file_entry.metadata_diff) if file_entry.metadata_diff else "-"


def _file_label(file_entry: FileEntry) -> Text:
    if file_entry.content_state == "only_left":
        badge = "Left"
    elif file_entry.content_state == "only_right":
        badge = "Right"
    elif file_entry.content_state == "different":
        badge = "Conflict"
    elif file_entry.content_state == "unknown":
        badge = "Uncertain"
    elif (
        file_entry.content_state == "identical"
        and file_entry.metadata_state == "different"
    ):
        badge = "Metadata"
    else:
        badge = "Identical"
    return Text.assemble(
        (file_entry.name, "white"),
        "  ",
        (f"[{badge}]", "yellow"),
        " ",
        (_file_reason(file_entry), "green"),
    )


def _row_to_diff(row: dict[str, object]) -> DiffRecord:
    return DiffRecord(
        relpath=str(row["relpath"]),
        content_state=ContentState(str(row["content_state"])),
        metadata_state=MetadataState(str(row["metadata_state"])),
        metadata_diff=tuple(str(item) for item in row.get("metadata_diff", [])),
        metadata_details=tuple(str(item) for item in row.get("metadata_details", [])),
        metadata_source=(
            str(row["metadata_source"])
            if row.get("metadata_source") is not None
            else None
        ),
        left_size=(int(row["left_size"]) if row.get("left_size") is not None else None),
        right_size=(
            int(row["right_size"]) if row.get("right_size") is not None else None
        ),
    )


def _build_model(
    rows: list[dict[str, object]],
    root_name: str,
) -> tuple[
    DirEntry,
    dict[str, DirEntry],
    dict[str, FileEntry],
    dict[str, list[str]],
    dict[str, DiffRecord],
]:
    root = DirEntry(name=root_name, relpath=".")
    dirs_by_relpath: dict[str, DirEntry] = {".": root}
    files_by_relpath: dict[str, FileEntry] = {}
    dir_files_map: dict[str, list[str]] = {".": []}
    diffs_by_relpath: dict[str, DiffRecord] = {}

    for row in rows:
        relpath = str(row["relpath"])
        diffs_by_relpath[relpath] = _row_to_diff(row)
        path = PurePosixPath(relpath)
        parts = path.parts
        if not parts:
            continue

        current = root
        current_rel = PurePosixPath(".")
        lineage = [root]
        lineage_keys = ["."]

        for part in parts[:-1]:
            next_rel = (
                PurePosixPath(part)
                if current_rel == PurePosixPath(".")
                else current_rel / part
            )
            next_key = next_rel.as_posix()
            child = current.dirs.get(part)
            if child is None:
                child = DirEntry(name=part, relpath=next_key)
                current.dirs[part] = child
                dirs_by_relpath[next_key] = child
            current = child
            current_rel = next_rel
            lineage.append(current)
            lineage_keys.append(next_key)
            dir_files_map.setdefault(next_key, [])

        file_entry = FileEntry(
            relpath=relpath,
            name=parts[-1],
            content_state=str(row["content_state"]),
            metadata_state=str(row["metadata_state"]),
            metadata_diff=list(row.get("metadata_diff", [])),
            metadata_details=list(row.get("metadata_details", [])),
            left_size=(
                int(row["left_size"]) if row.get("left_size") is not None else None
            ),
            right_size=(
                int(row["right_size"]) if row.get("right_size") is not None else None
            ),
        )
        current.files.append(file_entry)
        files_by_relpath[relpath] = file_entry
        for dir_key in lineage_keys:
            dir_files_map.setdefault(dir_key, []).append(relpath)

        delta = _file_counts(file_entry)
        for ancestor in lineage:
            _apply_counts(ancestor.counts, delta)

    return root, dirs_by_relpath, files_by_relpath, dir_files_map, diffs_by_relpath
