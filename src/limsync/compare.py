from __future__ import annotations

from datetime import UTC, datetime

from .models import ContentState, DiffRecord, FileRecord, MetadataState, NodeType


def _format_mode(mode: int) -> str:
    return f"0x{mode:03o}"


def _format_mtime_ns(value: int) -> str:
    dt = datetime.fromtimestamp(value / 1_000_000_000, tz=UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f UTC")


def _same_metadata(
    left: FileRecord, right: FileRecord, mtime_tolerance_ns: int
) -> tuple[bool, tuple[str, ...], tuple[str, ...]]:
    diff: list[str] = []
    details: list[str] = []
    if left.mode != right.mode:
        diff.append("mode")
        details.append(
            f"mode: left={_format_mode(left.mode)} right={_format_mode(right.mode)}"
        )
    if abs(left.mtime_ns - right.mtime_ns) > mtime_tolerance_ns:
        diff.append("mtime")
        details.append(
            f"mtime: left={_format_mtime_ns(left.mtime_ns)} right={_format_mtime_ns(right.mtime_ns)}"
        )
    return (len(diff) == 0, tuple(diff), tuple(details))


def _preferred_metadata_source(
    left: FileRecord, right: FileRecord, metadata_diff: tuple[str, ...]
) -> str | None:
    if "mode" in metadata_diff and left.mode != right.mode:
        return "left" if left.mode < right.mode else "right"
    if "mtime" in metadata_diff and left.mtime_ns != right.mtime_ns:
        return "left" if left.mtime_ns < right.mtime_ns else "right"
    return None


def compare_records(
    left_records: dict[str, FileRecord],
    right_records: dict[str, FileRecord],
    mtime_tolerance_ns: int = 2_000_000_000,
) -> list[DiffRecord]:
    diffs: list[DiffRecord] = []
    all_paths = sorted(set(left_records) | set(right_records))

    for relpath in all_paths:
        left = left_records.get(relpath)
        right = right_records.get(relpath)

        if left and not right:
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=ContentState.ONLY_LEFT,
                    metadata_state=MetadataState.NOT_APPLICABLE,
                    metadata_diff=(),
                    metadata_details=(),
                    metadata_source=None,
                    left_size=left.size,
                    right_size=None,
                )
            )
            continue

        if right and not left:
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=ContentState.ONLY_RIGHT,
                    metadata_state=MetadataState.NOT_APPLICABLE,
                    metadata_diff=(),
                    metadata_details=(),
                    metadata_source=None,
                    left_size=None,
                    right_size=right.size,
                )
            )
            continue

        assert left is not None and right is not None

        if left.node_type != right.node_type:
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=ContentState.DIFFERENT,
                    metadata_state=MetadataState.DIFFERENT,
                    metadata_diff=("type",),
                    metadata_details=(
                        f"type: {left.node_type.value} -> {right.node_type.value}",
                    ),
                    metadata_source=None,
                    left_size=left.size,
                    right_size=right.size,
                )
            )
            continue

        same_metadata, metadata_diff, metadata_details = _same_metadata(
            left, right, mtime_tolerance_ns
        )
        metadata_source = _preferred_metadata_source(left, right, metadata_diff)

        if left.node_type == NodeType.SYMLINK:
            left_target = left.link_target_key or left.link_target
            right_target = right.link_target_key or right.link_target
            same_symlink_target = left_target == right_target
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=(
                        ContentState.IDENTICAL
                        if same_symlink_target
                        else ContentState.DIFFERENT
                    ),
                    metadata_state=MetadataState.NOT_APPLICABLE,
                    metadata_diff=(),
                    metadata_details=(),
                    metadata_source=None,
                    left_size=left.size,
                    right_size=right.size,
                )
            )
            continue

        if left.node_type != NodeType.FILE:
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=ContentState.IDENTICAL,
                    metadata_state=MetadataState.IDENTICAL
                    if same_metadata
                    else MetadataState.DIFFERENT,
                    metadata_diff=metadata_diff,
                    metadata_details=metadata_details,
                    metadata_source=metadata_source,
                    left_size=left.size,
                    right_size=right.size,
                )
            )
            continue

        same_content = (
            left.size == right.size
            and abs(left.mtime_ns - right.mtime_ns) <= mtime_tolerance_ns
        )

        if same_content:
            content_state = ContentState.IDENTICAL
        elif left.size == right.size:
            content_state = ContentState.UNKNOWN
        else:
            content_state = ContentState.DIFFERENT

        diffs.append(
            DiffRecord(
                relpath=relpath,
                content_state=content_state,
                metadata_state=MetadataState.IDENTICAL
                if same_metadata
                else MetadataState.DIFFERENT,
                metadata_diff=metadata_diff,
                metadata_details=metadata_details,
                metadata_source=metadata_source,
                left_size=left.size,
                right_size=right.size,
            )
        )

    return diffs
