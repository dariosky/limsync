from __future__ import annotations

from .models import ContentState, DiffRecord, FileRecord, MetadataState, NodeType


def _same_metadata(
    local: FileRecord, remote: FileRecord, mtime_tolerance_ns: int
) -> tuple[bool, tuple[str, ...]]:
    diff: list[str] = []
    if local.mode != remote.mode:
        diff.append("mode")
    if abs(local.mtime_ns - remote.mtime_ns) > mtime_tolerance_ns:
        diff.append("mtime")
    return (len(diff) == 0, tuple(diff))


def compare_records(
    local_records: dict[str, FileRecord],
    remote_records: dict[str, FileRecord],
    mtime_tolerance_ns: int = 2_000_000_000,
) -> list[DiffRecord]:
    diffs: list[DiffRecord] = []
    all_paths = sorted(set(local_records) | set(remote_records))

    for relpath in all_paths:
        local = local_records.get(relpath)
        remote = remote_records.get(relpath)

        if local and not remote:
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=ContentState.ONLY_LOCAL,
                    metadata_state=MetadataState.NOT_APPLICABLE,
                    metadata_diff=(),
                )
            )
            continue

        if remote and not local:
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=ContentState.ONLY_REMOTE,
                    metadata_state=MetadataState.NOT_APPLICABLE,
                    metadata_diff=(),
                )
            )
            continue

        assert local is not None and remote is not None

        if local.node_type != remote.node_type:
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=ContentState.DIFFERENT,
                    metadata_state=MetadataState.DIFFERENT,
                    metadata_diff=("type",),
                )
            )
            continue

        same_metadata, metadata_diff = _same_metadata(local, remote, mtime_tolerance_ns)

        if local.node_type != NodeType.FILE:
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=ContentState.IDENTICAL,
                    metadata_state=MetadataState.IDENTICAL
                    if same_metadata
                    else MetadataState.DIFFERENT,
                    metadata_diff=metadata_diff,
                )
            )
            continue

        same_content = (
            local.size == remote.size
            and abs(local.mtime_ns - remote.mtime_ns) <= mtime_tolerance_ns
        )

        if same_content:
            content_state = ContentState.IDENTICAL
        elif local.size == remote.size:
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
            )
        )

    return diffs
