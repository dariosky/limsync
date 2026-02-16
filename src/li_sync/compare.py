from __future__ import annotations

from datetime import UTC, datetime

from .models import ContentState, DiffRecord, FileRecord, MetadataState, NodeType


def _format_mode(mode: int) -> str:
    return f"0x{mode:03o}"


def _format_mtime_ns(value: int) -> str:
    dt = datetime.fromtimestamp(value / 1_000_000_000, tz=UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f UTC")


def _same_metadata(
    local: FileRecord, remote: FileRecord, mtime_tolerance_ns: int
) -> tuple[bool, tuple[str, ...], tuple[str, ...]]:
    diff: list[str] = []
    details: list[str] = []
    if local.mode != remote.mode:
        diff.append("mode")
        details.append(
            f"mode: local={_format_mode(local.mode)} remote={_format_mode(remote.mode)}"
        )
    if abs(local.mtime_ns - remote.mtime_ns) > mtime_tolerance_ns:
        diff.append("mtime")
        details.append(
            f"mtime: local={_format_mtime_ns(local.mtime_ns)} remote={_format_mtime_ns(remote.mtime_ns)}"
        )
    return (len(diff) == 0, tuple(diff), tuple(details))


def _preferred_metadata_source(
    local: FileRecord, remote: FileRecord, metadata_diff: tuple[str, ...]
) -> str | None:
    if "mode" in metadata_diff and local.mode != remote.mode:
        return "local" if local.mode < remote.mode else "remote"
    if "mtime" in metadata_diff and local.mtime_ns != remote.mtime_ns:
        return "local" if local.mtime_ns < remote.mtime_ns else "remote"
    return None


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
                    metadata_details=(),
                    metadata_source=None,
                    local_size=local.size,
                    remote_size=None,
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
                    metadata_details=(),
                    metadata_source=None,
                    local_size=None,
                    remote_size=remote.size,
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
                    metadata_details=(
                        f"type: {local.node_type.value} -> {remote.node_type.value}",
                    ),
                    metadata_source=None,
                    local_size=local.size,
                    remote_size=remote.size,
                )
            )
            continue

        same_metadata, metadata_diff, metadata_details = _same_metadata(
            local, remote, mtime_tolerance_ns
        )
        metadata_source = _preferred_metadata_source(local, remote, metadata_diff)

        if local.node_type == NodeType.SYMLINK:
            # Symlink metadata (mode/mtime) is often unstable across platforms.
            # Treat symlinks as metadata-insensitive to avoid recurring drift noise.
            diffs.append(
                DiffRecord(
                    relpath=relpath,
                    content_state=ContentState.IDENTICAL,
                    metadata_state=MetadataState.NOT_APPLICABLE,
                    metadata_diff=(),
                    metadata_details=(),
                    metadata_source=None,
                    local_size=local.size,
                    remote_size=remote.size,
                )
            )
            continue

        if local.node_type != NodeType.FILE:
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
                    local_size=local.size,
                    remote_size=remote.size,
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
                metadata_details=metadata_details,
                metadata_source=metadata_source,
                local_size=local.size,
                remote_size=remote.size,
            )
        )

    return diffs
