from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class NodeType(str, Enum):
    FILE = "file"
    DIR = "dir"
    SYMLINK = "symlink"


class ContentState(str, Enum):
    IDENTICAL = "identical"
    DIFFERENT = "different"
    ONLY_LOCAL = "only_local"
    ONLY_REMOTE = "only_remote"
    UNKNOWN = "unknown"


class MetadataState(str, Enum):
    IDENTICAL = "identical"
    DIFFERENT = "different"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True)
class FileRecord:
    relpath: str
    node_type: NodeType
    size: int
    mtime_ns: int
    mode: int
    link_target: str | None = None
    link_target_key: str | None = None
    owner: str | None = None
    group: str | None = None


@dataclass(frozen=True)
class DiffRecord:
    relpath: str
    content_state: ContentState
    metadata_state: MetadataState
    metadata_diff: tuple[str, ...]
    metadata_details: tuple[str, ...] = ()
    metadata_source: str | None = None
    local_size: int | None = None
    remote_size: int | None = None
