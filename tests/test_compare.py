from __future__ import annotations

from limsync.compare import compare_records
from limsync.models import ContentState, MetadataState, NodeType

from conftest import mk_file


def _by_path(diffs):
    return {d.relpath: d for d in diffs}


def test_union_and_sorted_paths() -> None:
    local = {
        "b.txt": mk_file("b.txt"),
        "a.txt": mk_file("a.txt"),
    }
    remote = {
        "c.txt": mk_file("c.txt"),
    }

    diffs = compare_records(local, remote)
    assert [d.relpath for d in diffs] == ["a.txt", "b.txt", "c.txt"]


def test_only_left_and_only_right_states() -> None:
    local = {"left.txt": mk_file("left.txt", size=11)}
    remote = {"right.txt": mk_file("right.txt", size=22)}

    by = _by_path(compare_records(local, remote))

    assert by["left.txt"].content_state == ContentState.ONLY_LEFT
    assert by["left.txt"].metadata_state == MetadataState.NOT_APPLICABLE
    assert by["left.txt"].left_size == 11
    assert by["left.txt"].right_size is None

    assert by["right.txt"].content_state == ContentState.ONLY_RIGHT
    assert by["right.txt"].metadata_state == MetadataState.NOT_APPLICABLE
    assert by["right.txt"].left_size is None
    assert by["right.txt"].right_size == 22


def test_type_mismatch_is_different() -> None:
    local = {"x": mk_file("x", node_type=NodeType.FILE, size=1)}
    remote = {"x": mk_file("x", node_type=NodeType.DIR, size=0)}

    diff = compare_records(local, remote)[0]

    assert diff.content_state == ContentState.DIFFERENT
    assert diff.metadata_state == MetadataState.DIFFERENT
    assert diff.metadata_diff == ("type",)


def test_file_content_state_identical_unknown_different() -> None:
    local = {
        "same.txt": mk_file("same.txt", size=100, mtime_ns=1_000_000_000),
        "uncertain.txt": mk_file("uncertain.txt", size=100, mtime_ns=1_000_000_000),
        "different.txt": mk_file("different.txt", size=100, mtime_ns=1_000_000_000),
    }
    remote = {
        "same.txt": mk_file("same.txt", size=100, mtime_ns=2_000_000_000),
        "uncertain.txt": mk_file("uncertain.txt", size=100, mtime_ns=4_500_000_000),
        "different.txt": mk_file("different.txt", size=101, mtime_ns=1_000_000_000),
    }

    by = _by_path(compare_records(local, remote, mtime_tolerance_ns=2_000_000_000))

    assert by["same.txt"].content_state == ContentState.IDENTICAL
    assert by["uncertain.txt"].content_state == ContentState.UNKNOWN
    assert by["different.txt"].content_state == ContentState.DIFFERENT


def test_metadata_diff_details_and_source_precedence_mode_then_mtime() -> None:
    local = {"x.txt": mk_file("x.txt", mode=0o777, mtime_ns=10_000_000_000)}
    remote = {"x.txt": mk_file("x.txt", mode=0o600, mtime_ns=20_000_000_000)}

    diff = compare_records(local, remote, mtime_tolerance_ns=0)[0]

    assert diff.metadata_state == MetadataState.DIFFERENT
    assert "mode" in diff.metadata_diff
    assert "mtime" in diff.metadata_diff
    assert any("left=0x777 right=0x600" in detail for detail in diff.metadata_details)
    assert diff.metadata_source == "right"


def test_metadata_source_from_older_mtime_when_mode_same() -> None:
    local = {"x.txt": mk_file("x.txt", mode=0o644, mtime_ns=10_000_000_000)}
    remote = {"x.txt": mk_file("x.txt", mode=0o644, mtime_ns=20_000_000_000)}

    diff = compare_records(local, remote, mtime_tolerance_ns=0)[0]
    assert diff.metadata_state == MetadataState.DIFFERENT
    assert diff.metadata_diff == ("mtime",)
    assert diff.metadata_source == "left"


def test_directory_metadata_can_drift_but_content_identical() -> None:
    local = {"d": mk_file("d", node_type=NodeType.DIR, mode=0o755, mtime_ns=1)}
    remote = {"d": mk_file("d", node_type=NodeType.DIR, mode=0o700, mtime_ns=10)}

    diff = compare_records(local, remote, mtime_tolerance_ns=0)[0]
    assert diff.content_state == ContentState.IDENTICAL
    assert diff.metadata_state == MetadataState.DIFFERENT


def test_symlink_content_uses_normalized_target_key() -> None:
    local = {
        "l": mk_file(
            "l",
            node_type=NodeType.SYMLINK,
            mode=0o777,
            mtime_ns=1,
            size=3,
            link_target="/Users/dario/Dropbox/docs/readme.md",
            link_target_key="inroot:docs/readme.md",
        )
    }
    remote = {
        "l": mk_file(
            "l",
            node_type=NodeType.SYMLINK,
            mode=0o600,
            mtime_ns=10,
            size=3,
            link_target="../docs/readme.md",
            link_target_key="inroot:docs/readme.md",
        )
    }

    diff = compare_records(local, remote, mtime_tolerance_ns=0)[0]
    assert diff.content_state == ContentState.IDENTICAL
    assert diff.metadata_state == MetadataState.NOT_APPLICABLE
    assert diff.metadata_diff == ()
    assert diff.metadata_source is None


def test_symlink_with_different_target_is_different_content() -> None:
    local = {
        "l": mk_file(
            "l",
            node_type=NodeType.SYMLINK,
            link_target="docs/a.txt",
            link_target_key="inroot:docs/a.txt",
        )
    }
    remote = {
        "l": mk_file(
            "l",
            node_type=NodeType.SYMLINK,
            link_target="docs/b.txt",
            link_target_key="inroot:docs/b.txt",
        )
    }

    diff = compare_records(local, remote, mtime_tolerance_ns=0)[0]
    assert diff.content_state == ContentState.DIFFERENT
    assert diff.metadata_state == MetadataState.NOT_APPLICABLE
