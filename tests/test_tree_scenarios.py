from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from limsync.compare import compare_records
from limsync.models import ContentState, MetadataState
from limsync.planner_apply import ACTION_SUGGESTED, build_plan_operations
from limsync.scanner_local import LocalScanner

from conftest import mk_file


@dataclass(frozen=True)
class ScenarioResult:
    left: int
    right: int
    conflicts: int
    metadata: int
    suggested_actions: list[tuple[str, str]]


@pytest.fixture
def ts_2026_02_19_midnight_ns() -> int:
    return int(datetime(2026, 2, 19, 0, 0, tzinfo=UTC).timestamp() * 1_000_000_000)


@pytest.fixture
def build_tree():
    def _build(
        entries: list[tuple[str, int, int, int]],
    ) -> dict[str, object]:
        # entry: (relpath, size, mtime_ns, mode)
        return {
            relpath: mk_file(
                relpath,
                size=size,
                mtime_ns=mtime_ns,
                mode=mode,
            )
            for relpath, size, mtime_ns, mode in entries
        }

    return _build


def analyze_scenario(
    left_tree: dict[str, object],
    right_tree: dict[str, object],
) -> ScenarioResult:
    diffs = compare_records(left_tree, right_tree, mtime_tolerance_ns=0)
    suggested_overrides = {diff.relpath: ACTION_SUGGESTED for diff in diffs}
    suggested_ops = build_plan_operations(diffs, suggested_overrides)

    return ScenarioResult(
        left=sum(1 for diff in diffs if diff.content_state == ContentState.ONLY_LOCAL),
        right=sum(1 for diff in diffs if diff.content_state == ContentState.ONLY_REMOTE),
        conflicts=sum(
            1 for diff in diffs if diff.content_state == ContentState.DIFFERENT
        ),
        metadata=sum(
            1
            for diff in diffs
            if diff.content_state == ContentState.IDENTICAL
            and diff.metadata_state == MetadataState.DIFFERENT
        ),
        suggested_actions=sorted((op.kind, op.relpath) for op in suggested_ops),
    )


def test_counts_and_suggested_actions_for_two_one_sided_files(
    build_tree, ts_2026_02_19_midnight_ns
) -> None:
    left_tree = build_tree(
        [
            ("a.txt", 123, ts_2026_02_19_midnight_ns, 0o644),
        ]
    )
    right_tree = build_tree(
        [
            ("b.txt", 234, ts_2026_02_19_midnight_ns, 0o644),
        ]
    )

    result = analyze_scenario(left_tree, right_tree)

    assert result.left == 1
    assert result.right == 1
    assert result.conflicts == 0
    assert result.metadata == 0
    assert result.suggested_actions == [
        ("copy_left", "b.txt"),
        ("copy_right", "a.txt"),
    ]


def test_counts_and_suggested_actions_for_metadata_only_mode_drift(
    build_tree, ts_2026_02_19_midnight_ns
) -> None:
    left_tree = build_tree(
        [
            ("x.txt", 100, ts_2026_02_19_midnight_ns, 0o777),
        ]
    )
    right_tree = build_tree(
        [
            ("x.txt", 100, ts_2026_02_19_midnight_ns, 0o600),
        ]
    )

    result = analyze_scenario(left_tree, right_tree)

    assert result.left == 0
    assert result.right == 0
    assert result.conflicts == 0
    assert result.metadata == 1
    assert result.suggested_actions == [("metadata_update_left", "x.txt")]


def test_counts_and_suggested_actions_for_content_conflict(
    build_tree, ts_2026_02_19_midnight_ns
) -> None:
    left_tree = build_tree(
        [
            ("x.txt", 100, ts_2026_02_19_midnight_ns, 0o644),
        ]
    )
    right_tree = build_tree(
        [
            ("x.txt", 101, ts_2026_02_19_midnight_ns, 0o644),
        ]
    )

    result = analyze_scenario(left_tree, right_tree)

    assert result.left == 0
    assert result.right == 0
    assert result.conflicts == 1
    assert result.metadata == 0
    assert result.suggested_actions == []


def test_asymmetric_dropboxignore_between_trees(
    tmp_path, ts_2026_02_19_midnight_ns
) -> None:
    left_root = tmp_path / "left"
    right_root = tmp_path / "right"
    left_root.mkdir()
    right_root.mkdir()

    (left_root / "a.txt").write_text("same-a\n", encoding="utf-8")
    (right_root / "a.txt").write_text("same-a\n", encoding="utf-8")

    (left_root / "b.txt").write_text("same-b\n", encoding="utf-8")
    (right_root / "b.txt").write_text("same-b\n", encoding="utf-8")

    (left_root / "c.txt").write_text("same-c\n", encoding="utf-8")
    (right_root / "c.txt").write_text("same-c\n", encoding="utf-8")

    (left_root / ".dropboxignore").write_text("b.txt\n", encoding="utf-8")
    (right_root / ".dropboxignore").write_text("c.txt\n# extra\n", encoding="utf-8")

    os.utime(
        left_root / "a.txt",
        ns=(ts_2026_02_19_midnight_ns, ts_2026_02_19_midnight_ns),
    )
    os.utime(
        right_root / "a.txt",
        ns=(ts_2026_02_19_midnight_ns, ts_2026_02_19_midnight_ns),
    )

    left_records = LocalScanner(left_root).scan()
    right_records = LocalScanner(right_root).scan()
    diffs = compare_records(left_records, right_records, mtime_tolerance_ns=0)
    by = {diff.relpath: diff for diff in diffs}

    assert by["a.txt"].content_state == ContentState.IDENTICAL
    assert by[".dropboxignore"].content_state == ContentState.DIFFERENT
    assert by["c.txt"].content_state == ContentState.ONLY_LOCAL
    assert by["b.txt"].content_state == ContentState.ONLY_REMOTE

    suggested_overrides = {diff.relpath: ACTION_SUGGESTED for diff in diffs}
    suggested_ops = build_plan_operations(diffs, suggested_overrides)
    suggested_actions = sorted((op.kind, op.relpath) for op in suggested_ops)

    assert suggested_actions == [
        ("copy_left", "b.txt"),
        ("copy_right", "c.txt"),
    ]
