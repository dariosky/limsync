from __future__ import annotations

from limsync.deletion_intent import (
    DELETED_ON_LEFT,
    DELETED_ON_RIGHT,
    apply_intentional_deletion_hints,
)
from limsync.models import ContentState

from conftest import mk_diff


def test_apply_intentional_deletion_hints_marks_one_sided_entries() -> None:
    diffs = [
        mk_diff("gone_on_left.txt", content_state=ContentState.ONLY_RIGHT),
        mk_diff("gone_on_right.txt", content_state=ContentState.ONLY_LEFT),
        mk_diff("same.txt", content_state=ContentState.IDENTICAL),
        mk_diff("new_on_left.txt", content_state=ContentState.ONLY_LEFT),
        mk_diff("new_on_right.txt", content_state=ContentState.ONLY_RIGHT),
    ]
    previous_content_states = {
        "gone_on_left.txt": ContentState.IDENTICAL,
        "gone_on_right.txt": ContentState.DIFFERENT,
        "same.txt": ContentState.IDENTICAL,
        "new_on_left.txt": ContentState.ONLY_LEFT,
        "new_on_right.txt": ContentState.ONLY_RIGHT,
    }

    updated = apply_intentional_deletion_hints(diffs, previous_content_states)
    by = {diff.relpath: diff for diff in updated}

    assert by["gone_on_left.txt"].metadata_source == DELETED_ON_LEFT
    assert by["gone_on_right.txt"].metadata_source == DELETED_ON_RIGHT
    assert by["same.txt"].metadata_source is None
    assert by["new_on_left.txt"].metadata_source is None
    assert by["new_on_right.txt"].metadata_source is None
