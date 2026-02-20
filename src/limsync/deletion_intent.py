from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from .models import ContentState, DiffRecord

DELETED_ON_LEFT = "deleted_on_left"
DELETED_ON_RIGHT = "deleted_on_right"


def _was_present_on_both_sides(state: ContentState | str | None) -> bool:
    if state is None:
        return False
    value = state.value if isinstance(state, ContentState) else str(state)
    return value in {
        ContentState.IDENTICAL.value,
        ContentState.DIFFERENT.value,
        ContentState.UNKNOWN.value,
    }


def apply_intentional_deletion_hints(
    diffs: list[DiffRecord],
    previous_content_states: Mapping[str, ContentState | str],
) -> list[DiffRecord]:
    updated: list[DiffRecord] = []
    for diff in diffs:
        previous_state = previous_content_states.get(diff.relpath)
        if not _was_present_on_both_sides(previous_state):
            updated.append(diff)
            continue

        if diff.content_state == ContentState.ONLY_RIGHT:
            updated.append(replace(diff, metadata_source=DELETED_ON_LEFT))
            continue
        if diff.content_state == ContentState.ONLY_LEFT:
            updated.append(replace(diff, metadata_source=DELETED_ON_RIGHT))
            continue
        updated.append(diff)
    return updated
