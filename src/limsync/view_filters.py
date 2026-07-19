from __future__ import annotations

from collections import Counter
from enum import Enum

from .deletion_intent import DELETED_ON_LEFT, DELETED_ON_RIGHT
from .models import ContentState, DiffRecord, MetadataState


class ViewFilter(str, Enum):
    LEFT_TO_RIGHT = "left_to_right"
    RIGHT_TO_LEFT = "right_to_left"
    CONFLICTS = "conflicts"
    UNCERTAIN = "uncertain"
    METADATA = "metadata"
    DELETE_CANDIDATES = "delete_candidates"


VIEW_FILTER_ORDER = (
    ViewFilter.LEFT_TO_RIGHT,
    ViewFilter.RIGHT_TO_LEFT,
    ViewFilter.CONFLICTS,
    ViewFilter.UNCERTAIN,
    ViewFilter.METADATA,
    ViewFilter.DELETE_CANDIDATES,
)
ALL_VIEW_FILTERS = frozenset(VIEW_FILTER_ORDER)
VIEW_FILTER_LABELS = {
    ViewFilter.LEFT_TO_RIGHT: "Left -> Right",
    ViewFilter.RIGHT_TO_LEFT: "Right -> Left",
    ViewFilter.CONFLICTS: "Conflicts",
    ViewFilter.UNCERTAIN: "Uncertain",
    ViewFilter.METADATA: "Metadata",
    ViewFilter.DELETE_CANDIDATES: "Delete candidates",
}


def classify_diff_for_view(diff: DiffRecord) -> ViewFilter | None:
    if diff.content_state == ContentState.ONLY_LEFT:
        if diff.metadata_source == DELETED_ON_RIGHT:
            return ViewFilter.DELETE_CANDIDATES
        return ViewFilter.LEFT_TO_RIGHT
    if diff.content_state == ContentState.ONLY_RIGHT:
        if diff.metadata_source == DELETED_ON_LEFT:
            return ViewFilter.DELETE_CANDIDATES
        return ViewFilter.RIGHT_TO_LEFT
    if diff.content_state == ContentState.DIFFERENT:
        return ViewFilter.CONFLICTS
    if diff.content_state == ContentState.UNKNOWN:
        return ViewFilter.UNCERTAIN
    if (
        diff.content_state == ContentState.IDENTICAL
        and diff.metadata_state == MetadataState.DIFFERENT
    ):
        return ViewFilter.METADATA
    return None


def count_view_filters(diffs: list[DiffRecord]) -> dict[ViewFilter, int]:
    counts = Counter(
        category
        for diff in diffs
        if (category := classify_diff_for_view(diff)) is not None
    )
    return {category: counts[category] for category in VIEW_FILTER_ORDER}
