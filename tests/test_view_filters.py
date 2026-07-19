from limsync.deletion_intent import DELETED_ON_LEFT, DELETED_ON_RIGHT
from limsync.models import ContentState, MetadataState
from limsync.view_filters import (
    ViewFilter,
    classify_diff_for_view,
    count_view_filters,
)

from conftest import mk_diff


def test_view_filter_categories_partition_changed_diffs() -> None:
    diffs = [
        mk_diff("copy-right", content_state=ContentState.ONLY_LEFT),
        mk_diff("copy-left", content_state=ContentState.ONLY_RIGHT),
        mk_diff("conflict", content_state=ContentState.DIFFERENT),
        mk_diff("uncertain", content_state=ContentState.UNKNOWN),
        mk_diff(
            "metadata",
            content_state=ContentState.IDENTICAL,
            metadata_state=MetadataState.DIFFERENT,
        ),
        mk_diff(
            "delete-left",
            content_state=ContentState.ONLY_LEFT,
            metadata_source=DELETED_ON_RIGHT,
        ),
        mk_diff(
            "delete-right",
            content_state=ContentState.ONLY_RIGHT,
            metadata_source=DELETED_ON_LEFT,
        ),
        mk_diff("identical", content_state=ContentState.IDENTICAL),
    ]

    assert [classify_diff_for_view(diff) for diff in diffs] == [
        ViewFilter.LEFT_TO_RIGHT,
        ViewFilter.RIGHT_TO_LEFT,
        ViewFilter.CONFLICTS,
        ViewFilter.UNCERTAIN,
        ViewFilter.METADATA,
        ViewFilter.DELETE_CANDIDATES,
        ViewFilter.DELETE_CANDIDATES,
        None,
    ]

    assert count_view_filters(diffs) == {
        ViewFilter.LEFT_TO_RIGHT: 1,
        ViewFilter.RIGHT_TO_LEFT: 1,
        ViewFilter.CONFLICTS: 1,
        ViewFilter.UNCERTAIN: 1,
        ViewFilter.METADATA: 1,
        ViewFilter.DELETE_CANDIDATES: 2,
    }


def test_metadata_filter_is_only_for_metadata_only_drift() -> None:
    conflict_with_metadata = mk_diff(
        "conflict",
        content_state=ContentState.DIFFERENT,
        metadata_state=MetadataState.DIFFERENT,
    )
    uncertain_with_metadata = mk_diff(
        "uncertain",
        content_state=ContentState.UNKNOWN,
        metadata_state=MetadataState.DIFFERENT,
    )

    assert classify_diff_for_view(conflict_with_metadata) == ViewFilter.CONFLICTS
    assert classify_diff_for_view(uncertain_with_metadata) == ViewFilter.UNCERTAIN
