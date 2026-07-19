import asyncio
from pathlib import Path

from limsync.endpoints import EndpointSpec
from limsync.models import ContentState, MetadataState
from limsync.planner_apply import ACTION_SUGGESTED, build_plan_operations
from limsync.review_tui import ReviewApp
from limsync.state_db import (
    ScanStateSummary,
    save_current_state,
    upsert_action_overrides,
)

from conftest import mk_diff


def _summary() -> ScanStateSummary:
    return ScanStateSummary(
        source_endpoint="local:/left",
        destination_endpoint="local:/right",
        source_scan_seconds=0,
        destination_scan_seconds=0,
        source_files=2,
        destination_files=2,
        compared_paths=2,
        only_source=1,
        only_destination=0,
        different_content=0,
        uncertain=0,
        metadata_only=1,
    )


def test_filter_modal_applies_and_cancels_without_changing_plan(tmp_path: Path) -> None:
    db_path = tmp_path / "review.sqlite3"
    diffs = [
        mk_diff(
            "docs/left.txt",
            content_state=ContentState.ONLY_LEFT,
            metadata_state=MetadataState.NOT_APPLICABLE,
        ),
        mk_diff(
            "docs/meta.txt",
            content_state=ContentState.IDENTICAL,
            metadata_state=MetadataState.DIFFERENT,
            metadata_source="left",
        ),
    ]
    save_current_state(db_path, _summary(), diffs)
    upsert_action_overrides(
        db_path,
        {diff.relpath: ACTION_SUGGESTED for diff in diffs},
    )
    app = ReviewApp(
        db_path,
        EndpointSpec("local", "/left"),
        EndpointSpec("local", "/right"),
        hide_identical=True,
    )

    async def exercise() -> None:
        async with app.run_test() as pilot:
            await pilot.press("f")
            await pilot.press("space")  # Hide Left -> Right.
            await pilot.press("enter")
            await pilot.pause()

            assert app.visible_changed_relpaths == {"docs/meta.txt"}
            assert app._selected_target_files() == ["docs/meta.txt"]
            _selected, delete_ops = app._delete_ops_for_selected()
            assert {op.relpath for op in delete_ops} == {"docs/meta.txt"}
            assert len(build_plan_operations(app.diffs, app.action_overrides)) == 2

            await pilot.press("f")
            await pilot.press("space")  # Tentatively restore Left -> Right.
            await pilot.press("escape")
            await pilot.pause()

            assert app.visible_changed_relpaths == {"docs/meta.txt"}

    asyncio.run(exercise())
