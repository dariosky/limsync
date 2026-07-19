from __future__ import annotations

import asyncio

from textual.app import App
from textual.widgets import Button, Static

from limsync.endpoints import EndpointSpec
from limsync.modals import ApplyRunModal
from limsync.planner_apply import ExecuteResult, PlanOperation


class _ModalHost(App[None]):
    def __init__(self) -> None:
        super().__init__()
        self.modal_result: ExecuteResult | None = None

    def on_mount(self) -> None:
        self.push_screen(
            ApplyRunModal(
                source_endpoint=EndpointSpec("local", "/left"),
                destination_endpoint=EndpointSpec("local", "/right"),
                operations=[PlanOperation("copy_right", "x")],
            ),
            callback=self._capture_result,
        )

    def _capture_result(self, result: ExecuteResult | None) -> None:
        self.modal_result = result


def test_apply_modal_cancel_waits_for_executor_and_keeps_result(monkeypatch) -> None:
    observed_cancel_events = []

    def fake_execute_plan(
        source,
        destination,
        operations,
        progress_cb,
        settings,
        cancel_event,
    ):
        _ = (source, destination, operations, progress_cb, settings)
        observed_cancel_events.append(cancel_event)
        assert cancel_event.wait(timeout=2)
        return ExecuteResult(
            completed_paths=set(),
            errors=[],
            succeeded_operations=0,
            total_operations=1,
            cancelled=True,
        )

    monkeypatch.setattr("limsync.modals.execute_plan", fake_execute_plan)
    app = _ModalHost()

    async def exercise() -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            modal = app.screen
            cancel_button = modal.query_one("#cancel-apply", Button)
            close_button = modal.query_one("#close", Button)
            progress = modal.query_one("#apply-progress")
            bar = progress.query_one("#bar")
            percentage = progress.query_one("#percentage")
            assert cancel_button.disabled is False
            assert close_button.disabled is True
            assert bar.size.width + percentage.size.width == progress.size.width

            await pilot.click("#cancel-apply")
            await pilot.pause()

            assert observed_cancel_events[0].is_set()
            assert cancel_button.disabled is True
            assert close_button.disabled is False
            assert "Cancelled after 0/1 operations" in str(
                modal.query_one("#apply-status", Static).render()
            )

            await pilot.press("enter")
            await pilot.pause(0.1)
            assert app.modal_result is not None
            assert app.modal_result.cancelled is True

    asyncio.run(exercise())
