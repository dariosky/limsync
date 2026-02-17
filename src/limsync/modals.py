from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from pathlib import Path, PurePosixPath

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, ProgressBar, Static, Tree

from .planner_apply import ApplySettings, ExecuteResult, PlanOperation, execute_plan


def _op_label(kind: str) -> str:
    if kind == "copy_right":
        return "copy local -> remote"
    if kind == "copy_left":
        return "copy remote -> local"
    if kind == "delete_right":
        return "delete remote"
    if kind == "delete_left":
        return "delete local"
    if kind == "metadata_update_right":
        return "copy metadata from local"
    if kind == "metadata_update_left":
        return "copy metadata from remote"
    return kind


class ConfirmApplyModal(ModalScreen[bool]):
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("enter", "activate_focused", "Confirm"),
        ("left", "focus_prev_button", "Prev"),
        ("right", "focus_next_button", "Next"),
        ("a", "confirm", "Apply"),
        ("c", "cancel", "Cancel"),
    ]
    CSS = """
    Screen {
        background: $background 70%;
    }
    #confirm-root {
        width: 100%;
        height: 100%;
        align: center middle;
    }
    #confirm-box {
        width: 70;
        height: auto;
        border: round #666666;
        padding: 1 2;
    }
    #confirm-buttons {
        width: 100%;
        height: auto;
        align: center middle;
    }
    """

    def __init__(self, total_operations: int) -> None:
        super().__init__()
        self.total_operations = total_operations

    def compose(self) -> ComposeResult:
        with Container(id="confirm-root"):
            with Vertical(id="confirm-box"):
                yield Static(
                    f"Apply {self.total_operations} planned operations?\nThis cannot be automatically rolled back."
                )
                with Horizontal(id="confirm-buttons"):
                    yield Button("Cancel [C]", id="cancel")
                    yield Button("Apply [A]", id="apply")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "apply":
            self.dismiss(True)
        else:
            self.dismiss(False)

    def on_mount(self) -> None:
        self.query_one("#apply", Button).focus()

    def action_cancel(self) -> None:
        self.dismiss(False)

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_focus_prev_button(self) -> None:
        apply_btn = self.query_one("#apply", Button)
        cancel_btn = self.query_one("#cancel", Button)
        if apply_btn.has_focus:
            cancel_btn.focus()
        else:
            apply_btn.focus()

    def action_focus_next_button(self) -> None:
        self.action_focus_prev_button()

    def action_activate_focused(self) -> None:
        apply_btn = self.query_one("#apply", Button)
        cancel_btn = self.query_one("#cancel", Button)
        if apply_btn.has_focus:
            self.dismiss(True)
            return
        if cancel_btn.has_focus:
            self.dismiss(False)
            return
        # Safe default if focus is elsewhere.
        self.dismiss(False)


class ApplyRunModal(ModalScreen[ExecuteResult | None]):
    BINDINGS = [
        ("escape", "close_if_done", "Close"),
        ("enter", "close_if_done", "Close"),
        ("c", "close_if_done", "Close"),
        ("q", "close_if_done", "Close"),
    ]
    CSS = """
    Screen {
        background: $background 70%;
    }
    #apply-root {
        width: 100%;
        height: 100%;
        align: center middle;
    }
    #apply-box {
        width: 110;
        height: auto;
        max-height: 90%;
        border: round #666666;
        padding: 1;
    }
    #apply-progress {
        width: 100%;
    }
    #errors {
        height: 12;
        border: round #444444;
        padding: 1;
    }
    #apply-close-row {
        width: 100%;
        height: auto;
        align: center middle;
    }
    """

    def __init__(
        self,
        local_root: Path,
        remote_address: str,
        operations: list,
        apply_settings: ApplySettings | None = None,
        progress_event_cb: Callable[[int, int, object, bool, str | None], None]
        | None = None,
    ) -> None:
        super().__init__()
        self.local_root = local_root
        self.remote_address = remote_address
        self.operations = operations
        self.apply_settings = apply_settings or ApplySettings()
        self.progress_event_cb = progress_event_cb
        self.result: ExecuteResult | None = None

    def compose(self) -> ComposeResult:
        with Container(id="apply-root"):
            with Vertical(id="apply-box"):
                yield Static("Applying plan...", id="apply-status")
                yield ProgressBar(
                    total=len(self.operations), show_eta=False, id="apply-progress"
                )
                yield Static("Errors:\n-", id="errors")
                with Container(id="apply-close-row"):
                    yield Button("Running...", id="close", disabled=True)

    def on_mount(self) -> None:
        self.run_worker(self._run_apply(), exclusive=True)

    async def _run_apply(self) -> None:
        last_emit = 0.0
        pending_ops = 0

        def progress_cb(done: int, total: int, op, ok: bool, error: str | None) -> None:
            nonlocal last_emit, pending_ops
            pending_ops += 1
            now = time.monotonic()
            emit = (
                done == total
                or not ok
                or pending_ops >= self.apply_settings.progress_emit_every_ops
                or (now - last_emit) * 1000.0
                >= self.apply_settings.progress_emit_every_ms
            )
            if not emit:
                return
            pending_ops = 0
            last_emit = now
            self.app.call_from_thread(self._on_progress, done, total, op, ok, error)

        try:
            result = await asyncio.to_thread(
                execute_plan,
                self.local_root,
                self.remote_address,
                self.operations,
                progress_cb,
                self.apply_settings,
            )
            self.result = result
            top_costly = sorted(
                result.operation_seconds.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:3]
            timing_suffix = ""
            if top_costly:
                timing_suffix = "  slowest: " + ", ".join(
                    f"{kind}={seconds:.2f}s ({result.operation_counts.get(kind, 0)})"
                    for kind, seconds in top_costly
                )
            self.query_one("#apply-status", Static).update(
                f"Completed {result.succeeded_operations}/{result.total_operations} operations.{timing_suffix}"
            )
            if result.errors:
                error_text = "Errors:\n" + "\n".join(result.errors[:100])
            else:
                error_text = "Errors:\n-"
            self.query_one("#errors", Static).update(error_text)
        except Exception as exc:  # noqa: BLE001
            self.result = ExecuteResult(
                completed_paths=set(),
                errors=[f"fatal: {exc}"],
                succeeded_operations=0,
                total_operations=len(self.operations),
            )
            self.query_one("#apply-status", Static).update("Apply failed.")
            self.query_one("#errors", Static).update(f"Errors:\n{exc}")

        close_btn = self.query_one("#close", Button)
        close_btn.disabled = False
        close_btn.label = "Close"
        close_btn.focus()

    def _on_progress(
        self, done: int, total: int, op, ok: bool, error: str | None
    ) -> None:
        bar = self.query_one("#apply-progress", ProgressBar)
        bar.update(total=total, progress=done)
        label = _op_label(op.kind)
        status = f"[{done}/{total}] {label}: {op.relpath}"
        if not ok and error:
            status += f"  (error: {error})"
        self.query_one("#apply-status", Static).update(status)
        if self.progress_event_cb is not None:
            self.progress_event_cb(done, total, op, ok, error)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close" and not event.button.disabled:
            self.dismiss(self.result)

    def action_close_if_done(self) -> None:
        close_btn = self.query_one("#close", Button)
        if close_btn.disabled:
            return
        self.dismiss(self.result)


class OpenSideModal(ModalScreen[str | None]):
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("enter", "activate_focused", "Confirm"),
        ("left", "focus_prev_button", "Prev"),
        ("right", "focus_next_button", "Next"),
        ("l", "open_left", "Open Left"),
        ("r", "open_right", "Open Right"),
        ("c", "cancel", "Cancel"),
    ]
    CSS = """
    Screen {
        background: $background 70%;
    }
    #open-side-root {
        width: 100%;
        height: 100%;
        align: center middle;
    }
    #open-side-box {
        width: 72;
        height: auto;
        border: round #666666;
        padding: 1 2;
    }
    #open-side-buttons {
        height: auto;
    }
    """

    def __init__(self, relpath: str) -> None:
        super().__init__()
        self.relpath = relpath

    def compose(self) -> ComposeResult:
        with Container(id="open-side-root"):
            with Vertical(id="open-side-box"):
                yield Static(f"Open which side?\n{self.relpath}")
                with Horizontal(id="open-side-buttons"):
                    yield Button("Cancel [C]", id="cancel")
                    yield Button("Open Left [L]", id="left")
                    yield Button("Open Right [R]", id="right")

    def on_mount(self) -> None:
        self.query_one("#left", Button).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "left":
            self.dismiss("left")
        elif event.button.id == "right":
            self.dismiss("right")
        else:
            self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)

    def action_open_left(self) -> None:
        self.dismiss("left")

    def action_open_right(self) -> None:
        self.dismiss("right")

    def action_focus_prev_button(self) -> None:
        left_btn = self.query_one("#left", Button)
        right_btn = self.query_one("#right", Button)
        cancel_btn = self.query_one("#cancel", Button)
        if left_btn.has_focus:
            cancel_btn.focus()
        elif right_btn.has_focus:
            left_btn.focus()
        else:
            right_btn.focus()

    def action_focus_next_button(self) -> None:
        left_btn = self.query_one("#left", Button)
        right_btn = self.query_one("#right", Button)
        cancel_btn = self.query_one("#cancel", Button)
        if left_btn.has_focus:
            right_btn.focus()
        elif right_btn.has_focus:
            cancel_btn.focus()
        else:
            left_btn.focus()

    def action_activate_focused(self) -> None:
        left_btn = self.query_one("#left", Button)
        right_btn = self.query_one("#right", Button)
        if left_btn.has_focus:
            self.dismiss("left")
            return
        if right_btn.has_focus:
            self.dismiss("right")
            return
        self.dismiss(None)


class FileDiffModal(ModalScreen[None]):
    BINDINGS = [
        ("escape", "close", "Close"),
        ("enter", "close", "Close"),
        ("c", "close", "Close"),
        ("q", "close", "Close"),
    ]
    CSS = """
    Screen {
        background: $background 70%;
    }
    #diff-root {
        width: 100%;
        height: 100%;
        align: center middle;
    }
    #diff-box {
        width: 140;
        height: 90%;
        border: round #666666;
        padding: 1;
    }
    #diff-title {
        height: auto;
    }
    #diff-content {
        height: 1fr;
        border: round #444444;
        padding: 1;
    }
    #diff-close-row {
        width: 100%;
        height: auto;
        align: center middle;
    }
    """

    def __init__(self, relpath: str, diff_text: str) -> None:
        super().__init__()
        self.relpath = relpath
        self.diff_text = diff_text

    def compose(self) -> ComposeResult:
        with Container(id="diff-root"):
            with Vertical(id="diff-box"):
                yield Static(f"Diff: local vs remote\n{self.relpath}", id="diff-title")
                yield Static(Text(self.diff_text), id="diff-content")
                with Container(id="diff-close-row"):
                    yield Button("Close", id="close")

    def on_mount(self) -> None:
        self.query_one("#close", Button).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close":
            self.dismiss(None)

    def action_close(self) -> None:
        self.dismiss(None)


class CommandsModal(ModalScreen[str | None]):
    BINDINGS = [
        ("escape", "close", "Close"),
        ("enter", "run_selected", "Run"),
        ("up", "cursor_up", "Up"),
        ("down", "cursor_down", "Down"),
        ("k", "cursor_up", "Up"),
        ("j", "cursor_down", "Down"),
        ("c", "close", "Close"),
        ("q", "close", "Close"),
    ]
    CSS = """
    Screen {
        background: $background 70%;
    }
    #commands-root {
        width: 100%;
        height: 100%;
        align: center middle;
    }
    #commands-box {
        width: 72;
        height: auto;
        border: round #666666;
        padding: 1 2;
    }
    #commands-list {
        border: round #444444;
        padding: 1;
    }
    #commands-help {
        height: auto;
        color: #999999;
    }
    """

    COMMANDS: list[tuple[str, str]] = [
        ("h", "toggle_hide_identical"),
        ("o", "open_selected"),
        ("U", "update_selected_path"),
        ("D", "delete_selected_both"),
        ("F", "diff_selected"),
        ("P", "copy_selected_path"),
        ("V", "view_plan"),
        ("I", "add_to_dropboxignore"),
        ("C", "clear_plan"),
        ("M", "apply_all_metadata_suggestions"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.selected_index = 0

    def _render_commands(self) -> str:
        rows: list[str] = ["Advanced Commands", ""]
        descriptions = {
            "h": "show/hide identical",
            "o": "open",
            "U": "rescan selected path",
            "D": "delete file/folder both sides",
            "F": "diff",
            "P": "copy path",
            "V": "view plan",
            "I": "add ignore rule",
            "C": "clear plan",
            "M": "add all meta suggestions",
        }
        for idx, (key, _action) in enumerate(self.COMMANDS):
            pointer = ">" if idx == self.selected_index else " "
            rows.append(f"{pointer} {idx + 1}. {key} - {descriptions[key]}")
        return "\n".join(rows)

    def compose(self) -> ComposeResult:
        with Container(id="commands-root"):
            with Vertical(id="commands-box"):
                yield Static("", id="commands-list")
                yield Static(
                    "Up/Down to select, Enter to run, Esc to close", id="commands-help"
                )

    def on_mount(self) -> None:
        self._refresh_list()

    def _refresh_list(self) -> None:
        self.query_one("#commands-list", Static).update(self._render_commands())

    def action_cursor_up(self) -> None:
        self.selected_index = (self.selected_index - 1) % len(self.COMMANDS)
        self._refresh_list()

    def action_cursor_down(self) -> None:
        self.selected_index = (self.selected_index + 1) % len(self.COMMANDS)
        self._refresh_list()

    def action_run_selected(self) -> None:
        _key, action_name = self.COMMANDS[self.selected_index]
        self.dismiss(action_name)

    def action_close(self) -> None:
        self.dismiss(None)


class PlanTreeModal(ModalScreen[None]):
    BINDINGS = [
        ("escape", "close", "Close"),
        ("c", "close", "Close"),
        ("q", "close", "Close"),
    ]
    CSS = """
    Screen {
        background: $background 70%;
    }
    #plan-tree-root {
        width: 100%;
        height: 100%;
        align: center middle;
    }
    #plan-tree-box {
        width: 130;
        height: 90%;
        border: round #666666;
        padding: 1;
    }
    #plan-tree-title {
        height: auto;
    }
    #plan-tree {
        height: 1fr;
        border: round #444444;
    }
    #plan-tree-help {
        height: auto;
        color: #999999;
    }
    """

    def __init__(self, operations: list[PlanOperation]) -> None:
        super().__init__()
        self.operations = operations

    def compose(self) -> ComposeResult:
        with Container(id="plan-tree-root"):
            with Vertical(id="plan-tree-box"):
                yield Static("Current Plan", id="plan-tree-title")
                yield Tree("Plan", id="plan-tree")
                yield Static("Arrows to navigate, Esc to close", id="plan-tree-help")

    def _kind_label(self, kind: str) -> str:
        labels = {
            "copy_right": "copy left to right",
            "copy_left": "copy right to left",
            "metadata_update_right": "copy metadata left to right",
            "metadata_update_left": "copy metadata right to left",
            "delete_right": "delete on right",
            "delete_left": "delete on left",
        }
        return labels.get(kind, kind)

    def _build_trie(self, relpaths: list[str]) -> dict[str, dict]:
        root: dict[str, dict] = {}
        for relpath in sorted(set(relpaths)):
            node = root
            for part in PurePosixPath(relpath).parts:
                node = node.setdefault(part, {})
        return root

    def _populate_from_trie(self, tree_node, trie: dict[str, dict]) -> None:
        for name in sorted(trie):
            child_trie = trie[name]
            child = tree_node.add(name, allow_expand=bool(child_trie))
            if child_trie:
                self._populate_from_trie(child, child_trie)

    def on_mount(self) -> None:
        grouped: dict[str, list[str]] = {}
        for op in self.operations:
            grouped.setdefault(op.kind, []).append(op.relpath)

        tree = self.query_one("#plan-tree", Tree)
        tree.root.remove_children()
        tree.root.set_label("Plan")
        tree.root.expand()

        if not grouped:
            tree.root.add("no operations planned", allow_expand=False)
            tree.focus()
            return

        kind_order = [
            "copy_right",
            "copy_left",
            "metadata_update_right",
            "metadata_update_left",
            "delete_right",
            "delete_left",
        ]
        for kind in kind_order:
            if kind not in grouped:
                continue
            kind_node = tree.root.add(
                self._kind_label(kind),
                allow_expand=True,
            )
            trie = self._build_trie(grouped[kind])
            self._populate_from_trie(kind_node, trie)
            kind_node.expand()

        tree.focus()

    def action_close(self) -> None:
        self.dismiss(None)


class ConfirmDeleteModal(ModalScreen[bool]):
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("enter", "activate_focused", "Confirm"),
        ("left", "focus_prev_button", "Prev"),
        ("right", "focus_next_button", "Next"),
        ("d", "confirm", "Delete"),
        ("c", "cancel", "Cancel"),
    ]
    CSS = """
    Screen {
        background: $background 70%;
    }
    #delete-root {
        width: 100%;
        height: 100%;
        align: center middle;
    }
    #delete-box {
        width: 74;
        height: auto;
        border: round #666666;
        padding: 1 2;
    }
    #delete-buttons {
        height: auto;
    }
    """

    def __init__(self, selected_label: str, files_count: int) -> None:
        super().__init__()
        self.selected_label = selected_label
        self.files_count = files_count

    def compose(self) -> ComposeResult:
        lines = [
            "Delete selected item on both sides?",
            "",
            f"Target: {self.selected_label}",
            f"Files affected: {self.files_count}",
        ]
        with Container(id="delete-root"):
            with Vertical(id="delete-box"):
                yield Static("\n".join(lines))
                with Horizontal(id="delete-buttons"):
                    yield Button("Cancel [C]", id="cancel")
                    yield Button("DELETE [D]", id="delete", variant="error")

    def on_mount(self) -> None:
        self.query_one("#delete", Button).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "delete":
            self.dismiss(True)
        else:
            self.dismiss(False)

    def action_cancel(self) -> None:
        self.dismiss(False)

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_focus_prev_button(self) -> None:
        delete_btn = self.query_one("#delete", Button)
        cancel_btn = self.query_one("#cancel", Button)
        if delete_btn.has_focus:
            cancel_btn.focus()
        else:
            delete_btn.focus()

    def action_focus_next_button(self) -> None:
        self.action_focus_prev_button()

    def action_activate_focused(self) -> None:
        delete_btn = self.query_one("#delete", Button)
        cancel_btn = self.query_one("#cancel", Button)
        if delete_btn.has_focus:
            self.dismiss(True)
            return
        if cancel_btn.has_focus:
            self.dismiss(False)
            return
        self.dismiss(False)
