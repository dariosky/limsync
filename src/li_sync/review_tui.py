from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import BindingsMap
from textual.containers import Center, Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Footer, Header, ProgressBar, Static, Tree

from .models import ContentState, DiffRecord, MetadataState
from .planner_apply import (
    ACTION_IGNORE,
    ACTION_LEFT_WINS,
    ACTION_RIGHT_WINS,
    ACTION_SUGGESTED,
    ExecuteResult,
    PlanOperation,
    build_plan_operations,
    execute_plan,
    summarize_operations,
)
from .state_db import (
    load_action_overrides,
    load_current_diffs,
    mark_paths_identical,
    set_ui_pref,
    upsert_action_overrides,
)


@dataclass
class FolderCounts:
    only_local: int = 0
    only_remote: int = 0
    identical: int = 0
    metadata_only: int = 0
    different: int = 0
    uncertain: int = 0
    metadata_fields: dict[str, int] = field(default_factory=dict)


@dataclass
class FileEntry:
    relpath: str
    name: str
    content_state: str
    metadata_state: str
    metadata_diff: list[str]
    metadata_details: list[str]


@dataclass
class DirEntry:
    name: str
    relpath: str
    dirs: dict[str, DirEntry] = field(default_factory=dict)
    files: list[FileEntry] = field(default_factory=list)
    counts: FolderCounts = field(default_factory=FolderCounts)


def _file_counts(file_entry: FileEntry) -> FolderCounts:
    counts = FolderCounts()
    if file_entry.content_state == "only_local":
        counts.only_local = 1
    elif file_entry.content_state == "only_remote":
        counts.only_remote = 1
    elif file_entry.content_state == "different":
        counts.different = 1
    elif file_entry.content_state == "unknown":
        counts.uncertain = 1
    elif file_entry.content_state == "identical":
        if file_entry.metadata_state == "different":
            counts.metadata_only = 1
            for field_name in file_entry.metadata_diff:
                counts.metadata_fields[field_name] = (
                    counts.metadata_fields.get(field_name, 0) + 1
                )
        else:
            counts.identical = 1
    return counts


def _apply_counts(target: FolderCounts, increment: FolderCounts) -> None:
    target.only_local += increment.only_local
    target.only_remote += increment.only_remote
    target.identical += increment.identical
    target.metadata_only += increment.metadata_only
    target.different += increment.different
    target.uncertain += increment.uncertain
    for key, value in increment.metadata_fields.items():
        target.metadata_fields[key] = target.metadata_fields.get(key, 0) + value


def _is_identical_folder(entry: DirEntry) -> bool:
    c = entry.counts
    return (
        c.only_local == 0
        and c.only_remote == 0
        and c.metadata_only == 0
        and c.different == 0
        and c.uncertain == 0
    )


def _is_changed(entry: FileEntry) -> bool:
    return not (
        entry.content_state == "identical" and entry.metadata_state == "identical"
    )


def _folder_label(entry: DirEntry) -> Text:
    c = entry.counts
    extra = []
    if c.different:
        extra.append(f"D {c.different}")
    if c.uncertain:
        extra.append(f"U {c.uncertain}")
    summary = (
        f"L {c.only_local} | R {c.only_remote} | I {c.identical} | M {c.metadata_only}"
    )
    if extra:
        summary = f"{summary} | {' | '.join(extra)}"
    return Text.assemble((entry.name, "bold"), "  ", (summary, "cyan"))


def _file_label(file_entry: FileEntry) -> Text:
    if file_entry.content_state == "only_local":
        badge = "L"
    elif file_entry.content_state == "only_remote":
        badge = "R"
    elif file_entry.content_state == "different":
        badge = "D"
    elif file_entry.content_state == "unknown":
        badge = "U"
    elif (
        file_entry.content_state == "identical"
        and file_entry.metadata_state == "different"
    ):
        badge = "M"
    else:
        badge = "I"
    meta = ",".join(file_entry.metadata_diff) if file_entry.metadata_diff else "-"
    return Text.assemble(
        (file_entry.name, "white"), "  ", (f"[{badge}]", "yellow"), " ", (meta, "green")
    )


def _row_to_diff(row: dict[str, object]) -> DiffRecord:
    return DiffRecord(
        relpath=str(row["relpath"]),
        content_state=ContentState(str(row["content_state"])),
        metadata_state=MetadataState(str(row["metadata_state"])),
        metadata_diff=tuple(str(item) for item in row.get("metadata_diff", [])),
        metadata_details=tuple(str(item) for item in row.get("metadata_details", [])),
    )


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
        return "sync metadata on remote"
    if kind == "metadata_update_left":
        return "sync metadata on local"
    return kind


def _ops_text(kinds: list[str]) -> str:
    if not kinds:
        return "-"
    labels = [_op_label(kind) for kind in kinds]
    return ", ".join(labels)


def _ops_direction_marker(kinds: list[str]) -> str:
    if not kinds:
        return ""
    has_left = any(
        kind in {"copy_left", "metadata_update_left", "delete_left"} for kind in kinds
    )
    has_right = any(
        kind in {"copy_right", "metadata_update_right", "delete_right"}
        for kind in kinds
    )
    has_delete = any(kind in {"delete_left", "delete_right"} for kind in kinds)
    if has_delete:
        return " DEL<- " if "delete_left" in kinds else " DEL-> "
    if has_left and has_right:
        return " <-> "
    if has_left:
        return " <- "
    if has_right:
        return " -> "
    return ""


class ConfirmApplyModal(ModalScreen[bool]):
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("enter", "confirm", "Confirm"),
        ("a", "confirm", "Apply"),
        ("c", "cancel", "Cancel"),
    ]
    CSS = """
    ModalScreen {
        align: center middle;
    }
    #confirm-box {
        width: 70;
        height: auto;
        border: round #666666;
        padding: 1 2;
    }
    #confirm-buttons {
        height: auto;
    }
    """

    def __init__(self, total_operations: int) -> None:
        super().__init__()
        self.total_operations = total_operations

    def compose(self) -> ComposeResult:
        with Center():
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

    def action_cancel(self) -> None:
        self.dismiss(False)

    def action_confirm(self) -> None:
        self.dismiss(True)


class ApplyRunModal(ModalScreen[ExecuteResult | None]):
    BINDINGS = [
        ("escape", "close_if_done", "Close"),
        ("enter", "close_if_done", "Close"),
        ("c", "close_if_done", "Close"),
    ]
    CSS = """
    ModalScreen {
        align: center middle;
    }
    #apply-box {
        width: 110;
        height: 70%;
        border: round #666666;
        padding: 1;
    }
    #errors {
        height: 1fr;
        border: round #444444;
        padding: 1;
    }
    """

    def __init__(
        self,
        local_root: Path,
        remote_address: str,
        operations: list,
        progress_event_cb: Callable[[int, int, object, bool, str | None], None]
        | None = None,
    ) -> None:
        super().__init__()
        self.local_root = local_root
        self.remote_address = remote_address
        self.operations = operations
        self.progress_event_cb = progress_event_cb
        self.result: ExecuteResult | None = None

    def compose(self) -> ComposeResult:
        with Center():
            with Vertical(id="apply-box"):
                yield Static("Applying plan...", id="apply-status")
                yield ProgressBar(
                    total=len(self.operations), show_eta=False, id="apply-progress"
                )
                yield Static("Errors:\n-", id="errors")
                yield Button("Running...", id="close", disabled=True)

    def on_mount(self) -> None:
        self.run_worker(self._run_apply(), exclusive=True)

    async def _run_apply(self) -> None:
        def progress_cb(done: int, total: int, op, ok: bool, error: str | None) -> None:
            self.app.call_from_thread(self._on_progress, done, total, op, ok, error)

        try:
            result = await asyncio.to_thread(
                execute_plan,
                self.local_root,
                self.remote_address,
                self.operations,
                progress_cb,
            )
            self.result = result
            self.query_one("#apply-status", Static).update(
                f"Completed {result.succeeded_operations}/{result.total_operations} operations."
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


def _build_model(
    rows: list[dict[str, object]],
    root_name: str,
) -> tuple[
    DirEntry,
    dict[str, DirEntry],
    dict[str, FileEntry],
    dict[str, list[str]],
    dict[str, DiffRecord],
]:
    root = DirEntry(name=root_name, relpath=".")
    dirs_by_relpath: dict[str, DirEntry] = {".": root}
    files_by_relpath: dict[str, FileEntry] = {}
    dir_files_map: dict[str, list[str]] = {".": []}
    diffs_by_relpath: dict[str, DiffRecord] = {}

    for row in rows:
        relpath = str(row["relpath"])
        diffs_by_relpath[relpath] = _row_to_diff(row)
        path = PurePosixPath(relpath)
        parts = path.parts
        if not parts:
            continue

        current = root
        current_rel = PurePosixPath(".")
        lineage = [root]
        lineage_keys = ["."]

        for part in parts[:-1]:
            next_rel = (
                PurePosixPath(part)
                if current_rel == PurePosixPath(".")
                else current_rel / part
            )
            next_key = next_rel.as_posix()
            child = current.dirs.get(part)
            if child is None:
                child = DirEntry(name=part, relpath=next_key)
                current.dirs[part] = child
                dirs_by_relpath[next_key] = child
            current = child
            current_rel = next_rel
            lineage.append(current)
            lineage_keys.append(next_key)
            dir_files_map.setdefault(next_key, [])

        file_entry = FileEntry(
            relpath=relpath,
            name=parts[-1],
            content_state=str(row["content_state"]),
            metadata_state=str(row["metadata_state"]),
            metadata_diff=list(row.get("metadata_diff", [])),
            metadata_details=list(row.get("metadata_details", [])),
        )
        current.files.append(file_entry)
        files_by_relpath[relpath] = file_entry
        for dir_key in lineage_keys:
            dir_files_map.setdefault(dir_key, []).append(relpath)

        delta = _file_counts(file_entry)
        for ancestor in lineage:
            _apply_counts(ancestor.counts, delta)

    return root, dirs_by_relpath, files_by_relpath, dir_files_map, diffs_by_relpath


class ReviewApp(App[None]):
    CSS = """
    Screen {
        layout: vertical;
    }
    #body {
        height: 1fr;
    }
    #tree {
        width: 2fr;
        border: round #666666;
    }
    #side {
        width: 1fr;
    }
    #info {
        height: 1fr;
        border: round #666666;
        padding: 1;
    }
    #plan {
        height: 1fr;
        border: round #666666;
        padding: 1;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("h", "toggle_hide_identical", "Hide Identical"),
        ("enter", "toggle_cursor_node", "Open/Close"),
        ("l", "apply_left_wins", "Left Wins"),
        ("r", "apply_right_wins", "Right Wins"),
        ("i", "apply_ignore", "Ignore"),
        ("s", "apply_suggested", "Suggested"),
        ("a", "apply_plan", "Apply Plan"),
    ]

    def __init__(
        self,
        db_path: Path,
        local_root: Path,
        remote_address: str,
        hide_identical: bool,
    ) -> None:
        super().__init__()
        self.db_path = db_path
        self.local_root = local_root
        self.remote_address = remote_address
        self.hide_identical = hide_identical
        self.status_message = ""
        self.can_apply = False
        self._pending_apply_ops: list = []
        self._apply_required_ops: dict[str, set[str]] = {}
        self._apply_done_ops: dict[str, set[str]] = {}
        self._apply_newly_completed: set[str] = set()

        self._reload_state()
        self.action_overrides = load_action_overrides(self.db_path)

    def _reload_state(self) -> None:
        rows = load_current_diffs(db_path=self.db_path)
        (
            self.root,
            self.dirs_by_relpath,
            self.files_by_relpath,
            self.dir_files_map,
            self.diffs_by_relpath,
        ) = _build_model(rows, self.local_root.name or str(self.local_root))
        self.diffs = list(self.diffs_by_relpath.values())

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="body"):
            yield Tree(_folder_label(self.root), id="tree")
            with Vertical(id="side"):
                yield Static(id="info")
                yield Static(id="plan")
        yield Footer()

    def on_mount(self) -> None:
        self._sync_hide_binding_label()
        self._rebuild_tree()
        self._set_info_for_dir(self.root)
        self._update_plan_panel()

    def _visible_dir(self, entry: DirEntry) -> bool:
        return True if not self.hide_identical else not _is_identical_folder(entry)

    def _visible_file(self, entry: FileEntry) -> bool:
        return _is_changed(entry)

    def _dir_has_visible_children(self, entry: DirEntry) -> bool:
        return any(self._visible_dir(child) for child in entry.dirs.values()) or any(
            self._visible_file(file_entry) for file_entry in entry.files
        )

    def _populate_node(self, tree_node, dir_entry: DirEntry) -> None:
        tree_node.remove_children()
        for child_name in sorted(dir_entry.dirs):
            child = dir_entry.dirs[child_name]
            if not self._visible_dir(child):
                continue
            tree_node.add(
                _folder_label(child),
                data=("dir", child.relpath),
                allow_expand=self._dir_has_visible_children(child),
            )
        for file_entry in sorted(dir_entry.files, key=lambda item: item.name):
            if not self._visible_file(file_entry):
                continue
            action = self._effective_action(file_entry.relpath)
            ops = self._operations_for_entry(file_entry.relpath, action)
            marker = _ops_direction_marker(ops)
            label = _file_label(file_entry)
            if marker:
                if any(kind in {"delete_left", "delete_right"} for kind in ops):
                    label.stylize("dim")
                label.append(marker, style="magenta")
            tree_node.add(label, data=("file", file_entry.relpath), allow_expand=False)

    def _rebuild_tree(self) -> None:
        tree = self.query_one(Tree)
        tree.root.remove_children()
        tree.root.set_label(_folder_label(self.root))
        tree.root.data = ("dir", self.root.relpath)
        self._populate_node(tree.root, self.root)
        tree.root.expand()

    def _operations_for_entry(self, relpath: str, action: str) -> list[str]:
        diff = self.diffs_by_relpath.get(relpath)
        if diff is None:
            return []
        return [op.kind for op in build_plan_operations([diff], {relpath: action})]

    def _effective_action(self, relpath: str) -> str:
        return self.action_overrides.get(relpath, ACTION_IGNORE)

    def _sync_hide_binding_label(self) -> None:
        label = "Show Identical" if self.hide_identical else "Hide Identical"
        apply_label = "Apply Plan" if self.can_apply else "Apply Plan (disabled)"
        apply_action = "apply_plan" if self.can_apply else "apply_plan_disabled"
        self._bindings = BindingsMap(
            [
                ("q", "quit", "Quit"),
                ("h", "toggle_hide_identical", label),
                ("enter", "toggle_cursor_node", "Open/Close"),
                ("l", "apply_left_wins", "Left Wins"),
                ("r", "apply_right_wins", "Right Wins"),
                ("i", "apply_ignore", "Ignore"),
                ("s", "apply_suggested", "Suggested"),
                ("a", apply_action, apply_label),
            ]
        )
        self.refresh_bindings()

    def _current_selection(self) -> tuple[str, str] | None:
        tree = self.query_one(Tree)
        node = tree.cursor_node
        data = getattr(node, "data", None)
        if not data:
            return None
        return str(data[0]), str(data[1])

    def _selected_target_files(self) -> list[str]:
        selected = self._current_selection()
        if selected is None:
            return []
        kind, relpath = selected
        if kind == "file":
            return [relpath] if relpath in self.files_by_relpath else []
        return [
            path
            for path in self.dir_files_map.get(relpath, [])
            if path in self.files_by_relpath
        ]

    def _set_info_for_dir(self, entry: DirEntry) -> None:
        c = entry.counts
        meta_fields = (
            ", ".join(
                f"{name}:{count}"
                for name, count in sorted(
                    c.metadata_fields.items(), key=lambda item: (-item[1], item[0])
                )
            )
            if c.metadata_fields
            else "-"
        )
        lines = [
            f"Folder: {entry.relpath}",
            "",
            f"Only local: {c.only_local}",
            f"Only remote: {c.only_remote}",
            f"Identical: {c.identical}",
            f"Metadata-only: {c.metadata_only}",
            f"Metadata fields: {meta_fields}",
            f"Different: {c.different}",
            f"Uncertain: {c.uncertain}",
            "",
            f"Hide identical folders: {'ON' if self.hide_identical else 'OFF'}",
            "Actions: l=left wins r=right wins i=ignore s=suggested a=apply",
            "Keys: arrows navigate, enter open/close, h toggle identical, q quit",
        ]
        self.query_one("#info", Static).update("\n".join(lines))

    def _set_info_for_file(self, entry: FileEntry) -> None:
        suggested_ops = self._operations_for_entry(entry.relpath, ACTION_SUGGESTED)
        current_ops = self._operations_for_entry(
            entry.relpath, self._effective_action(entry.relpath)
        )
        lines = [f"File: {entry.relpath}", ""]
        if entry.content_state != "identical":
            lines.append(f"Content state: {entry.content_state}")
        if entry.metadata_details:
            lines.append(f"Metadata: {' | '.join(entry.metadata_details)}")
        lines.extend(
            [
                f"Suggested operations: {_ops_text(suggested_ops)}",
                f"Current action: {self._effective_action(entry.relpath)}",
                f"Current operations: {_ops_text(current_ops)}",
                "",
                "Actions: l=left wins r=right wins i=ignore s=suggested a=apply",
                "Keys: arrows navigate, enter open/close, h toggle identical, q quit",
            ]
        )
        self.query_one("#info", Static).update("\n".join(lines))

    def _update_plan_panel(
        self, *, plan_ops_override: list | None = None, include_selection: bool = True
    ) -> None:
        plan_ops = (
            plan_ops_override
            if plan_ops_override is not None
            else build_plan_operations(self.diffs, self.action_overrides)
        )
        summary = summarize_operations(plan_ops)
        new_can_apply = summary.total > 0
        if new_can_apply != self.can_apply:
            self.can_apply = new_can_apply
            self._sync_hide_binding_label()

        selected_changed: list[str] = []
        if include_selection:
            selected_changed = [
                relpath
                for relpath in self._selected_target_files()
                if relpath in self.files_by_relpath
                and _is_changed(self.files_by_relpath[relpath])
            ]

        lines = ["Plan Summary", ""]
        if summary.delete_left:
            lines.append(f"delete left: {summary.delete_left}")
        if summary.delete_right:
            lines.append(f"delete right: {summary.delete_right}")
        if summary.copy_left:
            lines.append(f"copy left: {summary.copy_left}")
        if summary.copy_right:
            lines.append(f"copy right: {summary.copy_right}")
        if summary.metadata_update_left:
            lines.append(f"metadata updates left: {summary.metadata_update_left}")
        if summary.metadata_update_right:
            lines.append(f"metadata updates right: {summary.metadata_update_right}")
        if summary.total == 0:
            lines.append("no operations planned")
        else:
            lines.append(f"total operations: {summary.total}")
        if include_selection:
            lines.extend(
                [
                    "",
                    f"Selection targets: {len(selected_changed)}",
                    "Selection actions:",
                ]
            )

            if not selected_changed:
                lines.append("-")
            else:
                for relpath in selected_changed[:20]:
                    action = self._effective_action(relpath)
                    ops = self._operations_for_entry(relpath, action)
                    lines.append(f"{relpath} -> {action} ({_ops_text(ops)})")
                if len(selected_changed) > 20:
                    lines.append(f"... and {len(selected_changed) - 20} more")

        if self.status_message:
            lines.extend(["", f"Status: {self.status_message}"])

        self.query_one("#plan", Static).update("\n".join(lines))

    def _apply_action(self, action: str) -> None:
        updates: dict[str, str] = {}
        for relpath in self._selected_target_files():
            entry = self.files_by_relpath.get(relpath)
            if entry is None or not _is_changed(entry):
                continue
            updates[relpath] = action
        if not updates:
            return

        self.confirm_apply_pending = False
        self.status_message = ""
        self.action_overrides.update(updates)
        upsert_action_overrides(self.db_path, updates)
        self._rebuild_tree()
        self._update_plan_panel()

        selected = self._current_selection()
        if selected is None:
            return
        kind, relpath = selected
        if kind == "file" and relpath in self.files_by_relpath:
            self._set_info_for_file(self.files_by_relpath[relpath])
        elif kind == "dir" and relpath in self.dirs_by_relpath:
            self._set_info_for_dir(self.dirs_by_relpath[relpath])

    def _mark_completed_paths(self, completed_paths: set[str]) -> None:
        if not completed_paths:
            return

        override_updates: dict[str, str] = {}
        touched_paths: set[str] = set()
        for relpath in completed_paths:
            file_entry = self.files_by_relpath.get(relpath)
            if file_entry is None:
                continue
            touched_paths.add(relpath)
            old_counts = _file_counts(file_entry)

            file_entry.content_state = "identical"
            file_entry.metadata_state = "identical"
            file_entry.metadata_diff = []
            file_entry.metadata_details = []
            new_counts = _file_counts(file_entry)

            self.diffs_by_relpath[relpath] = DiffRecord(
                relpath=relpath,
                content_state=ContentState.IDENTICAL,
                metadata_state=MetadataState.IDENTICAL,
                metadata_diff=(),
                metadata_details=(),
            )
            self.action_overrides.pop(relpath, None)
            override_updates[relpath] = ACTION_IGNORE

            path = PurePosixPath(relpath)
            dir_keys = ["."]
            current = PurePosixPath(".")
            for part in path.parts[:-1]:
                current = (
                    PurePosixPath(part)
                    if current == PurePosixPath(".")
                    else current / part
                )
                dir_keys.append(current.as_posix())

            for dir_key in dir_keys:
                dir_entry = self.dirs_by_relpath.get(dir_key)
                if dir_entry is None:
                    continue
                counts = dir_entry.counts
                counts.only_local += new_counts.only_local - old_counts.only_local
                counts.only_remote += new_counts.only_remote - old_counts.only_remote
                counts.identical += new_counts.identical - old_counts.identical
                counts.metadata_only += (
                    new_counts.metadata_only - old_counts.metadata_only
                )
                counts.different += new_counts.different - old_counts.different
                counts.uncertain += new_counts.uncertain - old_counts.uncertain
                keys = set(old_counts.metadata_fields) | set(new_counts.metadata_fields)
                for key in keys:
                    before = old_counts.metadata_fields.get(key, 0)
                    after = new_counts.metadata_fields.get(key, 0)
                    counts.metadata_fields[key] = counts.metadata_fields.get(key, 0) + (
                        after - before
                    )
                    if counts.metadata_fields.get(key) == 0:
                        counts.metadata_fields.pop(key, None)

        mark_paths_identical(self.db_path, touched_paths)
        if override_updates:
            upsert_action_overrides(self.db_path, override_updates)

        self.diffs = list(self.diffs_by_relpath.values())
        self._rebuild_tree()
        selected = self._current_selection()
        if selected is None:
            self._set_info_for_dir(self.root)
            return
        kind, relpath = selected
        if kind == "file" and relpath in self.files_by_relpath:
            self._set_info_for_file(self.files_by_relpath[relpath])
        elif kind == "dir" and relpath in self.dirs_by_relpath:
            self._set_info_for_dir(self.dirs_by_relpath[relpath])
        else:
            self._set_info_for_dir(self.root)

    def _on_apply_progress(
        self,
        done: int,
        total: int,
        op,
        ok: bool,
        error: str | None,
    ) -> None:
        _ = error
        if not ok:
            return
        relpath = op.relpath
        kind = op.kind
        self._apply_done_ops.setdefault(relpath, set()).add(kind)
        required = self._apply_required_ops.get(relpath, set())
        if required and required.issubset(self._apply_done_ops.get(relpath, set())):
            self._apply_newly_completed.add(relpath)

        should_flush = len(self._apply_newly_completed) >= 20 or done == total
        if should_flush and self._apply_newly_completed:
            batch = set(self._apply_newly_completed)
            self._apply_newly_completed.clear()
            self._mark_completed_paths(batch)

    def action_apply_plan(self) -> None:
        plan_ops = build_plan_operations(self.diffs, self.action_overrides)
        summary = summarize_operations(plan_ops)
        if summary.total == 0:
            self.status_message = "Nothing to apply."
            self._update_plan_panel()
            return

        self._pending_apply_ops = plan_ops
        self._apply_required_ops = {}
        self._apply_done_ops = {}
        self._apply_newly_completed = set()
        for op in plan_ops:
            self._apply_required_ops.setdefault(op.relpath, set()).add(op.kind)
        self.push_screen(
            ConfirmApplyModal(summary.total),
            callback=self._on_apply_confirmed,
        )

    def _on_apply_confirmed(self, confirmed: bool) -> None:
        if not confirmed:
            self.status_message = "Apply cancelled."
            self._update_plan_panel()
            return

        self.push_screen(
            ApplyRunModal(
                local_root=self.local_root,
                remote_address=self.remote_address,
                operations=self._pending_apply_ops,
                progress_event_cb=self._on_apply_progress,
            ),
            callback=self._on_apply_finished,
        )

    def _on_apply_finished(self, result: ExecuteResult | None) -> None:
        if result is None:
            self.status_message = "Apply interrupted."
            self._update_plan_panel()
            return

        if self._apply_newly_completed:
            batch = set(self._apply_newly_completed)
            self._apply_newly_completed.clear()
            self._mark_completed_paths(batch)

        remaining_ops = []
        for relpath, required in self._apply_required_ops.items():
            done = self._apply_done_ops.get(relpath, set())
            for kind in required:
                if kind not in done:
                    remaining_ops.append(PlanOperation(kind=kind, relpath=relpath))
        if result.errors:
            self.status_message = (
                f"Applied {result.succeeded_operations}/{result.total_operations} operations."
                f" {len(result.errors)} errors."
            )
        else:
            self.status_message = f"Applied {result.succeeded_operations}/{result.total_operations} operations."
        self._update_plan_panel(
            plan_ops_override=remaining_ops, include_selection=False
        )

    def action_apply_plan_disabled(self) -> None:
        self.status_message = "No operations in plan."
        self._update_plan_panel()

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        data = event.node.data
        if not data:
            return
        kind, relpath = data
        if kind != "dir":
            return
        dir_entry = self.dirs_by_relpath.get(str(relpath))
        if dir_entry is not None:
            self._populate_node(event.node, dir_entry)

    def on_tree_node_highlighted(self, event: Tree.NodeHighlighted) -> None:
        data = event.node.data
        if not data:
            return
        kind, relpath = data
        if kind == "dir":
            entry = self.dirs_by_relpath.get(str(relpath))
            if entry is not None:
                self._set_info_for_dir(entry)
        else:
            entry = self.files_by_relpath.get(str(relpath))
            if entry is not None:
                self._set_info_for_file(entry)
        self._update_plan_panel()

    def action_toggle_hide_identical(self) -> None:
        self.hide_identical = not self.hide_identical
        set_ui_pref(
            self.db_path, "review.hide_identical", "1" if self.hide_identical else "0"
        )
        self._sync_hide_binding_label()
        self._rebuild_tree()
        self._set_info_for_dir(self.root)
        self._update_plan_panel()

    def action_toggle_cursor_node(self) -> None:
        tree = self.query_one(Tree)
        node = tree.cursor_node
        data = getattr(node, "data", None)
        if not data:
            return
        kind, relpath = data
        if kind != "dir":
            return
        dir_entry = self.dirs_by_relpath.get(str(relpath))
        if dir_entry is None:
            return
        if node.is_expanded:
            node.collapse()
        else:
            self._populate_node(node, dir_entry)
            node.expand()

    def action_apply_left_wins(self) -> None:
        self._apply_action(ACTION_LEFT_WINS)

    def action_apply_right_wins(self) -> None:
        self._apply_action(ACTION_RIGHT_WINS)

    def action_apply_ignore(self) -> None:
        self._apply_action(ACTION_IGNORE)

    def action_apply_suggested(self) -> None:
        self._apply_action(ACTION_SUGGESTED)


def run_review_tui(
    db_path: Path,
    local_root: Path,
    remote_address: str,
    hide_identical: bool,
) -> None:
    app = ReviewApp(
        db_path=db_path,
        local_root=local_root,
        remote_address=remote_address,
        hide_identical=hide_identical,
    )
    app.run()
