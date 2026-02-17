from __future__ import annotations

import platform
import re
import subprocess
import tempfile
import unicodedata
from pathlib import Path, PurePosixPath

import paramiko
from textual.app import App, ComposeResult
from textual.binding import Binding, BindingsMap
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Header, Static, Tree

from .config import DEFAULT_STATE_SUBPATH, RemoteConfig
from .models import ContentState, DiffRecord, FileRecord, MetadataState
from .planner_apply import (
    ACTION_IGNORE,
    ACTION_SUGGESTED,
    ApplySettings,
    build_plan_operations,
    parse_remote_address,
    summarize_operations,
)
from .review_actions import ReviewActionsMixin
from .scanner_local import LocalScanner
from .scanner_remote import RemoteScanner
from .state_db import (
    load_action_overrides,
    load_current_diffs,
    mark_paths_identical,
    replace_diffs_in_scope,
    upsert_action_overrides,
)
from .tree_builder import (
    DirEntry,
    FileEntry,
    _build_model,
    _file_counts,
    _file_label,
    _folder_label,
    _is_changed,
    _is_identical_folder,
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
        return "copy metadata from local"
    if kind == "metadata_update_left":
        return "copy metadata from remote"
    return kind


def _ops_text(kinds: list[str]) -> str:
    if not kinds:
        return "-"
    labels = [_op_label(kind) for kind in kinds]
    return ", ".join(labels)


def _parse_metadata_details(details: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    mode_re = re.compile(r"mode:\s+local=(0x[0-7]{3})\s+remote=(0x[0-7]{3})")
    mtime_re = re.compile(r"mtime:\s+local=(.*?)\s+remote=(.*?)$")
    for detail in details:
        mode_match = mode_re.match(detail)
        if mode_match:
            parsed["mode_local"] = mode_match.group(1)
            parsed["mode_remote"] = mode_match.group(2)
            continue
        mtime_match = mtime_re.match(detail)
        if mtime_match:
            parsed["mtime_local"] = mtime_match.group(1)
            parsed["mtime_remote"] = mtime_match.group(2)
    return parsed


def _suggested_action_with_reason(entry: FileEntry, suggested_ops: list[str]) -> str:
    if entry.content_state == "different":
        return "no suggestion (manual content conflict)"
    if not suggested_ops:
        return "-"
    primary = suggested_ops[0]
    if primary not in {"metadata_update_left", "metadata_update_right"}:
        return _ops_text(suggested_ops)

    source = "local" if primary == "metadata_update_right" else "remote"
    parsed = _parse_metadata_details(entry.metadata_details)
    if "mode" in entry.metadata_diff and parsed.get("mode_local") != parsed.get(
        "mode_remote"
    ):
        return f"copy more restrictive metadata from {source}"
    if "mtime" in entry.metadata_diff and parsed.get("mtime_local") != parsed.get(
        "mtime_remote"
    ):
        return f"copy older metadata from {source}"
    return f"copy metadata from {source}"


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


class ReviewApp(ReviewActionsMixin, App[None]):
    TITLE = "LimSync"
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
        Binding("q", "quit", "Quit"),
        Binding("question_mark", "show_commands", "Commands"),
        Binding("h", "toggle_hide_identical", "Hide Identical", show=False),
        Binding("enter", "toggle_cursor_node", "Open/Close"),
        Binding("o", "open_selected", "Open", show=False),
        Binding("U", "update_selected_path", "Update Path", show=False),
        Binding("D", "delete_selected_both", "Delete Both", show=False),
        Binding("F", "diff_selected", "Diff", show=False),
        Binding("P", "copy_selected_path", "Copy Path", show=False),
        Binding("V", "view_plan", "View Plan", show=False),
        Binding("C", "clear_plan", "Clear Plan", show=False),
        Binding("M", "apply_all_metadata_suggestions", "Meta Suggestions", show=False),
        Binding("l", "apply_left_wins", "Left Wins"),
        Binding("r", "apply_right_wins", "Right Wins"),
        Binding("i", "apply_ignore", "Ignore"),
        Binding("I", "add_to_dropboxignore", "Add Ignore Rule", show=False),
        Binding("s", "apply_suggested", "Suggested"),
        Binding("a", "apply_plan", "Apply Plan"),
    ]

    def __init__(
        self,
        db_path: Path,
        local_root: Path,
        remote_address: str,
        hide_identical: bool,
        apply_settings: ApplySettings | None = None,
    ) -> None:
        super().__init__()
        self.db_path = db_path
        self.local_root = local_root
        self.remote_address = remote_address
        self.hide_identical = hide_identical
        self.apply_settings = apply_settings or ApplySettings()
        self.status_message = ""
        self.can_apply = False
        self._pending_apply_ops: list = []
        self._apply_required_ops: dict[str, set[str]] = {}
        self._apply_done_ops: dict[str, set[str]] = {}
        self._apply_newly_completed: set[str] = set()
        self._open_temp_dir: Path | None = None
        self._expanded_dir_relpaths: set[str] = {"."}

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
            yield Tree(
                _folder_label(self.root, include_identical=not self.hide_identical),
                id="tree",
            )
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
                _folder_label(child, include_identical=not self.hide_identical),
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
        expanded_before, selected_before = self._capture_tree_state()
        tree = self.query_one(Tree)
        tree.root.remove_children()
        tree.root.set_label(
            _folder_label(self.root, include_identical=not self.hide_identical)
        )
        tree.root.data = ("dir", self.root.relpath)
        self._populate_node(tree.root, self.root)
        tree.root.expand()
        self._restore_tree_state(expanded_before, selected_before)

    def _capture_tree_state(self) -> tuple[set[str], tuple[str, str] | None]:
        tree = self.query_one(Tree)
        expanded_dirs: set[str] = {"."}
        stack = [tree.root]
        while stack:
            node = stack.pop()
            data = getattr(node, "data", None)
            if data and str(data[0]) == "dir" and node.is_expanded:
                expanded_dirs.add(str(data[1]))
            for child in reversed(getattr(node, "children", ())):
                stack.append(child)

        selected: tuple[str, str] | None = None
        cursor_data = getattr(tree.cursor_node, "data", None)
        if cursor_data:
            selected = (str(cursor_data[0]), str(cursor_data[1]))
        return expanded_dirs, selected

    def _restore_tree_state(
        self, expanded_dirs: set[str], selected: tuple[str, str] | None
    ) -> None:
        tree = self.query_one(Tree)
        restored_expanded: set[str] = {"."}

        def expand_dir_node(node) -> None:
            data = getattr(node, "data", None)
            if not data or str(data[0]) != "dir":
                return
            relpath = str(data[1])
            if relpath != "." and relpath not in expanded_dirs:
                return
            entry = self.dirs_by_relpath.get(relpath)
            if entry is None:
                return
            self._populate_node(node, entry)
            node.expand()
            restored_expanded.add(relpath)
            for child in getattr(node, "children", ()):
                expand_dir_node(child)

        def find_node_by_data(node, target: tuple[str, str]):
            data = getattr(node, "data", None)
            if data and str(data[0]) == target[0] and str(data[1]) == target[1]:
                return node
            for child in getattr(node, "children", ()):
                found = find_node_by_data(child, target)
                if found is not None:
                    return found
            return None

        expand_dir_node(tree.root)
        if selected is not None:
            selected_node = find_node_by_data(tree.root, selected)
            if selected_node is not None:
                tree.select_node(selected_node)
                line = getattr(selected_node, "line", None)
                if isinstance(line, int):
                    tree.move_cursor_to_line(line, animate=False)
                tree.scroll_to_node(selected_node)
        self._expanded_dir_relpaths = restored_expanded

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
                Binding("q", "quit", "Quit"),
                Binding("question_mark", "show_commands", "Commands"),
                Binding("h", "toggle_hide_identical", label, show=False),
                Binding("enter", "toggle_cursor_node", "Open/Close"),
                Binding("o", "open_selected", "Open", show=False),
                Binding("U", "update_selected_path", "Update Path", show=False),
                Binding("D", "delete_selected_both", "Delete Both", show=False),
                Binding("F", "diff_selected", "Diff", show=False),
                Binding("P", "copy_selected_path", "Copy Path", show=False),
                Binding("V", "view_plan", "View Plan", show=False),
                Binding("C", "clear_plan", "Clear Plan", show=False),
                Binding(
                    "M",
                    "apply_all_metadata_suggestions",
                    "Meta Suggestions",
                    show=False,
                ),
                Binding("l", "apply_left_wins", "Left Wins"),
                Binding("r", "apply_right_wins", "Right Wins"),
                Binding("i", "apply_ignore", "Ignore"),
                Binding("I", "add_to_dropboxignore", "Add Ignore Rule", show=False),
                Binding("s", "apply_suggested", "Suggested"),
                Binding("a", apply_action, apply_label),
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

    def _scope_match(
        self, relpath: str, scope_relpath: str, scope_is_dir: bool
    ) -> bool:
        if scope_relpath == ".":
            return True
        if not scope_is_dir:
            return relpath == scope_relpath
        prefix = f"{scope_relpath.rstrip('/')}/"
        return relpath == scope_relpath or relpath.startswith(prefix)

    def _scan_subtree_records(
        self, scope_relpath: str, scope_is_dir: bool
    ) -> tuple[dict[str, FileRecord], dict[str, FileRecord]]:
        subtree = PurePosixPath(scope_relpath)
        local_records = LocalScanner(self.local_root).scan(subtree=subtree)
        user, host, remote_root = parse_remote_address(self.remote_address)
        remote_records = RemoteScanner(
            RemoteConfig(
                host=host,
                user=user,
                root=remote_root,
                state_db=f"{remote_root.rstrip('/')}/{DEFAULT_STATE_SUBPATH}",
            )
        ).scan(subtree=subtree)

        scoped_local = {
            relpath: record
            for relpath, record in local_records.items()
            if self._scope_match(relpath, scope_relpath, scope_is_dir)
        }
        scoped_remote = {
            relpath: record
            for relpath, record in remote_records.items()
            if self._scope_match(relpath, scope_relpath, scope_is_dir)
        }
        return scoped_local, scoped_remote

    def _replace_scope_with_diffs(
        self, scope_relpath: str, scope_is_dir: bool, scoped_diffs: list[DiffRecord]
    ) -> None:
        new_diffs_by_relpath = {diff.relpath: diff for diff in scoped_diffs}
        for relpath in list(self.diffs_by_relpath):
            if self._scope_match(relpath, scope_relpath, scope_is_dir):
                self.diffs_by_relpath.pop(relpath, None)
        self.diffs_by_relpath.update(new_diffs_by_relpath)
        self.diffs = [
            self.diffs_by_relpath[key] for key in sorted(self.diffs_by_relpath)
        ]
        replace_diffs_in_scope(
            self.db_path,
            scoped_diffs,
            scope_relpath=scope_relpath,
            scope_is_dir=scope_is_dir,
        )
        self.action_overrides = load_action_overrides(self.db_path)

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
            "Actions: ?=commands l=left wins r=right wins i=ignore s=suggested",
        ]
        self.query_one("#info", Static).update("\n".join(lines))

    def _set_info_for_file(self, entry: FileEntry) -> None:
        suggested_ops = self._operations_for_entry(entry.relpath, ACTION_SUGGESTED)
        current_ops = self._operations_for_entry(
            entry.relpath, self._effective_action(entry.relpath)
        )
        lines = [f"File: {entry.relpath}", ""]
        if entry.local_size is not None and entry.remote_size is not None:
            if entry.local_size == entry.remote_size:
                lines.append(f"Size: {entry.local_size:,} bytes")
            else:
                lines.append(
                    f"Size: local={entry.local_size:,} bytes remote={entry.remote_size:,} bytes"
                )
        elif entry.local_size is not None:
            lines.append(f"Size: local={entry.local_size:,} bytes")
        elif entry.remote_size is not None:
            lines.append(f"Size: remote={entry.remote_size:,} bytes")
        if entry.content_state not in {"identical", "unknown"}:
            lines.append(f"Content state: {entry.content_state}")
        if entry.metadata_state == "different":
            parsed = _parse_metadata_details(entry.metadata_details)
            if parsed.get("mode_local") != parsed.get("mode_remote"):
                lines.append(
                    f"Permissions: local={parsed.get('mode_local', '?')} remote={parsed.get('mode_remote', '?')}"
                )
            if parsed.get("mtime_local") != parsed.get("mtime_remote"):
                lines.append(
                    f"MTime: local={parsed.get('mtime_local', '?')} remote={parsed.get('mtime_remote', '?')}"
                )
        lines.extend(
            [
                f"Suggested action: {_suggested_action_with_reason(entry, suggested_ops)}",
                f"Current action: {self._effective_action(entry.relpath)}",
                f"Current operations: {_ops_text(current_ops)}",
                "",
                "Actions: ?=commands l=left wins r=right wins i=ignore s=suggested",
            ]
        )
        self.query_one("#info", Static).update("\n".join(lines))

    def _selected_file_relpath(self) -> str | None:
        selected = self._current_selection()
        if selected is None:
            return None
        kind, relpath = selected
        if kind != "file":
            return None
        if relpath not in self.files_by_relpath:
            return None
        return relpath

    def _open_with_default_app(self, file_path: Path) -> None:
        if platform.system() == "Darwin":
            cmd = ["open", str(file_path)]
        elif platform.system() == "Windows":
            cmd = ["cmd", "/c", "start", "", str(file_path)]
        else:
            cmd = ["xdg-open", str(file_path)]
        subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )

    def _notify_message(self, message: str, severity: str = "information") -> None:
        try:
            self.notify(message, severity=severity, timeout=4)
        except Exception:  # noqa: BLE001
            # Fallback for older textual versions.
            self.status_message = message
            self._update_plan_panel()

    def _candidate_relpaths(self, relpath: str) -> list[str]:
        candidates: list[str] = [relpath]
        nfc = unicodedata.normalize("NFC", relpath)
        nfd = unicodedata.normalize("NFD", relpath)
        if nfc not in candidates:
            candidates.append(nfc)
        if nfd not in candidates:
            candidates.append(nfd)
        return candidates

    def _download_remote_file(self, relpath: str) -> Path:
        user, host, remote_root = parse_remote_address(self.remote_address)
        if self._open_temp_dir is None:
            self._open_temp_dir = Path(tempfile.mkdtemp(prefix="limsync-open-"))
        target = self._open_temp_dir / relpath
        target.parent.mkdir(parents=True, exist_ok=True)

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=host,
            username=user,
            port=22,
            look_for_keys=True,
            allow_agent=True,
            timeout=10,
        )
        sftp = client.open_sftp()
        try:
            # Expand ~ on remote shell, then resolve via SFTP.
            quoted = remote_root.replace("'", "'\\''")
            _stdin, stdout, _stderr = client.exec_command(
                f"python3 -c \"import os; print(os.path.expanduser('{quoted}'))\""
            )
            expanded = (
                stdout.read().decode("utf-8", errors="replace").strip() or remote_root
            )
            remote_root_abs = sftp.normalize(expanded)

            last_error: Exception | None = None
            for rel_candidate in self._candidate_relpaths(relpath):
                remote_path = str(
                    PurePosixPath(remote_root_abs) / PurePosixPath(rel_candidate)
                )
                try:
                    sftp.get(remote_path, str(target))
                    return target
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    continue
            if last_error is not None:
                raise last_error
        finally:
            sftp.close()
            client.close()
        return target

    def _has_local_copy(self, relpath: str) -> bool:
        diff = self.diffs_by_relpath.get(relpath)
        return diff is not None and diff.content_state != ContentState.ONLY_REMOTE

    def _has_remote_copy(self, relpath: str) -> bool:
        diff = self.diffs_by_relpath.get(relpath)
        return diff is not None and diff.content_state != ContentState.ONLY_LOCAL

    def _open_file_side(self, relpath: str, side: str) -> None:
        try:
            if side == "left":
                path = self.local_root / relpath
                self._open_with_default_app(path)
                self._notify_message(f"Opened local file: {relpath}")
            else:
                downloaded = self._download_remote_file(relpath)
                self._open_with_default_app(downloaded)
                self._notify_message(f"Opened remote file: {relpath}")
        except Exception as exc:  # noqa: BLE001
            self._notify_message(f"Open failed: {exc}", severity="error")

    def _on_open_side_chosen(self, relpath: str, side: str | None) -> None:
        if side is None:
            self._notify_message("Open cancelled.")
            return
        self._open_file_side(relpath, side)

    def _update_plan_panel(self, *, plan_ops_override: list | None = None) -> None:
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
            resolved_size = (
                file_entry.local_size
                if file_entry.local_size is not None
                else file_entry.remote_size
            )
            file_entry.local_size = resolved_size
            file_entry.remote_size = resolved_size
            new_counts = _file_counts(file_entry)

            self.diffs_by_relpath[relpath] = DiffRecord(
                relpath=relpath,
                content_state=ContentState.IDENTICAL,
                metadata_state=MetadataState.IDENTICAL,
                metadata_diff=(),
                metadata_details=(),
                local_size=file_entry.local_size,
                remote_size=file_entry.remote_size,
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


def run_review_tui(
    db_path: Path,
    local_root: Path,
    remote_address: str,
    hide_identical: bool,
    apply_settings: ApplySettings | None = None,
) -> None:
    app = ReviewApp(
        db_path=db_path,
        local_root=local_root,
        remote_address=remote_address,
        hide_identical=hide_identical,
        apply_settings=apply_settings,
    )
    app.run()
