from __future__ import annotations

import difflib
import platform
import posixpath
import shutil
import subprocess
from pathlib import Path, PurePosixPath

from textual.widgets import Tree

from .compare import compare_records
from .deletion_intent import apply_intentional_deletion_hints
from .modals import (
    ApplyRunModal,
    CommandsModal,
    ConfirmApplyModal,
    ConfirmDeleteModal,
    FileDiffModal,
    OpenSideModal,
    PlanTreeModal,
)
from .models import ContentState
from .planner_apply import (
    ACTION_IGNORE,
    ACTION_LEFT_WINS,
    ACTION_RIGHT_WINS,
    ACTION_SUGGESTED,
    ExecuteResult,
    PlanOperation,
    build_plan_operations,
    summarize_operations,
)
from .ssh_pool import pooled_ssh_client
from .state_db import (
    clear_action_overrides,
    delete_paths_from_current_state,
    load_action_overrides,
    set_ui_pref,
    upsert_action_overrides,
)


class ReviewActionsMixin:
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
                source_endpoint=self.source_endpoint,
                destination_endpoint=self.destination_endpoint,
                operations=self._pending_apply_ops,
                apply_settings=self.apply_settings,
                progress_event_cb=self._on_apply_progress,
            ),
            callback=self._on_apply_finished,
        )

    def _on_apply_finished(self, result: ExecuteResult | None) -> None:
        if result is None:
            self.status_message = "Apply interrupted."
            self._update_plan_panel()
            return

        if result.completed_paths:
            self._mark_completed_paths(set(result.completed_paths))

        remaining_ops = [
            op
            for op in self._pending_apply_ops
            if (op.kind, op.relpath) not in result.succeeded_operation_keys
        ]
        if result.errors:
            self.status_message = (
                f"Applied {result.succeeded_operations}/{result.total_operations} operations."
                f" {len(result.errors)} errors."
            )
        else:
            self.status_message = f"Applied {result.succeeded_operations}/{result.total_operations} operations."
        self._update_plan_panel(plan_ops_override=remaining_ops)

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
        self._expanded_dir_relpaths.add(str(relpath))
        dir_entry = self.dirs_by_relpath.get(str(relpath))
        # Avoid repopulating already-built nodes during restore/rebuild.
        # Re-population here can reset nested expansion state.
        if dir_entry is not None and not event.node.children:
            self._populate_node(event.node, dir_entry)

    def on_tree_node_collapsed(self, event: Tree.NodeCollapsed) -> None:
        data = event.node.data
        if not data:
            return
        kind, relpath = data
        if kind != "dir":
            return
        if str(relpath) != ".":
            self._expanded_dir_relpaths.discard(str(relpath))

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
            if str(relpath) != ".":
                self._expanded_dir_relpaths.discard(str(relpath))
        else:
            self._populate_node(node, dir_entry)
            node.expand()
            self._expanded_dir_relpaths.add(str(relpath))

    def action_apply_left_wins(self) -> None:
        self._apply_action(ACTION_LEFT_WINS)

    def action_apply_right_wins(self) -> None:
        self._apply_action(ACTION_RIGHT_WINS)

    def action_apply_ignore(self) -> None:
        self._apply_action(ACTION_IGNORE)

    def _selected_node(self) -> tuple[str, str] | None:
        selected = self._current_selection()
        if selected is None:
            return None
        kind, relpath = selected
        if kind not in {"dir", "file"}:
            return None
        return kind, relpath

    def _append_dropboxignore_rule(
        self,
        parent_relpath: str,
        rule_name: str,
        *,
        is_dir: bool,
    ) -> bool:
        rule = f"{rule_name}/" if is_dir else rule_name
        existing_aliases = {rule}
        if is_dir:
            existing_aliases.add(rule_name)

        if self.source_endpoint.is_local:
            source_root = Path(self.source_endpoint.root)
            parent_path = (
                source_root
                if parent_relpath == "."
                else source_root / PurePosixPath(parent_relpath)
            )
            ignore_path = parent_path / ".dropboxignore"
            if ignore_path.exists():
                content = ignore_path.read_text(encoding="utf-8")
            else:
                parent_path.mkdir(parents=True, exist_ok=True)
                content = ""
        else:
            endpoint = self.source_endpoint

            def _ensure_remote_dir(sftp, path: str) -> None:
                if path in {"", "/"}:
                    return
                parts: list[str] = []
                cur = path
                while cur and cur != "/":
                    parts.append(cur)
                    cur = posixpath.dirname(cur)
                for seg in reversed(parts):
                    try:
                        sftp.stat(seg)
                    except OSError:
                        sftp.mkdir(seg)

            with pooled_ssh_client(
                host=str(endpoint.host),
                user=str(endpoint.user),
                port=endpoint.port or 22,
                compress=self.apply_settings.ssh_compression,
                timeout=10,
            ) as client:
                sftp = client.open_sftp()
                try:
                    quoted = endpoint.root.replace("'", "'\\''")
                    _stdin, stdout, _stderr = client.exec_command(
                        f"python3 -c \"import os; print(os.path.expanduser('{quoted}'))\""
                    )
                    expanded = (
                        stdout.read().decode("utf-8", errors="replace").strip()
                        or endpoint.root
                    )
                    remote_root_abs = sftp.normalize(expanded)
                    parent_path = (
                        remote_root_abs
                        if parent_relpath == "."
                        else f"{remote_root_abs.rstrip('/')}/{parent_relpath}"
                    )
                    _ensure_remote_dir(sftp, parent_path)
                    ignore_path = f"{parent_path.rstrip('/')}/.dropboxignore"
                    try:
                        with sftp.open(ignore_path, "r") as handle:
                            content = handle.read().decode("utf-8", errors="replace")
                    except OSError:
                        content = ""
                finally:
                    sftp.close()

        for line in content.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped in existing_aliases:
                return False

        if content and not content.endswith("\n"):
            content += "\n"
        content += f"{rule}\n"
        if self.source_endpoint.is_local:
            ignore_path.write_text(content, encoding="utf-8")
        else:
            endpoint = self.source_endpoint
            with pooled_ssh_client(
                host=str(endpoint.host),
                user=str(endpoint.user),
                port=endpoint.port or 22,
                compress=self.apply_settings.ssh_compression,
                timeout=10,
            ) as client:
                sftp = client.open_sftp()
                try:
                    quoted = endpoint.root.replace("'", "'\\''")
                    _stdin, stdout, _stderr = client.exec_command(
                        f"python3 -c \"import os; print(os.path.expanduser('{quoted}'))\""
                    )
                    expanded = (
                        stdout.read().decode("utf-8", errors="replace").strip()
                        or endpoint.root
                    )
                    remote_root_abs = sftp.normalize(expanded)
                    parent_path = (
                        remote_root_abs
                        if parent_relpath == "."
                        else f"{remote_root_abs.rstrip('/')}/{parent_relpath}"
                    )
                    ignore_path = f"{parent_path.rstrip('/')}/.dropboxignore"
                    with sftp.open(ignore_path, "w") as handle:
                        handle.write(content)
                finally:
                    sftp.close()
        return True

    def action_add_to_dropboxignore(self) -> None:
        selected = self._selected_node()
        if selected is None:
            self._notify_message("Select a file or folder first.", severity="warning")
            return
        kind, relpath = selected
        if relpath == ".":
            self._notify_message("Cannot ignore the root folder.", severity="warning")
            return

        selected_path = PurePosixPath(relpath)
        parent_relpath = selected_path.parent.as_posix()
        name = selected_path.name

        try:
            added = self._append_dropboxignore_rule(
                parent_relpath=parent_relpath,
                rule_name=name,
                is_dir=(kind == "dir"),
            )
        except Exception as exc:  # noqa: BLE001
            self._notify_message(
                f"Failed to update .dropboxignore: {exc}", severity="error"
            )
            return

        removed_paths = (
            {relpath} if kind == "file" else set(self.dir_files_map.get(relpath, []))
        )
        if removed_paths:
            delete_paths_from_current_state(self.db_path, removed_paths)

        self._reload_state()
        self.action_overrides = load_action_overrides(self.db_path)
        self._expanded_dir_relpaths.discard(relpath)
        ignore_file = (
            ".dropboxignore"
            if parent_relpath == "."
            else f"{parent_relpath}/.dropboxignore"
        )
        if added:
            self.status_message = f"Added '{name}' to {ignore_file}."
        else:
            self.status_message = f"'{name}' already present in {ignore_file}."
        self._rebuild_tree()
        self._set_info_for_dir(self.root)
        self._update_plan_panel()

    def action_apply_suggested(self) -> None:
        self._apply_action(ACTION_SUGGESTED)

    def action_view_plan(self) -> None:
        plan_ops = build_plan_operations(self.diffs, self.action_overrides)
        self.push_screen(PlanTreeModal(plan_ops))

    def action_update_selected_path(self) -> None:
        selected = self._selected_node()
        if selected is None:
            self._notify_message("Select a file or folder first.", severity="warning")
            return
        kind, relpath = selected
        scope_relpath = relpath
        scope_is_dir = kind == "dir"

        try:
            previous_content_states = {
                relpath: diff.content_state
                for relpath, diff in self.diffs_by_relpath.items()
                if self._scope_match(relpath, scope_relpath, scope_is_dir)
            }
            source_records, destination_records = self._scan_subtree_records(
                scope_relpath, scope_is_dir
            )
            scoped_diffs = compare_records(source_records, destination_records)
            scoped_diffs = apply_intentional_deletion_hints(
                scoped_diffs, previous_content_states
            )
            self._replace_scope_with_diffs(scope_relpath, scope_is_dir, scoped_diffs)
        except Exception as exc:  # noqa: BLE001
            self._notify_message(f"Update path failed: {exc}", severity="error")
            return

        self.status_message = (
            f"Updated path: {scope_relpath} ({len(scoped_diffs)} compared path"
            f"{'' if len(scoped_diffs) == 1 else 's'})."
        )
        self._refresh_after_plan_change()

    def _copy_text_to_clipboard(self, text: str) -> None:
        system = platform.system()
        cmd: list[str] | None = None
        if system == "Darwin":
            cmd = ["pbcopy"]
        elif system == "Windows":
            cmd = ["clip"]
        else:
            if shutil.which("wl-copy"):
                cmd = ["wl-copy"]
            elif shutil.which("xclip"):
                cmd = ["xclip", "-selection", "clipboard"]
            elif shutil.which("xsel"):
                cmd = ["xsel", "--clipboard", "--input"]
        if cmd is None:
            raise RuntimeError("No clipboard utility found.")
        subprocess.run(
            cmd,
            input=text,
            text=True,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def action_copy_selected_path(self) -> None:
        selected = self._selected_node()
        if selected is None:
            self._notify_message(
                "Select a file or folder to copy path.", severity="warning"
            )
            return
        _kind, relpath = selected
        if relpath == ".":
            self._notify_message("Cannot copy root path.", severity="warning")
            return
        try:
            self._copy_text_to_clipboard(relpath)
        except Exception as exc:  # noqa: BLE001
            self._notify_message(f"Copy path failed: {exc}", severity="error")
            return
        self.status_message = f"Copied path: {relpath}"
        self._update_plan_panel()

    def _delete_ops_for_selected(self) -> tuple[str, list[PlanOperation]]:
        selected = self._selected_node()
        if selected is None:
            return "", []
        kind, relpath = selected
        relpaths = (
            [relpath] if kind == "file" else list(self.dir_files_map.get(relpath, []))
        )

        ops_by_key: dict[tuple[str, str], PlanOperation] = {}
        for target_relpath in relpaths:
            diff = self.diffs_by_relpath.get(target_relpath)
            if diff is None:
                continue
            if diff.content_state != ContentState.ONLY_RIGHT:
                ops_by_key[("delete_left", target_relpath)] = PlanOperation(
                    "delete_left", target_relpath
                )
            if diff.content_state != ContentState.ONLY_LEFT:
                ops_by_key[("delete_right", target_relpath)] = PlanOperation(
                    "delete_right", target_relpath
                )
        return relpath, list(ops_by_key.values())

    def action_delete_selected_both(self) -> None:
        relpath, ops = self._delete_ops_for_selected()
        if not relpath:
            self._notify_message(
                "Select a file or folder to delete.", severity="warning"
            )
            return
        if relpath == ".":
            self._notify_message("Cannot delete the root folder.", severity="warning")
            return
        if not ops:
            self._notify_message("Nothing to delete for selection.", severity="warning")
            return

        files_affected = len({op.relpath for op in ops})
        self.push_screen(
            ConfirmDeleteModal(relpath, files_affected),
            callback=lambda confirmed: self._on_delete_confirmed(ops, confirmed),
        )

    def _on_delete_confirmed(
        self, operations: list[PlanOperation], confirmed: bool
    ) -> None:
        if not confirmed:
            self.status_message = "Delete cancelled."
            self._update_plan_panel()
            return

        self._pending_apply_ops = operations
        self._apply_required_ops = {}
        self._apply_done_ops = {}
        self._apply_newly_completed = set()
        for op in operations:
            self._apply_required_ops.setdefault(op.relpath, set()).add(op.kind)

        self.push_screen(
            ApplyRunModal(
                source_endpoint=self.source_endpoint,
                destination_endpoint=self.destination_endpoint,
                operations=operations,
                apply_settings=self.apply_settings,
                progress_event_cb=self._on_apply_progress,
            ),
            callback=self._on_delete_finished,
        )

    def _on_delete_finished(self, result: ExecuteResult | None) -> None:
        self._on_apply_finished(result)

    def _refresh_after_plan_change(self) -> None:
        self._rebuild_tree()
        self._update_plan_panel()
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

    def action_clear_plan(self) -> None:
        if not self.action_overrides:
            self.status_message = "Plan already clear."
            self._update_plan_panel()
            return
        clear_action_overrides(self.db_path)
        self.action_overrides = {}
        self.status_message = "Plan cleared: all actions reset to ignore."
        self._refresh_after_plan_change()

    def action_apply_all_metadata_suggestions(self) -> None:
        updates: dict[str, str] = {}
        for relpath, entry in self.files_by_relpath.items():
            if entry.metadata_state != "different":
                continue
            suggested_ops = self._operations_for_entry(relpath, ACTION_SUGGESTED)
            if not suggested_ops:
                continue
            if not all(
                op_kind in {"metadata_update_left", "metadata_update_right"}
                for op_kind in suggested_ops
            ):
                continue
            updates[relpath] = ACTION_SUGGESTED
        if not updates:
            self.status_message = "No metadata-only suggestions available."
            self._update_plan_panel()
            return

        self.action_overrides.update(updates)
        upsert_action_overrides(self.db_path, updates)
        self.status_message = (
            f"Applied metadata suggestions to {len(updates)} file"
            f"{'' if len(updates) == 1 else 's'}."
        )
        self._refresh_after_plan_change()

    def _on_command_chosen(self, action_name: str | None) -> None:
        if action_name is None:
            return
        handler = getattr(self, f"action_{action_name}", None)
        if callable(handler):
            handler()

    def action_show_commands(self) -> None:
        self.push_screen(CommandsModal(), callback=self._on_command_chosen)

    def _read_text_lines_for_diff(
        self, file_path: Path, side_label: str
    ) -> tuple[list[str] | None, str | None]:
        try:
            payload = file_path.read_bytes()
        except Exception as exc:  # noqa: BLE001
            return None, f"{side_label}: read failed ({exc})"
        if b"\x00" in payload:
            return (
                None,
                f"{side_label}: binary content detected; textual diff is not available.",
            )
        text = payload.decode("utf-8", errors="replace")
        return text.splitlines(), None

    def _build_text_diff(
        self, relpath: str, source_path: Path, destination_path: Path
    ) -> str:
        source_lines, source_error = self._read_text_lines_for_diff(source_path, "left")
        if source_error is not None:
            return source_error
        destination_lines, destination_error = self._read_text_lines_for_diff(
            destination_path, "right"
        )
        if destination_error is not None:
            return destination_error

        diff_lines = list(
            difflib.unified_diff(
                source_lines or [],
                destination_lines or [],
                fromfile=f"left/{relpath}",
                tofile=f"right/{relpath}",
                lineterm="",
            )
        )
        if not diff_lines:
            return "No textual differences."

        max_lines = 2500
        if len(diff_lines) > max_lines:
            shown = diff_lines[:max_lines]
            shown.append("")
            shown.append(
                f"... diff truncated: showing {max_lines} of {len(diff_lines)} lines ..."
            )
            diff_lines = shown
        return "\n".join(diff_lines)

    def action_diff_selected(self) -> None:
        relpath = self._selected_file_relpath()
        if relpath is None:
            self._notify_message("Select a file to diff.", severity="warning")
            return
        if not self._has_left_copy(relpath) or not self._has_right_copy(relpath):
            self._notify_message(
                "Diff is available only when both left and right files exist.",
                severity="warning",
            )
            return

        try:
            source_path = self._download_endpoint_file(self.source_endpoint, relpath)
            destination_path = self._download_endpoint_file(
                self.destination_endpoint, relpath
            )
            diff_text = self._build_text_diff(relpath, source_path, destination_path)
            self.push_screen(FileDiffModal(relpath, diff_text))
        except Exception as exc:  # noqa: BLE001
            self._notify_message(f"Diff failed: {exc}", severity="error")

    def action_open_selected(self) -> None:
        relpath = self._selected_file_relpath()
        if relpath is None:
            self._notify_message("Select a file to open.", severity="warning")
            return

        has_source = self._has_left_copy(relpath)
        has_destination = self._has_right_copy(relpath)
        if has_source and has_destination:
            self.push_screen(
                OpenSideModal(relpath),
                callback=lambda side: self._on_open_side_chosen(relpath, side),
            )
            return

        if has_source:
            self._open_file_side(relpath, "left")
            return
        if has_destination:
            self._open_file_side(relpath, "right")
            return

        self._notify_message("File not found on either side.", severity="warning")
