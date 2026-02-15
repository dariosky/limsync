from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import BindingsMap
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Static, Tree

from .state_db import load_run_diffs, set_ui_pref


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
    elif file_entry.metadata_state == "different":
        for field_name in file_entry.metadata_diff:
            counts.metadata_fields[field_name] = (
                counts.metadata_fields.get(field_name, 0) + 1
            )
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


def _build_model(
    rows: list[dict[str, object]], root_name: str
) -> tuple[DirEntry, dict[str, DirEntry], dict[str, FileEntry]]:
    root = DirEntry(name=root_name, relpath=".")
    dirs_by_relpath: dict[str, DirEntry] = {".": root}
    files_by_relpath: dict[str, FileEntry] = {}

    for row in rows:
        relpath = str(row["relpath"])
        path = PurePosixPath(relpath)
        parts = path.parts
        if not parts:
            continue

        current = root
        current_rel = PurePosixPath(".")
        lineage = [root]

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

        delta = _file_counts(file_entry)
        for ancestor in lineage:
            _apply_counts(ancestor.counts, delta)

    return root, dirs_by_relpath, files_by_relpath


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
    #info {
        width: 1fr;
        border: round #666666;
        padding: 1;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("h", "toggle_hide_identical", "Hide Identical"),
        ("enter", "toggle_cursor_node", "Open/Close"),
    ]

    def __init__(
        self,
        db_path: Path,
        run_id: int,
        local_root: Path,
        hide_identical: bool,
    ) -> None:
        super().__init__()
        self.db_path = db_path
        self.run_id = run_id
        self.local_root = local_root
        self.hide_identical = hide_identical

        rows = load_run_diffs(db_path=self.db_path, run_id=self.run_id)
        root_name = local_root.name or str(local_root)
        self.root, self.dirs_by_relpath, self.files_by_relpath = _build_model(
            rows, root_name
        )

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="body"):
            yield Tree(_folder_label(self.root), id="tree")
            yield Static(id="info")
        yield Footer()

    def on_mount(self) -> None:
        self._sync_hide_binding_label()
        self._rebuild_tree()
        self._set_info_for_dir(self.root)

    def _visible_dir(self, entry: DirEntry) -> bool:
        if not self.hide_identical:
            return True
        return not _is_identical_folder(entry)

    def _visible_file(self, entry: FileEntry) -> bool:
        # Keep identical files collapsed from the view; folder counters still include them.
        return not (
            entry.content_state == "identical" and entry.metadata_state == "identical"
        )

    def _dir_has_visible_children(self, entry: DirEntry) -> bool:
        for child in entry.dirs.values():
            if self._visible_dir(child):
                return True
        for file_entry in entry.files:
            if self._visible_file(file_entry):
                return True
        return False

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
            tree_node.add(
                _file_label(file_entry),
                data=("file", file_entry.relpath),
                allow_expand=False,
            )

    def _rebuild_tree(self) -> None:
        tree = self.query_one(Tree)
        tree.root.remove_children()
        tree.root.set_label(_folder_label(self.root))
        tree.root.data = ("dir", self.root.relpath)
        self._populate_node(tree.root, self.root)
        tree.root.expand()

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
            "Keys: arrows navigate, enter open/close, h toggle identical, q quit",
        ]
        self.query_one("#info", Static).update("\n".join(lines))

    def _set_info_for_file(self, entry: FileEntry) -> None:
        lines = [
            f"File: {entry.relpath}",
            "",
            f"Content state: {entry.content_state}",
            f"Metadata state: {entry.metadata_state}",
            f"Metadata fields: {', '.join(entry.metadata_diff) if entry.metadata_diff else '-'}",
            "Metadata details:",
            *(entry.metadata_details if entry.metadata_details else ["-"]),
            "",
            "Keys: arrows navigate, enter open/close, h toggle identical, q quit",
        ]
        self.query_one("#info", Static).update("\n".join(lines))

    def _sync_hide_binding_label(self) -> None:
        label = "Show Identical" if self.hide_identical else "Hide Identical"
        self._bindings = BindingsMap(
            [
                ("q", "quit", "Quit"),
                ("h", "toggle_hide_identical", label),
                ("enter", "toggle_cursor_node", "Open/Close"),
            ]
        )
        self.refresh_bindings()

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        data = event.node.data
        if not data:
            return
        kind, relpath = data
        if kind != "dir":
            return
        dir_entry = self.dirs_by_relpath.get(str(relpath))
        if dir_entry is None:
            return
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

    def action_toggle_hide_identical(self) -> None:
        self.hide_identical = not self.hide_identical
        set_ui_pref(
            self.db_path, "review.hide_identical", "1" if self.hide_identical else "0"
        )
        self._sync_hide_binding_label()
        self._rebuild_tree()
        self._set_info_for_dir(self.root)

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


def run_review_tui(
    db_path: Path,
    run_id: int,
    local_root: Path,
    hide_identical: bool,
) -> None:
    app = ReviewApp(
        db_path=db_path,
        run_id=run_id,
        local_root=local_root,
        hide_identical=hide_identical,
    )
    app.run()
