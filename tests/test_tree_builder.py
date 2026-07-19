from limsync.tree_builder import (
    ActionCounts,
    DirEntry,
    FileEntry,
    FolderCounts,
    _file_label,
    _folder_action_counts_by_relpath,
    _folder_counts_by_relpath,
    _folder_label,
)


def _mk_file_entry(
    content_state: str,
    metadata_state: str = "identical",
    relpath: str = "a.txt",
    metadata_diff: list[str] | None = None,
    left_size: int | None = None,
    right_size: int | None = None,
) -> FileEntry:
    return FileEntry(
        relpath=relpath,
        name=relpath.rsplit("/", 1)[-1],
        content_state=content_state,
        metadata_state=metadata_state,
        metadata_diff=metadata_diff or [],
        metadata_details=[],
        left_size=left_size,
        right_size=right_size,
    )


def test_folder_label_hides_zero_counts_and_uses_readable_names() -> None:
    entry = DirEntry(
        name="docs",
        relpath="docs",
        counts=FolderCounts(
            only_left=2,
            only_right=0,
            identical=0,
            metadata_only=3,
            different=1,
            uncertain=0,
        ),
    )

    assert _folder_label(entry).plain == "docs  Left 2 | Conflict 1 | Metadata 3"


def test_folder_label_shows_no_changes_when_all_counts_zero() -> None:
    entry = DirEntry(name="docs", relpath="docs", counts=FolderCounts())
    assert _folder_label(entry).plain == "docs  No changes"


def test_folder_label_can_hide_identical_count() -> None:
    entry = DirEntry(
        name="docs",
        relpath="docs",
        counts=FolderCounts(only_left=1, identical=9, metadata_only=2),
    )

    assert (
        _folder_label(entry, include_identical=False).plain
        == "docs  Left 1 | Metadata 2"
    )


def test_folder_label_orders_left_right_conflict_uncertain_and_metadata() -> None:
    entry = DirEntry(
        name="docs",
        relpath="docs",
        counts=FolderCounts(
            only_left=2,
            only_right=1,
            different=3,
            metadata_only=4,
            uncertain=5,
        ),
    )

    assert (
        _folder_label(entry, include_identical=False).plain
        == "docs  Left 2 | Right 1 | Conflict 3 | Uncertain 5 | Metadata 4"
    )


def test_folder_label_prefers_action_counts_when_present() -> None:
    entry = DirEntry(
        name="docs",
        relpath="docs",
        counts=FolderCounts(only_left=10, only_right=10),
    )

    assert (
        _folder_label(
            entry,
            action_counts=ActionCounts(left=20),
        ).plain
        == "docs  Left 20"
    )


def test_folder_label_falls_back_to_diff_counts_without_action_counts() -> None:
    entry = DirEntry(
        name="docs",
        relpath="docs",
        counts=FolderCounts(only_left=10, only_right=10),
    )

    assert _folder_label(entry, action_counts=ActionCounts()).plain == (
        "docs  Left 10 | Right 10"
    )


def test_folder_action_counts_roll_up_mixed_files_by_chosen_action() -> None:
    files_by_relpath = {
        "docs/left-1.txt": _mk_file_entry("only_left", relpath="docs/left-1.txt"),
        "docs/left-2.txt": _mk_file_entry("only_left", relpath="docs/left-2.txt"),
        "docs/right-1.txt": _mk_file_entry("only_right", relpath="docs/right-1.txt"),
        "docs/right-2.txt": _mk_file_entry("only_right", relpath="docs/right-2.txt"),
    }
    dir_files_map = {
        ".": list(files_by_relpath),
        "docs": list(files_by_relpath),
    }
    action_overrides = {relpath: "left_wins" for relpath in files_by_relpath}

    counts = _folder_action_counts_by_relpath(
        dir_files_map, files_by_relpath, action_overrides
    )

    assert counts["docs"] == ActionCounts(left=4)


def test_filtered_folder_counts_and_actions_include_only_visible_changes() -> None:
    files_by_relpath = {
        "docs/left.txt": _mk_file_entry("only_left", relpath="docs/left.txt"),
        "docs/meta.txt": _mk_file_entry(
            "identical", "different", relpath="docs/meta.txt"
        ),
        "docs/same.txt": _mk_file_entry("identical", relpath="docs/same.txt"),
    }
    dir_files_map = {".": list(files_by_relpath), "docs": list(files_by_relpath)}
    visible = {"docs/left.txt"}

    counts = _folder_counts_by_relpath(
        dir_files_map, files_by_relpath, included_changed_relpaths=visible
    )
    actions = _folder_action_counts_by_relpath(
        dir_files_map,
        files_by_relpath,
        {
            "docs/left.txt": "left_wins",
            "docs/meta.txt": "suggested",
        },
        included_relpaths=visible,
    )

    assert counts["docs"] == FolderCounts(only_left=1, identical=1)
    assert actions["docs"] == ActionCounts(left=1)


def test_file_label_uses_readable_badges() -> None:
    assert _file_label(_mk_file_entry("only_left")).plain == "a.txt  [Left] -"
    assert _file_label(_mk_file_entry("only_right")).plain == "a.txt  [Right] -"
    assert _file_label(_mk_file_entry("different")).plain == "a.txt  [Conflict] -"
    assert _file_label(_mk_file_entry("unknown")).plain == "a.txt  [Uncertain] -"
    assert (
        _file_label(_mk_file_entry("identical", "different")).plain
        == "a.txt  [Metadata] -"
    )


def test_file_label_prioritizes_size_for_content_conflicts() -> None:
    entry = _mk_file_entry(
        "different",
        "different",
        metadata_diff=["mtime"],
        left_size=10,
        right_size=11,
    )

    assert _file_label(entry).plain == "a.txt  [Conflict] size"
