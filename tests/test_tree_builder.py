from limsync.tree_builder import DirEntry, FileEntry, FolderCounts, _file_label, _folder_label


def _mk_file_entry(content_state: str, metadata_state: str = "identical") -> FileEntry:
    return FileEntry(
        relpath="a.txt",
        name="a.txt",
        content_state=content_state,
        metadata_state=metadata_state,
        metadata_diff=[],
        metadata_details=[],
        local_size=None,
        remote_size=None,
    )


def test_folder_label_hides_zero_counts_and_uses_readable_names() -> None:
    entry = DirEntry(
        name="docs",
        relpath="docs",
        counts=FolderCounts(
            only_local=2,
            only_remote=0,
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
        counts=FolderCounts(only_local=1, identical=9, metadata_only=2),
    )

    assert (
        _folder_label(entry, include_identical=False).plain
        == "docs  Left 1 | Metadata 2"
    )


def test_file_label_uses_readable_badges() -> None:
    assert _file_label(_mk_file_entry("only_local")).plain == "a.txt  [Left] -"
    assert _file_label(_mk_file_entry("only_remote")).plain == "a.txt  [Right] -"
    assert _file_label(_mk_file_entry("different")).plain == "a.txt  [Conflict] -"
    assert _file_label(_mk_file_entry("identical", "different")).plain == "a.txt  [Metadata] -"
