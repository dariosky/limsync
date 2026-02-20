from __future__ import annotations

from pathlib import PurePosixPath

from limsync.excludes import (
    IgnoreRules,
    is_excluded_file_name,
    is_excluded_folder_name,
    load_ignore_rules_tree,
)
from limsync.remote_helper import IgnoreRules as RemoteIgnoreRules


def test_name_based_exclusions() -> None:
    assert is_excluded_folder_name("node_modules")
    assert is_excluded_folder_name(".tox")
    assert is_excluded_folder_name(".venv")
    assert is_excluded_folder_name(".limsync")
    assert is_excluded_folder_name("__pycache__")
    assert not is_excluded_folder_name("node_module")

    assert is_excluded_file_name(".DS_Store")
    assert is_excluded_file_name("Icon\r")
    assert not is_excluded_file_name("Icon")


def test_add_spec_ignores_comments_and_blank_lines() -> None:
    rules = IgnoreRules()
    rules.add_spec(
        PurePosixPath("."),
        ["", "   ", "# comment", "  # another", "*.tmp"],
    )

    assert rules.is_ignored(PurePosixPath("x.tmp"), is_dir=False)
    assert not rules.is_ignored(PurePosixPath("x.txt"), is_dir=False)


def test_nested_dropboxignore_scoping(tmp_path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / ".dropboxignore").write_text("*.tmp\n", encoding="utf-8")
    (root / "src").mkdir()
    (root / "src" / ".dropboxignore").write_text("cache\n", encoding="utf-8")

    rules = load_ignore_rules_tree(root)

    assert rules.is_ignored(PurePosixPath("a.tmp"), is_dir=False)
    assert rules.is_ignored(PurePosixPath("src/cache"), is_dir=True)
    assert not rules.is_ignored(PurePosixPath("other/cache"), is_dir=True)


def test_directory_pattern_with_is_dir_flag() -> None:
    rules = IgnoreRules()
    rules.add_spec(PurePosixPath("."), ["build/"])

    assert rules.is_ignored(PurePosixPath("build"), is_dir=True)
    assert rules.is_ignored(PurePosixPath("build/x.txt"), is_dir=False)


def test_load_tree_skips_excluded_folders(tmp_path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / ".dropboxignore").write_text("*.tmp\n", encoding="utf-8")

    nm = root / "node_modules"
    nm.mkdir()
    (nm / ".dropboxignore").write_text("*.js\n", encoding="utf-8")

    rules = load_ignore_rules_tree(root)

    assert "." in rules._patterns
    assert "node_modules" not in rules._patterns


def test_load_tree_missing_root_returns_empty_rules(tmp_path) -> None:
    rules = load_ignore_rules_tree(tmp_path / "missing")
    assert rules._patterns == {}


def test_remote_and_local_ignore_rules_match_behavior() -> None:
    local = IgnoreRules()
    remote = RemoteIgnoreRules()
    lines = ["*.tmp", "!keep.tmp", "build/", "/root-only.txt"]
    local.add_spec(PurePosixPath("."), lines)
    remote.add_spec(PurePosixPath("."), lines)

    checks = [
        (PurePosixPath("a.tmp"), False),
        (PurePosixPath("keep.tmp"), False),
        (PurePosixPath("x/build"), True),
        (PurePosixPath("x/build/nested.txt"), False),
        (PurePosixPath("root-only.txt"), False),
        (PurePosixPath("x/root-only.txt"), False),
    ]
    for relpath, is_dir in checks:
        assert local.is_ignored(relpath, is_dir=is_dir) == remote.is_ignored(
            relpath, is_dir=is_dir
        )
