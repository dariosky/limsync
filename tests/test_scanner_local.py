from __future__ import annotations

from pathlib import PurePosixPath

from limsync.scanner_local import LocalScanner


def test_scan_subtree_applies_ancestor_dropboxignore(tmp_path):
    root = tmp_path / "root"
    (root / "nested").mkdir(parents=True)
    (root / ".dropboxignore").write_text("skip.txt\n", encoding="utf-8")
    (root / "nested" / "skip.txt").write_text("x", encoding="utf-8")
    (root / "nested" / "keep.txt").write_text("y", encoding="utf-8")

    records = LocalScanner(root).scan(subtree=PurePosixPath("nested"))

    assert "nested/keep.txt" in records
    assert "nested/skip.txt" not in records


def test_scan_subtree_missing_path_returns_empty(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    (root / "a.txt").write_text("x", encoding="utf-8")

    records = LocalScanner(root).scan(subtree=PurePosixPath("missing/path"))

    assert records == {}
