from __future__ import annotations

import io
import json
import os
import stat

from limsync import remote_metadata_helper as helper
from limsync.remote_metadata_helper import process_request


def test_read_returns_only_requested_metadata(tmp_path) -> None:
    target = tmp_path / "x"
    target.write_text("x", encoding="utf-8")
    os.chmod(target, 0o640)

    response = process_request(
        "read",
        str(tmp_path),
        {"id": 1, "relpath": "x", "fields": ["mode"]},
    )

    assert response == {"id": 1, "relpath": "x", "mode": 0o640, "ok": True}


def test_apply_mode_only_preserves_mtime(tmp_path) -> None:
    target = tmp_path / "x"
    target.write_text("x", encoding="utf-8")
    os.chmod(target, 0o777)
    os.utime(target, ns=(10_000_000_000, 20_000_000_000))

    response = process_request(
        "apply",
        str(tmp_path),
        {"id": 1, "relpath": "x", "fields": ["mode"], "mode": 0o600},
    )

    assert response["ok"] is True
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert target.stat().st_mtime_ns == 20_000_000_000


def test_apply_mtime_only_preserves_mode_and_atime(tmp_path) -> None:
    target = tmp_path / "x"
    target.write_text("x", encoding="utf-8")
    os.chmod(target, 0o744)
    os.utime(target, ns=(10_000_000_000, 20_000_000_000))

    response = process_request(
        "apply",
        str(tmp_path),
        {
            "id": 1,
            "relpath": "x",
            "fields": ["mtime"],
            "mtime_ns": 30_000_000_000,
        },
    )

    assert response["ok"] is True
    target_stat = target.stat()
    assert stat.S_IMODE(target_stat.st_mode) == 0o744
    assert target_stat.st_atime_ns == 10_000_000_000
    assert target_stat.st_mtime_ns == 30_000_000_000


def test_symlink_is_a_successful_noop(tmp_path) -> None:
    (tmp_path / "link").symlink_to("missing")

    response = process_request(
        "apply",
        str(tmp_path),
        {"id": 1, "relpath": "link", "fields": ["mode"], "mode": 0o600},
    )

    assert response["ok"] is True
    assert response["noop"] is True


def test_rejects_paths_outside_root(tmp_path) -> None:
    response = process_request(
        "read",
        str(tmp_path),
        {"id": 1, "relpath": "../outside", "fields": ["mode"]},
    )

    assert response["ok"] is False
    assert "unsafe relative path" in str(response["error"])


def test_cancel_signal_does_not_interrupt_current_metadata_action(
    tmp_path, monkeypatch
) -> None:
    target = tmp_path / "x"
    target.write_text("x", encoding="utf-8")
    os.utime(target, ns=(10_000_000_000, 20_000_000_000))
    original_chmod = helper.os.chmod

    def cancelling_chmod(path, mode):
        original_chmod(path, mode)
        helper._request_cancel(0, None)

    monkeypatch.setattr(helper.os, "chmod", cancelling_chmod)
    response = process_request(
        "apply",
        str(tmp_path),
        {
            "id": 1,
            "relpath": "x",
            "fields": ["mode", "mtime"],
            "mode": 0o600,
            "mtime_ns": 30_000_000_000,
        },
    )

    assert response["ok"] is True
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert target.stat().st_mtime_ns == 30_000_000_000


def test_helper_stops_before_request_after_cancel(tmp_path, monkeypatch) -> None:
    for name in ("a", "b"):
        (tmp_path / name).write_text(name, encoding="utf-8")
    requests = "".join(
        json.dumps({"id": index, "relpath": name, "fields": ["mode"]}) + "\n"
        for index, name in enumerate(("a", "b"))
    )
    output = io.StringIO()
    original_process_request = helper.process_request
    processed: list[str] = []

    def cancelling_process_request(mode, root, request):
        response = original_process_request(mode, root, request)
        processed.append(str(request["relpath"]))
        helper._request_cancel(0, None)
        return response

    monkeypatch.setattr(helper, "process_request", cancelling_process_request)
    monkeypatch.setattr(helper.sys, "stdin", io.StringIO(requests))
    monkeypatch.setattr(helper.sys, "stdout", output)

    assert helper.run("read", str(tmp_path)) == 0
    events = [json.loads(line) for line in output.getvalue().splitlines()]

    assert processed == ["a"]
    assert events[-1] == {"event": "done", "processed": 1, "cancelled": True}
