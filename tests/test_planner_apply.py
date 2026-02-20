from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from limsync.models import ContentState, MetadataState
from limsync.deletion_intent import DELETED_ON_LEFT, DELETED_ON_RIGHT
from limsync.planner_apply import (
    ACTION_LEFT_WINS,
    ACTION_RIGHT_WINS,
    ACTION_SUGGESTED,
    ApplySettings,
    PlanOperation,
    _ensure_remote_parent_cached,
    _infer_metadata_source_from_details,
    _join_remote,
    _remote_atime_ns,
    _remote_expand_root,
    _remote_mtime_ns,
    build_plan_operations,
    execute_plan,
    parse_remote_address,
    summarize_operations,
)

from conftest import (
    DummyAutoAddPolicy,
    FakeSFTPClient,
    FakeSSHClient,
    make_remote_stat,
    mk_diff,
)


def _ops_set(ops: list[PlanOperation]) -> set[tuple[str, str]]:
    return {(op.kind, op.relpath) for op in ops}


def test_parse_remote_address_valid_invalid() -> None:
    assert parse_remote_address("user@host:~/Dropbox") == ("user", "host", "~/Dropbox")

    with pytest.raises(ValueError):
        parse_remote_address("bad-address")


def test_helpers_join_and_remote_time_conversion() -> None:
    assert _join_remote("/root/", "a/b.txt") == "/root/a/b.txt"
    st = make_remote_stat(atime=1.5, mtime=2.25)
    assert _remote_atime_ns(st) == 1_500_000_000
    assert _remote_mtime_ns(st) == 2_250_000_000


def test_infer_metadata_source_from_details_mode_then_mtime() -> None:
    mode_diff = mk_diff(
        "x",
        content_state=ContentState.IDENTICAL,
        metadata_state=MetadataState.DIFFERENT,
        metadata_details=("mode: left=0x777 right=0x600",),
    )
    assert _infer_metadata_source_from_details(mode_diff) == "right"

    mtime_diff = mk_diff(
        "x",
        content_state=ContentState.IDENTICAL,
        metadata_state=MetadataState.DIFFERENT,
        metadata_details=(
            "mtime: left=2024-01-01 00:00:00.000000 UTC right=2024-01-02 00:00:00.000000 UTC",
        ),
    )
    assert _infer_metadata_source_from_details(mtime_diff) == "left"


def test_build_plan_operations_default_ignore_no_ops() -> None:
    diffs = [mk_diff("a", content_state=ContentState.ONLY_LEFT)]
    assert build_plan_operations(diffs, {}) == []


def test_build_plan_only_left_only_right_directions() -> None:
    diffs = [
        mk_diff("left", content_state=ContentState.ONLY_LEFT),
        mk_diff("right", content_state=ContentState.ONLY_RIGHT),
    ]

    assert _ops_set(
        build_plan_operations(
            diffs,
            {"left": ACTION_SUGGESTED, "right": ACTION_SUGGESTED},
        )
    ) == {("copy_right", "left"), ("copy_left", "right")}

    assert _ops_set(
        build_plan_operations(
            diffs,
            {"left": ACTION_RIGHT_WINS, "right": ACTION_LEFT_WINS},
        )
    ) == {("delete_left", "left"), ("delete_right", "right")}


def test_build_plan_suggested_honors_intentional_deletion_hints() -> None:
    diffs = [
        mk_diff(
            "left_deleted",
            content_state=ContentState.ONLY_RIGHT,
            metadata_source=DELETED_ON_LEFT,
        ),
        mk_diff(
            "right_deleted",
            content_state=ContentState.ONLY_LEFT,
            metadata_source=DELETED_ON_RIGHT,
        ),
    ]

    assert _ops_set(
        build_plan_operations(
            diffs,
            {
                "left_deleted": ACTION_SUGGESTED,
                "right_deleted": ACTION_SUGGESTED,
            },
        )
    ) == {
        ("delete_right", "left_deleted"),
        ("delete_left", "right_deleted"),
    }


def test_build_plan_different_unknown_and_metadata_rules() -> None:
    diffs = [
        mk_diff(
            "conflict",
            content_state=ContentState.DIFFERENT,
            metadata_state=MetadataState.DIFFERENT,
            metadata_source="left",
        ),
        mk_diff(
            "uncertain",
            content_state=ContentState.UNKNOWN,
            metadata_state=MetadataState.DIFFERENT,
            metadata_source="right",
        ),
    ]

    assert _ops_set(build_plan_operations(diffs, {"conflict": ACTION_SUGGESTED})) == set()

    uncertain_ops = _ops_set(build_plan_operations(diffs, {"uncertain": ACTION_SUGGESTED}))
    assert uncertain_ops == {("metadata_update_left", "uncertain")}

    both = _ops_set(
        build_plan_operations(
            diffs,
            {"conflict": ACTION_LEFT_WINS, "uncertain": ACTION_RIGHT_WINS},
        )
    )
    assert both == {
        ("copy_right", "conflict"),
        ("metadata_update_right", "conflict"),
        ("copy_left", "uncertain"),
        ("metadata_update_left", "uncertain"),
    }


def test_summarize_operations_counts() -> None:
    ops = [
        PlanOperation("delete_left", "a"),
        PlanOperation("delete_right", "b"),
        PlanOperation("copy_left", "c"),
        PlanOperation("copy_right", "d"),
        PlanOperation("metadata_update_left", "e"),
        PlanOperation("metadata_update_right", "f"),
    ]
    summary = summarize_operations(ops)
    assert summary.delete_left == 1
    assert summary.delete_right == 1
    assert summary.copy_left == 1
    assert summary.copy_right == 1
    assert summary.metadata_update_left == 1
    assert summary.metadata_update_right == 1
    assert summary.total == 6


def test_execute_plan_empty_returns_immediately(tmp_path, monkeypatch) -> None:
    class BoomClient:
        def __init__(self):
            raise AssertionError("SSH should not be created for empty operations")

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", BoomClient)

    result = execute_plan(tmp_path, "u@h:/r", [])
    assert result.total_operations == 0
    assert result.succeeded_operations == 0
    assert result.errors == []


def test_execute_plan_copy_right_and_delete_right(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    src = local_root / "a.txt"
    src.write_text("hello", encoding="utf-8")
    os.chmod(src, 0o640)

    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")

    progress = []
    result = execute_plan(
        local_root,
        "u@h:~/x",
        [PlanOperation("copy_right", "a.txt"), PlanOperation("delete_right", "a.txt")],
        progress_cb=lambda done, total, op, ok, err: progress.append((done, total, op.kind, ok, err)),
    )

    assert result.total_operations == 2
    assert result.succeeded_operations == 2
    assert result.errors == []
    assert result.completed_paths == {"a.txt"}
    assert ("copy_right", "a.txt") in result.succeeded_operation_keys
    assert progress[-1][0] == 2
    assert progress[-1][1] == 2


def test_execute_plan_copy_right_symlink_rewrites_in_root_target(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    (local_root / "docs").mkdir(parents=True)
    (local_root / "nested").mkdir(parents=True)
    (local_root / "docs" / "x.txt").write_text("x", encoding="utf-8")
    (local_root / "nested" / "link").symlink_to(str(local_root / "docs" / "x.txt"))

    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")
    monkeypatch.setattr(pa, "_remote_expand_home", lambda _client: "/home/dario")

    result = execute_plan(
        local_root,
        "u@h:~/x",
        [PlanOperation("copy_right", "nested/link")],
    )

    assert result.errors == []
    assert result.succeeded_operations == 1
    assert sftp.remote_symlinks["/remote/nested/link"] == "../docs/x.txt"


def test_ensure_remote_parent_cached_reuses_known_dirs() -> None:
    sftp = FakeSFTPClient()
    sftp.existing_dirs.update({"/remote"})
    known_dirs = {"/", "/remote"}

    _ensure_remote_parent_cached(
        sftp,
        "/remote/a/b/c.txt",
        known_dirs=known_dirs,
    )
    first_stats = [call for call in sftp.calls if call[0] == "stat"]
    assert len(first_stats) == 2
    assert "/remote/a" in known_dirs
    assert "/remote/a/b" in known_dirs

    sftp.calls.clear()
    _ensure_remote_parent_cached(
        sftp,
        "/remote/a/b/d.txt",
        known_dirs=known_dirs,
    )
    second_stats = [call for call in sftp.calls if call[0] == "stat"]
    assert second_stats == []


def test_execute_plan_copy_left_and_delete_left(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    to_delete = local_root / "gone.txt"
    to_delete.write_text("bye", encoding="utf-8")

    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    sftp.remote_files["/remote/b.txt"] = b"remote-content"
    sftp.remote_stats["/remote/b.txt"] = make_remote_stat(mode=0o100600, atime=5.0, mtime=10.0)
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")

    result = execute_plan(
        local_root,
        "u@h:~/x",
        [PlanOperation("copy_left", "b.txt"), PlanOperation("delete_left", "gone.txt")],
    )

    target = local_root / "b.txt"
    assert target.read_bytes() == b"remote-content"
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert not to_delete.exists()
    assert result.total_operations == 2
    assert result.succeeded_operations == 2


def test_execute_plan_copy_left_symlink_rewrites_in_root_target(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    (local_root / "nested").mkdir(parents=True)

    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    sftp.remote_symlinks["/remote/nested/link"] = "/remote/docs/x.txt"
    sftp.remote_lstats["/remote/nested/link"] = make_remote_stat(
        mode=stat.S_IFLNK | 0o777, atime=1.0, mtime=1.0
    )
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")
    monkeypatch.setattr(pa, "_remote_expand_home", lambda _client: "/home/dario")

    result = execute_plan(
        local_root,
        "u@h:~/x",
        [PlanOperation("copy_left", "nested/link")],
    )

    assert result.errors == []
    assert result.succeeded_operations == 1
    link = local_root / "nested" / "link"
    assert link.is_symlink()
    assert os.readlink(link) == "../docs/x.txt"


def test_execute_plan_copy_left_symlink_rewrites_home_prefix(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()

    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    sftp.remote_symlinks["/remote/link"] = "/home/dario/Outside/file.txt"
    sftp.remote_lstats["/remote/link"] = make_remote_stat(
        mode=stat.S_IFLNK | 0o777, atime=1.0, mtime=1.0
    )
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")
    monkeypatch.setattr(pa, "_remote_expand_home", lambda _client: "/home/dario")

    result = execute_plan(
        local_root,
        "u@h:~/x",
        [PlanOperation("copy_left", "link")],
    )

    assert result.errors == []
    assert result.succeeded_operations == 1
    expected = str(Path.home().expanduser().resolve() / "Outside" / "file.txt")
    assert os.readlink(local_root / "link") == expected


def test_execute_plan_put_uses_confirm_false_by_default(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    src = local_root / "a.txt"
    src.write_text("hello", encoding="utf-8")

    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")

    execute_plan(
        local_root,
        "u@h:~/x",
        [PlanOperation("copy_right", "a.txt")],
    )

    put_calls = [call for call in sftp.calls if call[0] == "put"]
    assert put_calls
    assert put_calls[0][-1] is False


def test_execute_plan_connect_respects_ssh_compression(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")

    execute_plan(
        local_root,
        "u@h:~/x",
        [PlanOperation("delete_right", "missing.txt")],
        settings=ApplySettings(ssh_compression=True),
    )

    assert ssh.connect_calls
    assert ssh.connect_calls[0]["compress"] is True


def test_execute_plan_metadata_bidirectional_uses_restrictive_and_oldest(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    p = local_root / "x.txt"
    p.write_text("x", encoding="utf-8")
    os.chmod(p, 0o777)
    os.utime(p, ns=(300_000_000_000, 200_000_000_000))

    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    sftp.remote_stats["/remote/x.txt"] = make_remote_stat(mode=0o100600, atime=50.0, mtime=100.0)
    sftp.remote_lstats["/remote/x.txt"] = make_remote_stat(mode=0o100600, atime=50.0, mtime=100.0)
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")

    result = execute_plan(
        local_root,
        "u@h:~/x",
        [
            PlanOperation("metadata_update_left", "x.txt"),
            PlanOperation("metadata_update_right", "x.txt"),
        ],
    )

    assert result.errors == []
    assert stat.S_IMODE(p.stat().st_mode) == 0o600
    assert p.stat().st_mtime_ns == 100_000_000_000

    remote = sftp.remote_stats["/remote/x.txt"]
    assert stat.S_IMODE(remote.st_mode) == 0o600
    assert int(remote.st_mtime) == 100


def test_execute_plan_metadata_symlink_is_noop(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    link = local_root / "l"
    link.symlink_to("missing-target")

    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    sftp.remote_lstats["/remote/l"] = make_remote_stat(mode=stat.S_IFLNK | 0o777, atime=1.0, mtime=1.0)
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")

    progress = []
    result = execute_plan(
        local_root,
        "u@h:~/x",
        [PlanOperation("metadata_update_right", "l")],
        progress_cb=lambda done, total, op, ok, err: progress.append((done, total, op.kind, ok, err)),
    )

    assert result.errors == []
    assert result.succeeded_operations == 1
    assert progress == [(1, 1, "metadata_update_right", True, None)]
    assert not any(call[0] == "chmod" for call in sftp.calls)


def test_execute_plan_partial_failures_and_completed_paths(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "local"
    local_root.mkdir()
    ok = local_root / "ok.txt"
    ok.write_text("ok", encoding="utf-8")

    sftp = FakeSFTPClient()
    sftp.existing_dirs.add("/remote")
    ssh = FakeSSHClient(sftp)

    from limsync import planner_apply as pa

    monkeypatch.setattr(pa.paramiko, "SSHClient", lambda: ssh)
    monkeypatch.setattr(pa.paramiko, "AutoAddPolicy", DummyAutoAddPolicy)
    monkeypatch.setattr(pa, "_remote_expand_root", lambda _client, _root: "/remote")

    result = execute_plan(
        local_root,
        "u@h:~/x",
        [PlanOperation("delete_left", "missing.txt"), PlanOperation("copy_right", "ok.txt")],
    )

    assert result.total_operations == 2
    assert result.succeeded_operations == 1
    assert len(result.errors) == 1
    assert result.errors[0].startswith("delete_left missing.txt:")
    assert result.completed_paths == {"ok.txt"}
    assert ("copy_right", "ok.txt") in result.succeeded_operation_keys


def test_remote_expand_root_success_and_error() -> None:
    sftp = FakeSFTPClient()
    ssh_ok = FakeSSHClient(sftp, expand_stdout="/expanded/path\n")
    assert _remote_expand_root(ssh_ok, "~/Dropbox") == "/expanded/path"

    ssh_fail = FakeSSHClient(sftp, expand_stdout="", expand_stderr="boom")
    with pytest.raises(RuntimeError):
        _remote_expand_root(ssh_fail, "~/Dropbox")
