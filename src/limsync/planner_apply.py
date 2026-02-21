from __future__ import annotations

import os
import re
import shutil
import stat
import tempfile
import time
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import paramiko

from .config import DEFAULT_REMOTE_PORT
from .deletion_intent import DELETED_ON_LEFT, DELETED_ON_RIGHT
from .endpoints import EndpointSpec, parse_endpoint, parse_legacy_remote_address
from .models import ContentState, DiffRecord, MetadataState
from .ssh_pool import pooled_ssh_client
from .symlink_utils import map_symlink_target_for_destination

ACTION_LEFT_WINS = "left_wins"
ACTION_RIGHT_WINS = "right_wins"
ACTION_IGNORE = "ignore"
ACTION_SUGGESTED = "suggested"

type StatLike = os.stat_result | paramiko.SFTPAttributes


@dataclass(frozen=True)
class PlanOperation:
    kind: str
    relpath: str


@dataclass(frozen=True)
class PlanSummary:
    delete_left: int = 0
    delete_right: int = 0
    copy_left: int = 0
    copy_right: int = 0
    metadata_update_left: int = 0
    metadata_update_right: int = 0

    @property
    def total(self) -> int:
        return (
            self.delete_left
            + self.delete_right
            + self.copy_left
            + self.copy_right
            + self.metadata_update_left
            + self.metadata_update_right
        )


@dataclass(frozen=True)
class ExecuteResult:
    completed_paths: set[str]
    errors: list[str]
    succeeded_operations: int
    total_operations: int
    succeeded_operation_keys: frozenset[tuple[str, str]] = field(
        default_factory=frozenset
    )
    operation_counts: dict[str, int] = field(default_factory=dict)
    operation_seconds: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ApplySettings:
    ssh_compression: bool = False
    sftp_put_confirm: bool = False
    progress_emit_every_ops: int = 100
    progress_emit_every_ms: int = 200


@dataclass
class _RemoteRuntime:
    client: paramiko.SSHClient
    sftp: paramiko.SFTPClient
    root: str
    home: str
    user: str
    host: str
    port: int


@dataclass
class _SideRuntime:
    endpoint: EndpointSpec
    local_root: Path | None
    local_home: Path
    remote: _RemoteRuntime | None

    @property
    def is_local(self) -> bool:
        return self.endpoint.is_local

    @property
    def root_text(self) -> str:
        if self.is_local:
            assert self.local_root is not None
            return str(self.local_root)
        assert self.remote is not None
        return self.remote.root

    @property
    def home_text(self) -> str:
        if self.is_local:
            return str(self.local_home)
        assert self.remote is not None
        return self.remote.home


def parse_remote_address(remote_address: str) -> tuple[str, str, str]:
    parsed = parse_legacy_remote_address(remote_address)
    return parsed.user, parsed.host, parsed.root


def _infer_metadata_source_from_details(diff: DiffRecord) -> str | None:
    mode_re = re.compile(r"mode:\s+left=0x([0-7]{3})\s+right=0x([0-7]{3})")
    mtime_re = re.compile(r"mtime:\s+left=(.*?)\s+right=(.*?)$")

    for detail in diff.metadata_details:
        mode_match = mode_re.match(detail)
        if mode_match:
            left_mode = int(mode_match.group(1), 8)
            right_mode = int(mode_match.group(2), 8)
            if left_mode != right_mode:
                return "left" if left_mode < right_mode else "right"

    for detail in diff.metadata_details:
        mtime_match = mtime_re.match(detail)
        if mtime_match:
            left_mtime = datetime.strptime(
                mtime_match.group(1), "%Y-%m-%d %H:%M:%S.%f UTC"
            )
            right_mtime = datetime.strptime(
                mtime_match.group(2), "%Y-%m-%d %H:%M:%S.%f UTC"
            )
            if left_mtime != right_mtime:
                return "left" if left_mtime < right_mtime else "right"

    return None


def _suggested_metadata_op(relpath: str, diff: DiffRecord) -> list[PlanOperation]:
    source = diff.metadata_source or _infer_metadata_source_from_details(diff)
    if source == "left":
        return [PlanOperation("metadata_update_right", relpath)]
    if source == "right":
        return [PlanOperation("metadata_update_left", relpath)]
    return []


def _metadata_ops(relpath: str, action: str, diff: DiffRecord) -> list[PlanOperation]:
    if diff.metadata_state != MetadataState.DIFFERENT:
        return []
    if action == ACTION_LEFT_WINS:
        return [PlanOperation("metadata_update_right", relpath)]
    if action == ACTION_RIGHT_WINS:
        return [PlanOperation("metadata_update_left", relpath)]
    if action == ACTION_SUGGESTED:
        return _suggested_metadata_op(relpath, diff)
    return []


def build_plan_operations(
    diffs: list[DiffRecord],
    action_overrides: dict[str, str],
) -> list[PlanOperation]:
    ops: list[PlanOperation] = []
    for diff in diffs:
        action = action_overrides.get(diff.relpath, ACTION_IGNORE)
        if action == ACTION_IGNORE:
            continue

        if diff.content_state == ContentState.ONLY_LEFT:
            if action in {ACTION_LEFT_WINS, ACTION_SUGGESTED}:
                if (
                    action == ACTION_SUGGESTED
                    and diff.metadata_source == DELETED_ON_RIGHT
                ):
                    ops.append(PlanOperation("delete_left", diff.relpath))
                else:
                    ops.append(PlanOperation("copy_right", diff.relpath))
            elif action == ACTION_RIGHT_WINS:
                ops.append(PlanOperation("delete_left", diff.relpath))
            continue

        if diff.content_state == ContentState.ONLY_RIGHT:
            if action in {ACTION_RIGHT_WINS, ACTION_SUGGESTED}:
                if (
                    action == ACTION_SUGGESTED
                    and diff.metadata_source == DELETED_ON_LEFT
                ):
                    ops.append(PlanOperation("delete_right", diff.relpath))
                else:
                    ops.append(PlanOperation("copy_left", diff.relpath))
            elif action == ACTION_LEFT_WINS:
                ops.append(PlanOperation("delete_right", diff.relpath))
            continue

        if diff.content_state in {ContentState.DIFFERENT, ContentState.UNKNOWN}:
            if action == ACTION_LEFT_WINS:
                ops.append(PlanOperation("copy_right", diff.relpath))
            elif action == ACTION_RIGHT_WINS:
                ops.append(PlanOperation("copy_left", diff.relpath))
            if (
                diff.content_state == ContentState.DIFFERENT
                and action == ACTION_SUGGESTED
            ):
                continue

        ops.extend(_metadata_ops(diff.relpath, action, diff))

    dedup = {(op.kind, op.relpath): op for op in ops}
    return list(dedup.values())


def summarize_operations(ops: list[PlanOperation]) -> PlanSummary:
    counts = {
        "delete_left": 0,
        "delete_right": 0,
        "copy_left": 0,
        "copy_right": 0,
        "metadata_update_left": 0,
        "metadata_update_right": 0,
    }
    for op in ops:
        if op.kind in counts:
            counts[op.kind] += 1
    return PlanSummary(**counts)


def _ensure_local_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_remote_parent(sftp: paramiko.SFTPClient, remote_path: str) -> None:
    _ensure_remote_parent_cached(sftp, remote_path, known_dirs=None)


def _ensure_remote_parent_cached(
    sftp: paramiko.SFTPClient,
    remote_path: str,
    known_dirs: set[str] | None,
) -> None:
    parent = os.path.dirname(remote_path)
    if not parent:
        return
    parts = []
    while parent and parent != "/":
        parts.append(parent)
        parent = os.path.dirname(parent)
    for segment in reversed(parts):
        if known_dirs is not None and segment in known_dirs:
            continue
        try:
            sftp.stat(segment)
        except OSError:
            sftp.mkdir(segment)
        if known_dirs is not None:
            known_dirs.add(segment)


def _join_remote(root: str, relpath: str) -> str:
    return f"{root.rstrip('/')}/{relpath}"


def _remote_mtime_ns(st: StatLike) -> int:
    return int(float(getattr(st, "st_mtime", 0)) * 1_000_000_000)


def _remote_atime_ns(st: StatLike) -> int:
    return int(float(getattr(st, "st_atime", 0)) * 1_000_000_000)


def _apply_remote_metadata_from_local(
    sftp: paramiko.SFTPClient,
    remote_path: str,
    local_stat: os.stat_result,
) -> None:
    mode = stat.S_IMODE(local_stat.st_mode)
    sftp.chmod(remote_path, mode)
    sftp.utime(
        remote_path,
        (
            int(local_stat.st_atime_ns / 1_000_000_000),
            int(local_stat.st_mtime_ns / 1_000_000_000),
        ),
    )


def _apply_remote_metadata_from_remote(
    sftp: paramiko.SFTPClient,
    remote_path: str,
    remote_stat: StatLike,
) -> None:
    mode = stat.S_IMODE(getattr(remote_stat, "st_mode", 0))
    sftp.chmod(remote_path, mode)
    sftp.utime(
        remote_path,
        (
            int(float(getattr(remote_stat, "st_atime", 0))),
            int(float(getattr(remote_stat, "st_mtime", 0))),
        ),
    )


def _apply_local_metadata_from_remote(
    local_path: Path,
    remote_stat: StatLike,
) -> None:
    mode = stat.S_IMODE(getattr(remote_stat, "st_mode", 0))
    os.chmod(local_path, mode)
    os.utime(
        local_path,
        ns=(_remote_atime_ns(remote_stat), _remote_mtime_ns(remote_stat)),
    )


def _apply_local_metadata_from_local(
    local_path: Path,
    source_stat: os.stat_result,
) -> None:
    mode = stat.S_IMODE(source_stat.st_mode)
    os.chmod(local_path, mode)
    os.utime(local_path, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))


def _remote_expand_root(client: paramiko.SSHClient, root: str) -> str:
    quoted = root.replace("'", "'\\''")
    command = f"python3 -c \"import os; print(os.path.expanduser('{quoted}'))\""
    _stdin, stdout, stderr = client.exec_command(command)
    out = stdout.read().decode("utf-8", errors="replace").strip()
    err = stderr.read().decode("utf-8", errors="replace").strip()
    if out:
        return out
    raise RuntimeError(f"Failed to resolve remote root {root!r}: {err}")


def _remote_expand_home(client: paramiko.SSHClient) -> str:
    command = "python3 -c \"import os; print(os.path.expanduser('~'))\""
    _stdin, stdout, stderr = client.exec_command(command)
    out = stdout.read().decode("utf-8", errors="replace").strip()
    err = stderr.read().decode("utf-8", errors="replace").strip()
    if out:
        return out
    raise RuntimeError(f"Failed to resolve remote home: {err}")


def _unlink_local_if_exists(path: Path) -> None:
    if path.is_symlink() or path.exists():
        path.unlink()


def _remove_remote_if_exists(sftp: paramiko.SFTPClient, path: str) -> None:
    try:
        sftp.remove(path)
    except OSError:
        return


def _coerce_endpoint(value: EndpointSpec | str | Path) -> EndpointSpec:
    if isinstance(value, EndpointSpec):
        return value
    if isinstance(value, Path):
        return EndpointSpec(kind="local", root=str(value.expanduser().resolve()))
    return parse_endpoint(str(value))


def _remote_runtime(
    stack: ExitStack,
    endpoint: EndpointSpec,
    *,
    compress: bool,
) -> _RemoteRuntime:
    assert endpoint.is_remote
    client = stack.enter_context(
        pooled_ssh_client(
            host=str(endpoint.host),
            user=str(endpoint.user),
            port=endpoint.port or DEFAULT_REMOTE_PORT,
            compress=compress,
            timeout=10,
            client_factory=paramiko.SSHClient,
            auto_add_policy_factory=paramiko.AutoAddPolicy,
        )
    )
    root = _remote_expand_root(client, endpoint.root)
    try:
        home = _remote_expand_home(client)
    except Exception:
        home = f"/home/{endpoint.user}"
    sftp = client.open_sftp()
    stack.callback(sftp.close)
    return _RemoteRuntime(
        client=client,
        sftp=sftp,
        root=root,
        home=home,
        user=str(endpoint.user),
        host=str(endpoint.host),
        port=endpoint.port or DEFAULT_REMOTE_PORT,
    )


def _side_runtime(
    stack: ExitStack,
    endpoint: EndpointSpec,
    *,
    compress: bool,
    local_home: Path,
) -> _SideRuntime:
    if endpoint.is_local:
        return _SideRuntime(
            endpoint=endpoint,
            local_root=Path(endpoint.root).expanduser().resolve(),
            local_home=local_home,
            remote=None,
        )
    remote = _remote_runtime(stack, endpoint, compress=compress)
    return _SideRuntime(
        endpoint=endpoint,
        local_root=None,
        local_home=local_home,
        remote=remote,
    )


def _side_path(side: _SideRuntime, relpath: str) -> Path | str:
    if side.is_local:
        assert side.local_root is not None
        return side.local_root / relpath
    assert side.remote is not None
    return _join_remote(side.remote.root, relpath)


def _side_lstat(side: _SideRuntime, relpath: str) -> StatLike:
    if side.is_local:
        path = _side_path(side, relpath)
        assert isinstance(path, Path)
        return path.lstat()
    assert side.remote is not None
    path = _side_path(side, relpath)
    assert isinstance(path, str)
    return side.remote.sftp.lstat(path)


def _side_stat(side: _SideRuntime, relpath: str) -> StatLike:
    if side.is_local:
        path = _side_path(side, relpath)
        assert isinstance(path, Path)
        return path.stat()
    assert side.remote is not None
    path = _side_path(side, relpath)
    assert isinstance(path, str)
    return side.remote.sftp.stat(path)


def _side_remove_file(side: _SideRuntime, relpath: str) -> None:
    if side.is_local:
        path = _side_path(side, relpath)
        assert isinstance(path, Path)
        path.unlink()
        return
    assert side.remote is not None
    path = _side_path(side, relpath)
    assert isinstance(path, str)
    side.remote.sftp.remove(path)


def _side_remove_if_exists(side: _SideRuntime, relpath: str) -> None:
    if side.is_local:
        path = _side_path(side, relpath)
        assert isinstance(path, Path)
        _unlink_local_if_exists(path)
        return
    assert side.remote is not None
    path = _side_path(side, relpath)
    assert isinstance(path, str)
    _remove_remote_if_exists(side.remote.sftp, path)


def _side_readlink(side: _SideRuntime, relpath: str) -> str:
    if side.is_local:
        path = _side_path(side, relpath)
        assert isinstance(path, Path)
        return os.readlink(path)
    assert side.remote is not None
    path = _side_path(side, relpath)
    assert isinstance(path, str)
    return side.remote.sftp.readlink(path)


def _side_symlink(side: _SideRuntime, target: str, relpath: str) -> None:
    if side.is_local:
        path = _side_path(side, relpath)
        assert isinstance(path, Path)
        os.symlink(target, path)
        return
    assert side.remote is not None
    path = _side_path(side, relpath)
    assert isinstance(path, str)
    side.remote.sftp.symlink(target, path)


def _side_ensure_parent(
    side: _SideRuntime,
    relpath: str,
    *,
    known_remote_dirs: set[str] | None,
) -> None:
    if side.is_local:
        path = _side_path(side, relpath)
        assert isinstance(path, Path)
        _ensure_local_parent(path)
        return
    assert side.remote is not None
    path = _side_path(side, relpath)
    assert isinstance(path, str)
    _ensure_remote_parent_cached(side.remote.sftp, path, known_dirs=known_remote_dirs)


def _local_mode(st_obj: StatLike) -> int:
    return stat.S_IMODE(getattr(st_obj, "st_mode", 0))


def _local_mtime_ns(st_obj: StatLike) -> int:
    if hasattr(st_obj, "st_mtime_ns"):
        return int(getattr(st_obj, "st_mtime_ns"))
    return _remote_mtime_ns(st_obj)


def _is_symlink(st_obj: StatLike) -> bool:
    return stat.S_ISLNK(getattr(st_obj, "st_mode", 0))


def _copy_between(
    source_side: _SideRuntime,
    destination_side: _SideRuntime,
    relpath: str,
    *,
    settings: ApplySettings,
    known_remote_dirs: dict[int, set[str]],
) -> None:
    source_lstat = _side_lstat(source_side, relpath)

    dst_known_dirs = None
    if not destination_side.is_local and destination_side.remote is not None:
        dst_known_dirs = known_remote_dirs.get(id(destination_side.remote.sftp))

    _side_ensure_parent(
        destination_side,
        relpath,
        known_remote_dirs=dst_known_dirs,
    )

    if _is_symlink(source_lstat):
        source_target = _side_readlink(source_side, relpath)
        mapped_target = map_symlink_target_for_destination(
            source_root=Path(source_side.root_text),
            source_home=Path(source_side.home_text),
            source_relpath=relpath,
            source_target=source_target,
            destination_root=Path(destination_side.root_text),
            destination_home=Path(destination_side.home_text),
            destination_relpath=relpath,
        )
        _side_remove_if_exists(destination_side, relpath)
        _side_symlink(destination_side, mapped_target, relpath)
        return

    src_path = _side_path(source_side, relpath)
    dst_path = _side_path(destination_side, relpath)

    if source_side.is_local and destination_side.is_local:
        assert isinstance(src_path, Path)
        assert isinstance(dst_path, Path)
        shutil.copyfile(src_path, dst_path)
        _apply_local_metadata_from_local(dst_path, source_lstat)
        return

    if source_side.is_local and not destination_side.is_local:
        assert isinstance(src_path, Path)
        assert isinstance(dst_path, str)
        assert destination_side.remote is not None
        destination_side.remote.sftp.put(
            str(src_path),
            dst_path,
            confirm=settings.sftp_put_confirm,
        )
        _apply_remote_metadata_from_local(
            destination_side.remote.sftp,
            dst_path,
            source_lstat,
        )
        return

    if not source_side.is_local and destination_side.is_local:
        assert isinstance(src_path, str)
        assert isinstance(dst_path, Path)
        assert source_side.remote is not None
        source_stat = _side_stat(source_side, relpath)
        source_side.remote.sftp.get(src_path, str(dst_path))
        _apply_local_metadata_from_remote(dst_path, source_stat)
        return

    assert isinstance(src_path, str)
    assert isinstance(dst_path, str)
    assert source_side.remote is not None
    assert destination_side.remote is not None

    source_stat = _side_stat(source_side, relpath)
    with tempfile.NamedTemporaryFile(prefix="limsync-r2r-", delete=False) as handle:
        tmp_path = Path(handle.name)
    try:
        source_side.remote.sftp.get(src_path, str(tmp_path))
        destination_side.remote.sftp.put(
            str(tmp_path),
            dst_path,
            confirm=settings.sftp_put_confirm,
        )
        _apply_remote_metadata_from_remote(
            destination_side.remote.sftp,
            dst_path,
            source_stat,
        )
    finally:
        tmp_path.unlink(missing_ok=True)


def _apply_metadata_from_right_to_left(
    left_side: _SideRuntime,
    right_side: _SideRuntime,
    relpath: str,
    *,
    both_directions: bool,
) -> None:
    left_stat = _side_lstat(left_side, relpath)
    right_stat = _side_lstat(right_side, relpath)

    if _is_symlink(left_stat) or _is_symlink(right_stat):
        return

    target_mode = (
        min(_local_mode(left_stat), _local_mode(right_stat))
        if both_directions
        else None
    )
    target_mtime_ns = (
        min(_local_mtime_ns(left_stat), _local_mtime_ns(right_stat))
        if both_directions
        else None
    )

    mode = target_mode if target_mode is not None else _local_mode(right_stat)
    mtime_ns = (
        target_mtime_ns if target_mtime_ns is not None else _local_mtime_ns(right_stat)
    )

    left_path = _side_path(left_side, relpath)
    if left_side.is_local:
        assert isinstance(left_path, Path)
        os.chmod(left_path, mode)
        os.utime(left_path, ns=(int(getattr(left_stat, "st_atime_ns")), mtime_ns))
    else:
        assert isinstance(left_path, str)
        assert left_side.remote is not None
        left_side.remote.sftp.chmod(left_path, mode)
        left_side.remote.sftp.utime(
            left_path,
            (
                int(float(getattr(left_stat, "st_atime", 0))),
                int(mtime_ns / 1_000_000_000),
            ),
        )


def _apply_metadata_from_left_to_right(
    left_side: _SideRuntime,
    right_side: _SideRuntime,
    relpath: str,
    *,
    both_directions: bool,
) -> None:
    _apply_metadata_from_right_to_left(
        right_side,
        left_side,
        relpath,
        both_directions=both_directions,
    )


def execute_plan(
    source: EndpointSpec | str | Path,
    destination: EndpointSpec | str | Path,
    operations: list[PlanOperation],
    progress_cb: Callable[[int, int, PlanOperation, bool, str | None], None]
    | None = None,
    settings: ApplySettings | None = None,
) -> ExecuteResult:
    source_endpoint = _coerce_endpoint(source)
    destination_endpoint = _coerce_endpoint(destination)
    local_home = Path.home().expanduser().resolve()
    resolved_settings = settings or ApplySettings()

    if not operations:
        return ExecuteResult(
            completed_paths=set(),
            errors=[],
            succeeded_operations=0,
            total_operations=0,
        )

    path_ops: dict[str, list[PlanOperation]] = {}
    for op in operations:
        path_ops.setdefault(op.relpath, []).append(op)

    errors: list[str] = []
    succeeded: set[tuple[str, str]] = set()
    done_count = 0
    total = len(operations)
    op_counts: dict[str, int] = {}
    op_seconds: dict[str, float] = {}

    with ExitStack() as stack:
        left_side = _side_runtime(
            stack,
            source_endpoint,
            compress=resolved_settings.ssh_compression,
            local_home=local_home,
        )
        right_side = _side_runtime(
            stack,
            destination_endpoint,
            compress=resolved_settings.ssh_compression,
            local_home=local_home,
        )

        known_remote_dirs: dict[int, set[str]] = {}
        for side in (left_side, right_side):
            if side.remote is None:
                continue
            normalized_remote_root = side.remote.root.rstrip("/") or "/"
            known_remote_dirs[id(side.remote.sftp)] = {"/", normalized_remote_root}

        for op in operations:
            relpath = op.relpath
            ok = False
            error: str | None = None
            started = time.perf_counter()

            try:
                if op.kind == "copy_right":
                    _copy_between(
                        left_side,
                        right_side,
                        relpath,
                        settings=resolved_settings,
                        known_remote_dirs=known_remote_dirs,
                    )
                    ok = True
                elif op.kind == "copy_left":
                    _copy_between(
                        right_side,
                        left_side,
                        relpath,
                        settings=resolved_settings,
                        known_remote_dirs=known_remote_dirs,
                    )
                    ok = True
                elif op.kind == "delete_right":
                    _side_remove_file(right_side, relpath)
                    ok = True
                elif op.kind == "delete_left":
                    _side_remove_file(left_side, relpath)
                    ok = True
                elif op.kind in {"metadata_update_left", "metadata_update_right"}:
                    op_kinds = {item.kind for item in path_ops.get(relpath, [])}
                    both_directions = (
                        "metadata_update_left" in op_kinds
                        and "metadata_update_right" in op_kinds
                    )
                    if op.kind == "metadata_update_left":
                        _apply_metadata_from_right_to_left(
                            left_side,
                            right_side,
                            relpath,
                            both_directions=both_directions,
                        )
                    else:
                        _apply_metadata_from_left_to_right(
                            left_side,
                            right_side,
                            relpath,
                            both_directions=both_directions,
                        )
                    ok = True
                else:
                    error = f"unsupported operation kind: {op.kind}"
            except Exception as exc:  # noqa: BLE001
                error = str(exc)
            finally:
                elapsed = time.perf_counter() - started
                op_counts[op.kind] = op_counts.get(op.kind, 0) + 1
                op_seconds[op.kind] = op_seconds.get(op.kind, 0.0) + elapsed

            if ok:
                succeeded.add((op.kind, op.relpath))
            elif error:
                errors.append(f"{op.kind} {op.relpath}: {error}")

            done_count += 1
            if progress_cb is not None:
                progress_cb(done_count, total, op, ok, error)

    completed_paths: set[str] = set()
    for relpath, path_operations in path_ops.items():
        if all((op.kind, op.relpath) in succeeded for op in path_operations):
            completed_paths.add(relpath)

    return ExecuteResult(
        completed_paths=completed_paths,
        errors=errors,
        succeeded_operations=len(succeeded),
        total_operations=total,
        succeeded_operation_keys=frozenset(succeeded),
        operation_counts=op_counts,
        operation_seconds=op_seconds,
    )
