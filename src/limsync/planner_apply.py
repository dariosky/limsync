from __future__ import annotations

import os
import re
import stat
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import paramiko

from .models import ContentState, DiffRecord, MetadataState

ACTION_LEFT_WINS = "left_wins"
ACTION_RIGHT_WINS = "right_wins"
ACTION_IGNORE = "ignore"
ACTION_SUGGESTED = "suggested"


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


def parse_remote_address(remote_address: str) -> tuple[str, str, str]:
    # format: user@host:/abs/or/tilde/path
    if "@" not in remote_address or ":" not in remote_address:
        raise ValueError(f"Invalid remote address: {remote_address}")
    user_host, root = remote_address.split(":", 1)
    user, host = user_host.split("@", 1)
    return user, host, root


def _infer_metadata_source_from_details(diff: DiffRecord) -> str | None:
    mode_re = re.compile(r"mode:\s+local=0x([0-7]{3})\s+remote=0x([0-7]{3})")
    mtime_re = re.compile(r"mtime:\s+local=(.*?)\s+remote=(.*?)$")

    for detail in diff.metadata_details:
        mode_match = mode_re.match(detail)
        if mode_match:
            local_mode = int(mode_match.group(1), 8)
            remote_mode = int(mode_match.group(2), 8)
            if local_mode != remote_mode:
                return "local" if local_mode < remote_mode else "remote"

    for detail in diff.metadata_details:
        mtime_match = mtime_re.match(detail)
        if mtime_match:
            local_mtime = datetime.strptime(
                mtime_match.group(1), "%Y-%m-%d %H:%M:%S.%f UTC"
            )
            remote_mtime = datetime.strptime(
                mtime_match.group(2), "%Y-%m-%d %H:%M:%S.%f UTC"
            )
            if local_mtime != remote_mtime:
                return "local" if local_mtime < remote_mtime else "remote"

    return None


def _suggested_metadata_op(relpath: str, diff: DiffRecord) -> list[PlanOperation]:
    source = diff.metadata_source or _infer_metadata_source_from_details(diff)
    if source == "local":
        return [PlanOperation("metadata_update_right", relpath)]
    if source == "remote":
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

        if diff.content_state == ContentState.ONLY_LOCAL:
            if action in {ACTION_LEFT_WINS, ACTION_SUGGESTED}:
                ops.append(PlanOperation("copy_right", diff.relpath))
            elif action == ACTION_RIGHT_WINS:
                ops.append(PlanOperation("delete_left", diff.relpath))
            continue

        if diff.content_state == ContentState.ONLY_REMOTE:
            if action in {ACTION_RIGHT_WINS, ACTION_SUGGESTED}:
                ops.append(PlanOperation("copy_left", diff.relpath))
            elif action == ACTION_LEFT_WINS:
                ops.append(PlanOperation("delete_right", diff.relpath))
            continue

        if diff.content_state in {ContentState.DIFFERENT, ContentState.UNKNOWN}:
            if action == ACTION_LEFT_WINS:
                ops.append(PlanOperation("copy_right", diff.relpath))
            elif action == ACTION_RIGHT_WINS:
                ops.append(PlanOperation("copy_left", diff.relpath))
            # suggested => no content op for conflict/unknown
            # For explicit different-content conflicts, also avoid metadata suggestions.
            if (
                diff.content_state == ContentState.DIFFERENT
                and action == ACTION_SUGGESTED
            ):
                continue

        ops.extend(_metadata_ops(diff.relpath, action, diff))

    # dedupe by (kind, path)
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


def _remote_mtime_ns(st: object) -> int:
    return int(float(getattr(st, "st_mtime", 0)) * 1_000_000_000)


def _remote_atime_ns(st: object) -> int:
    return int(float(getattr(st, "st_atime", 0)) * 1_000_000_000)


def _apply_remote_metadata_from_local(
    sftp: paramiko.SFTPClient,
    remote_path: str,
    local_stat: os.stat_result,
) -> None:
    mode = stat.S_IMODE(local_stat.st_mode)
    sftp.chmod(remote_path, mode)
    # SFTP exposes second-level utime; ns precision is not available.
    sftp.utime(
        remote_path,
        (
            int(local_stat.st_atime_ns / 1_000_000_000),
            int(local_stat.st_mtime_ns / 1_000_000_000),
        ),
    )


def _apply_local_metadata_from_remote(
    local_path: Path,
    remote_stat: object,
) -> None:
    mode = stat.S_IMODE(getattr(remote_stat, "st_mode", 0))
    os.chmod(local_path, mode)
    os.utime(
        local_path,
        ns=(_remote_atime_ns(remote_stat), _remote_mtime_ns(remote_stat)),
    )


def _remote_expand_root(client: paramiko.SSHClient, root: str) -> str:
    quoted = root.replace("'", "'\\''")
    command = f"python3 -c \"import os; print(os.path.expanduser('{quoted}'))\""
    _stdin, stdout, stderr = client.exec_command(command)
    out = stdout.read().decode("utf-8", errors="replace").strip()
    err = stderr.read().decode("utf-8", errors="replace").strip()
    if out:
        return out
    raise RuntimeError(f"Failed to resolve remote root {root!r}: {err}")


def execute_plan(
    local_root: Path,
    remote_address: str,
    operations: list[PlanOperation],
    progress_cb: Callable[[int, int, PlanOperation, bool, str | None], None]
    | None = None,
    settings: ApplySettings | None = None,
) -> ExecuteResult:
    resolved_settings = settings or ApplySettings()
    if not operations:
        return ExecuteResult(
            completed_paths=set(),
            errors=[],
            succeeded_operations=0,
            total_operations=0,
        )

    user, host, remote_root_raw = parse_remote_address(remote_address)
    path_ops: dict[str, list[PlanOperation]] = {}
    for op in operations:
        path_ops.setdefault(op.relpath, []).append(op)

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
        compress=resolved_settings.ssh_compression,
    )

    errors: list[str] = []
    succeeded: set[tuple[str, str]] = set()
    attempted: set[tuple[str, str]] = set()
    done_count = 0
    total = len(operations)
    op_counts: dict[str, int] = {}
    op_seconds: dict[str, float] = {}
    metadata_context_cache: dict[str, tuple[os.stat_result, object, set[str]]] = {}

    try:
        remote_root = _remote_expand_root(client, remote_root_raw)
        sftp = client.open_sftp()
        normalized_remote_root = remote_root.rstrip("/") or "/"
        known_remote_dirs: set[str] = {"/", normalized_remote_root}
        try:
            for op in operations:
                attempted.add((op.kind, op.relpath))
                relpath = op.relpath
                local_path = local_root / relpath
                remote_path = _join_remote(remote_root, relpath)
                ok = False
                error: str | None = None
                started = time.perf_counter()

                try:
                    if op.kind == "copy_right":
                        if not local_path.exists():
                            raise FileNotFoundError(
                                f"missing local source: {local_path}"
                            )
                        source_local_stat = local_path.lstat()
                        _ensure_remote_parent_cached(
                            sftp,
                            remote_path,
                            known_dirs=known_remote_dirs,
                        )
                        sftp.put(
                            str(local_path),
                            remote_path,
                            confirm=resolved_settings.sftp_put_confirm,
                        )
                        _apply_remote_metadata_from_local(
                            sftp, remote_path, source_local_stat
                        )
                        ok = True
                    elif op.kind == "copy_left":
                        source_remote_stat = sftp.stat(remote_path)
                        _ensure_local_parent(local_path)
                        sftp.get(remote_path, str(local_path))
                        _apply_local_metadata_from_remote(
                            local_path, source_remote_stat
                        )
                        ok = True
                    elif op.kind == "delete_right":
                        sftp.remove(remote_path)
                        ok = True
                    elif op.kind == "delete_left":
                        local_path.unlink()
                        ok = True
                    elif op.kind in {"metadata_update_left", "metadata_update_right"}:
                        context = metadata_context_cache.get(relpath)
                        if context is None:
                            lst = local_path.lstat()
                            rst = sftp.lstat(remote_path)
                            op_kinds = {item.kind for item in path_ops.get(relpath, [])}
                            context = (lst, rst, op_kinds)
                            metadata_context_cache[relpath] = context
                        else:
                            lst, rst, op_kinds = context
                        local_is_symlink = stat.S_ISLNK(lst.st_mode)
                        remote_is_symlink = stat.S_ISLNK(getattr(rst, "st_mode", 0))
                        if local_is_symlink or remote_is_symlink:
                            # Symlink metadata propagation is platform-dependent and
                            # may fail for broken links; treat as no-op.
                            ok = True
                            done_count += 1
                            succeeded.add((op.kind, op.relpath))
                            if progress_cb is not None:
                                progress_cb(
                                    done_count,
                                    total,
                                    op,
                                    True,
                                    None,
                                )
                            continue
                        has_left = "metadata_update_left" in op_kinds
                        has_right = "metadata_update_right" in op_kinds

                        target_mode = (
                            min(stat.S_IMODE(lst.st_mode), stat.S_IMODE(rst.st_mode))
                            if (has_left and has_right)
                            else None
                        )
                        target_mtime_ns = (
                            min(lst.st_mtime_ns, _remote_mtime_ns(rst))
                            if (has_left and has_right)
                            else None
                        )

                        if op.kind == "metadata_update_left":
                            mode = (
                                target_mode
                                if target_mode is not None
                                else stat.S_IMODE(rst.st_mode)
                            )
                            mtime_ns = (
                                target_mtime_ns
                                if target_mtime_ns is not None
                                else _remote_mtime_ns(rst)
                            )
                            os.chmod(local_path, mode)
                            os.utime(local_path, ns=(lst.st_atime_ns, mtime_ns))
                            ok = True
                        else:
                            mode = (
                                target_mode
                                if target_mode is not None
                                else stat.S_IMODE(lst.st_mode)
                            )
                            mtime_ns = (
                                target_mtime_ns
                                if target_mtime_ns is not None
                                else lst.st_mtime_ns
                            )
                            sftp.chmod(remote_path, mode)
                            sftp.utime(
                                remote_path,
                                (int(rst.st_atime), int(mtime_ns / 1_000_000_000)),
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

        finally:
            sftp.close()
    finally:
        client.close()

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
