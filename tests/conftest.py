from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from limsync.models import (
    ContentState,
    DiffRecord,
    FileRecord,
    MetadataState,
    NodeType,
)


def mk_file(
    relpath: str,
    *,
    node_type: NodeType = NodeType.FILE,
    size: int = 0,
    mtime_ns: int = 0,
    mode: int = 0o644,
) -> FileRecord:
    return FileRecord(
        relpath=relpath,
        node_type=node_type,
        size=size,
        mtime_ns=mtime_ns,
        mode=mode,
    )


def mk_diff(
    relpath: str,
    *,
    content_state: ContentState,
    metadata_state: MetadataState = MetadataState.IDENTICAL,
    metadata_diff: tuple[str, ...] = (),
    metadata_details: tuple[str, ...] = (),
    metadata_source: str | None = None,
    local_size: int | None = None,
    remote_size: int | None = None,
) -> DiffRecord:
    return DiffRecord(
        relpath=relpath,
        content_state=content_state,
        metadata_state=metadata_state,
        metadata_diff=metadata_diff,
        metadata_details=metadata_details,
        metadata_source=metadata_source,
        local_size=local_size,
        remote_size=remote_size,
    )


class _FakeStream:
    def __init__(self, text: str) -> None:
        self._data = text.encode("utf-8")

    def read(self) -> bytes:
        return self._data


@dataclass
class RemoteStat:
    st_mode: int
    st_atime: float
    st_mtime: float


class FakeSFTPClient:
    def __init__(self) -> None:
        self.remote_files: dict[str, bytes] = {}
        self.remote_stats: dict[str, RemoteStat] = {}
        self.remote_lstats: dict[str, RemoteStat] = {}
        self.existing_dirs: set[str] = {"/"}
        self.failures: dict[tuple[str, str], Exception] = {}
        self.calls: list[tuple] = []

    def _check_failure(self, method: str, path: str) -> None:
        err = self.failures.get((method, path))
        if err is not None:
            raise err

    def _default_file_stat(self) -> RemoteStat:
        return RemoteStat(st_mode=0o100644, st_atime=1.0, st_mtime=1.0)

    def stat(self, path: str):
        self._check_failure("stat", path)
        self.calls.append(("stat", path))
        if path in self.remote_stats:
            return self.remote_stats[path]
        if path in self.existing_dirs:
            return RemoteStat(st_mode=0o040755, st_atime=1.0, st_mtime=1.0)
        raise OSError(f"no such file: {path}")

    def lstat(self, path: str):
        self._check_failure("lstat", path)
        self.calls.append(("lstat", path))
        if path in self.remote_lstats:
            return self.remote_lstats[path]
        return self.stat(path)

    def mkdir(self, path: str) -> None:
        self._check_failure("mkdir", path)
        self.calls.append(("mkdir", path))
        self.existing_dirs.add(path)

    def put(self, local_path: str, remote_path: str, *, confirm: bool = True) -> None:
        self._check_failure("put", remote_path)
        self.calls.append(("put", local_path, remote_path, confirm))
        data = Path(local_path).read_bytes()
        self.remote_files[remote_path] = data
        self.remote_stats.setdefault(remote_path, self._default_file_stat())

    def get(self, remote_path: str, local_path: str) -> None:
        self._check_failure("get", remote_path)
        self.calls.append(("get", remote_path, local_path))
        if remote_path not in self.remote_files:
            raise FileNotFoundError(f"remote missing: {remote_path}")
        Path(local_path).write_bytes(self.remote_files[remote_path])

    def remove(self, remote_path: str) -> None:
        self._check_failure("remove", remote_path)
        self.calls.append(("remove", remote_path))
        if remote_path in self.remote_files:
            del self.remote_files[remote_path]
            self.remote_stats.pop(remote_path, None)
            self.remote_lstats.pop(remote_path, None)
            return
        raise FileNotFoundError(f"remote missing: {remote_path}")

    def chmod(self, path: str, mode: int) -> None:
        self._check_failure("chmod", path)
        self.calls.append(("chmod", path, mode))
        stat_entry = self.remote_stats.get(path, self._default_file_stat())
        file_type_bits = stat_entry.st_mode & 0o170000
        self.remote_stats[path] = RemoteStat(
            st_mode=file_type_bits | mode,
            st_atime=stat_entry.st_atime,
            st_mtime=stat_entry.st_mtime,
        )

    def utime(self, path: str, times: tuple[int, int]) -> None:
        self._check_failure("utime", path)
        self.calls.append(("utime", path, times))
        stat_entry = self.remote_stats.get(path, self._default_file_stat())
        self.remote_stats[path] = RemoteStat(
            st_mode=stat_entry.st_mode,
            st_atime=float(times[0]),
            st_mtime=float(times[1]),
        )

    def close(self) -> None:
        self.calls.append(("close",))


class FakeSSHClient:
    def __init__(self, sftp: FakeSFTPClient, *, expand_stdout: str = "", expand_stderr: str = "") -> None:
        self.sftp = sftp
        self.expand_stdout = expand_stdout
        self.expand_stderr = expand_stderr
        self.connect_calls: list[dict[str, object]] = []
        self.closed = False

    def load_system_host_keys(self) -> None:
        return None

    def set_missing_host_key_policy(self, policy: object) -> None:
        _ = policy

    def connect(self, **kwargs) -> None:
        self.connect_calls.append(kwargs)

    def exec_command(self, _command: str):
        return (
            None,
            _FakeStream(self.expand_stdout),
            _FakeStream(self.expand_stderr),
        )

    def open_sftp(self) -> FakeSFTPClient:
        return self.sftp

    def close(self) -> None:
        self.closed = True


class DummyAutoAddPolicy:
    pass


def make_remote_stat(
    *,
    mode: int = 0o100644,
    atime: float = 1.0,
    mtime: float = 1.0,
) -> RemoteStat:
    return RemoteStat(st_mode=mode, st_atime=atime, st_mtime=mtime)
