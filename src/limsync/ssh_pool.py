from __future__ import annotations

import atexit
import subprocess
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import paramiko


@dataclass
class _PoolEntry:
    client: Any
    refcount: int = 0


_POOL_LOCK = threading.Lock()
_POOL: dict[tuple[object, ...], _PoolEntry] = {}


@dataclass(frozen=True)
class SSHConnectionOptions:
    hostname: str
    username: str | None
    port: int
    key_filenames: tuple[str, ...] = ()
    proxy_command: str | None = None


def resolve_ssh_connection_options(
    host: str,
    user: str | None,
    port: int | None,
) -> SSHConnectionOptions:
    """Resolve an SSH alias using the same config rules as the OpenSSH client."""
    command = ["ssh", "-G"]
    if user is not None:
        command.extend(["-l", user])
    if port is not None:
        command.extend(["-p", str(port)])
    command.append(host)

    values: dict[str, list[str]] = {}
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        result = None

    if result is not None and result.returncode == 0:
        for line in result.stdout.splitlines():
            key, separator, value = line.partition(" ")
            if separator and value:
                values.setdefault(key.lower(), []).append(value.strip())

    resolved_host = values.get("hostname", [host])[0]
    resolved_user = values.get("user", [user])[0]
    resolved_port_text = values.get("port", [str(port or 22)])[0]
    try:
        resolved_port = int(resolved_port_text)
    except ValueError:
        resolved_port = port or 22

    key_filenames = tuple(
        str(path)
        for raw_path in values.get("identityfile", [])
        if (path := Path(raw_path).expanduser()).is_file()
    )
    proxy_command = values.get("proxycommand", [None])[0]
    if proxy_command == "none":
        proxy_command = None

    return SSHConnectionOptions(
        hostname=resolved_host,
        username=resolved_user,
        port=resolved_port,
        key_filenames=key_filenames,
        proxy_command=proxy_command,
    )


def _client_alive(client: Any) -> bool:
    get_transport = getattr(client, "get_transport", None)
    if get_transport is None:
        return True
    try:
        transport = get_transport()
    except Exception:
        return False
    if transport is None:
        return False
    is_active = getattr(transport, "is_active", None)
    if is_active is None:
        return True
    try:
        return bool(is_active())
    except Exception:
        return False


def _close_client_quietly(client: Any) -> None:
    try:
        client.close()
    except Exception:
        return


@contextmanager
def pooled_ssh_client(
    *,
    host: str,
    user: str | None,
    port: int | None,
    compress: bool,
    timeout: int = 10,
    client_factory: Callable[[], Any] = paramiko.SSHClient,
    auto_add_policy_factory: Callable[[], Any] = paramiko.AutoAddPolicy,
) -> Iterator[Any]:
    options = resolve_ssh_connection_options(host, user, port)
    key = (
        options.hostname,
        options.username,
        options.port,
        options.key_filenames,
        options.proxy_command,
        compress,
        id(client_factory),
        id(auto_add_policy_factory),
    )
    with _POOL_LOCK:
        entry = _POOL.get(key)
        if entry is not None and not _client_alive(entry.client):
            _close_client_quietly(entry.client)
            _POOL.pop(key, None)
            entry = None
        if entry is None:
            client = client_factory()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(auto_add_policy_factory())
            sock = (
                paramiko.ProxyCommand(options.proxy_command)
                if options.proxy_command is not None
                else None
            )
            client.connect(
                hostname=options.hostname,
                username=options.username,
                port=options.port,
                key_filename=list(options.key_filenames) or None,
                look_for_keys=True,
                allow_agent=True,
                timeout=timeout,
                compress=compress,
                sock=sock,
            )
            entry = _PoolEntry(client=client, refcount=0)
            _POOL[key] = entry
        entry.refcount += 1
        client = entry.client

    try:
        yield client
    finally:
        with _POOL_LOCK:
            cached = _POOL.get(key)
            if cached is not None and cached.client is client:
                cached.refcount = max(0, cached.refcount - 1)


def close_ssh_pool() -> None:
    with _POOL_LOCK:
        items = list(_POOL.items())
        _POOL.clear()
    for _key, entry in items:
        _close_client_quietly(entry.client)


atexit.register(close_ssh_pool)
