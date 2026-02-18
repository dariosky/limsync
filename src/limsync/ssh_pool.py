from __future__ import annotations

import atexit
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any


@dataclass
class _PoolEntry:
    client: Any
    refcount: int = 0


_POOL_LOCK = threading.Lock()
_POOL: dict[tuple[str, str, int, bool, int, int], _PoolEntry] = {}


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
    user: str,
    port: int,
    compress: bool,
    timeout: int = 10,
    client_factory: Callable[[], Any],
    auto_add_policy_factory: Callable[[], Any],
) -> Iterator[Any]:
    key = (
        host,
        user,
        port,
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
            client.connect(
                hostname=host,
                username=user,
                port=port,
                look_for_keys=True,
                allow_agent=True,
                timeout=timeout,
                compress=compress,
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
