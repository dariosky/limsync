from __future__ import annotations

from dataclasses import dataclass

CACHE_FOLDERS = {"__pycache__", ".pytest_cache", ".cache", ".ruff_cache"}
EXCLUDED_FOLDERS = {"node_modules", ".tox", ".venv", ".limsync"} | CACHE_FOLDERS
EXCLUDED_FILE_NAMES = {".DS_Store", "Icon\r"}

DEFAULT_REMOTE_PORT = 22
DEFAULT_STATE_SUBPATH = ".limsync/state.sqlite3"


@dataclass(frozen=True)
class RemoteConfig:
    host: str
    user: str | None = None
    port: int | None = None
    root: str = "."
    state_db: str = DEFAULT_STATE_SUBPATH

    @property
    def address(self) -> str:
        user_part = f"{self.user}@" if self.user else ""
        port_part = f":{self.port}" if self.port is not None else ""
        return f"{user_part}{self.host}{port_part}:{self.root}"
