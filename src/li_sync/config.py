from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

CACHE_FOLDERS = {"__pycache__", ".pytest_cache", ".cache", ".ruff_cache"}
EXCLUDED_FOLDERS = {"node_modules", ".tox"} | CACHE_FOLDERS
EXCLUDED_FILE_NAMES = {".DS_Store"}

DEFAULT_LOCAL_ROOT = Path("/Users/dario.varotto/Dropbox")
DEFAULT_REMOTE_HOST = "192.168.18.18"
DEFAULT_REMOTE_USER = "dario"
DEFAULT_REMOTE_ROOT = "~/Dropbox"
DEFAULT_REMOTE_PORT = 22
DEFAULT_REMOTE_STATE_DB = "~/.cache/li-sync/scan_state.sqlite3"


@dataclass(frozen=True)
class RemoteConfig:
    host: str = DEFAULT_REMOTE_HOST
    user: str = DEFAULT_REMOTE_USER
    port: int = DEFAULT_REMOTE_PORT
    root: str = DEFAULT_REMOTE_ROOT
    state_db: str = DEFAULT_REMOTE_STATE_DB

    @property
    def address(self) -> str:
        return f"{self.user}@{self.host}:{self.root}"
