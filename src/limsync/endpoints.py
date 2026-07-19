from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from .config import DEFAULT_REMOTE_PORT, DEFAULT_STATE_SUBPATH

_SCP_REMOTE_RE = re.compile(
    r"^(?:(?P<user>[^@:/\s]+)@)?(?P<host>[^:/\s]+):(?P<root>.+)$"
)


@dataclass(frozen=True)
class EndpointSpec:
    kind: str
    root: str
    user: str | None = None
    host: str | None = None
    port: int | None = None

    @property
    def is_local(self) -> bool:
        return self.kind == "local"

    @property
    def is_remote(self) -> bool:
        return self.kind == "remote"

    @property
    def label(self) -> str:
        if self.is_local:
            return f"local:{self.root}"
        port_part = ""
        if self.port is not None and self.port != DEFAULT_REMOTE_PORT:
            port_part = f":{self.port}"
        user_part = f"{self.user}@" if self.user else ""
        return f"{user_part}{self.host}{port_part}:{self.root}"


@dataclass(frozen=True)
class ParsedRemoteAddress:
    user: str
    host: str
    root: str
    port: int


def parse_endpoint(value: str) -> EndpointSpec:
    text = value.strip()
    if not text:
        raise ValueError("endpoint cannot be empty")

    if text.startswith("local:"):
        path_value = text[len("local:") :].strip()
        if not path_value:
            raise ValueError("local endpoint path cannot be empty")
        root = str(Path(path_value).expanduser().resolve())
        return EndpointSpec(kind="local", root=root)

    if text.startswith("ssh://"):
        parsed = urlparse(text)
        if parsed.scheme != "ssh" or not parsed.hostname:
            raise ValueError(f"invalid ssh endpoint: {value}")
        root = parsed.path or "/"
        if root.startswith("/~/"):
            root = root[1:]
        elif root == "/~":
            root = "~"
        if not root.startswith("/"):
            if not root.startswith("~"):
                root = f"/{root}"
        return EndpointSpec(
            kind="remote",
            root=root,
            user=parsed.username,
            host=parsed.hostname,
            port=parsed.port,
        )

    scp_match = _SCP_REMOTE_RE.match(text)
    if scp_match is not None:
        return EndpointSpec(
            kind="remote",
            root=scp_match.group("root"),
            user=scp_match.group("user"),
            host=scp_match.group("host"),
            port=None,
        )

    root = str(Path(text).expanduser().resolve())
    return EndpointSpec(kind="local", root=root)


def endpoint_to_string(endpoint: EndpointSpec) -> str:
    if endpoint.is_local:
        return f"local:{endpoint.root}"
    user_part = f"{endpoint.user}@" if endpoint.user else ""
    port_part = f":{endpoint.port}" if endpoint.port is not None else ""
    root = endpoint.root
    if root == "~":
        path = "/~"
    elif root.startswith("~/"):
        path = f"/{root}"
    elif root.startswith("/"):
        path = root
    else:
        path = f"/{root}"
    return f"ssh://{user_part}{endpoint.host}{port_part}{path}"


def default_endpoint_state_db(endpoint: EndpointSpec) -> str:
    if endpoint.is_local:
        return str(Path(endpoint.root) / DEFAULT_STATE_SUBPATH)
    return f"{endpoint.root.rstrip('/')}/{DEFAULT_STATE_SUBPATH}"


def default_review_db_path(source: EndpointSpec, destination: EndpointSpec) -> Path:
    base = Path.home().expanduser() / ".limsync"
    base.mkdir(parents=True, exist_ok=True)
    src_slug = endpoint_slug(source)
    dst_slug = endpoint_slug(destination)
    return base / f"{src_slug}__{dst_slug}.sqlite3"


def endpoint_slug(endpoint: EndpointSpec) -> str:
    digest = hashlib.sha1(endpoint_to_string(endpoint).encode("utf-8")).hexdigest()[:12]
    if endpoint.is_local:
        tail = Path(endpoint.root).name or "root"
        return _sanitize_slug(f"local-{tail}-{digest}")
    host = endpoint.host or "host"
    tail = Path(endpoint.root).name or "root"
    return _sanitize_slug(f"remote-{host}-{tail}-{digest}")


def _sanitize_slug(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-")
    return cleaned or "state"


def parse_legacy_remote_address(remote_address: str) -> ParsedRemoteAddress:
    if "@" not in remote_address or ":" not in remote_address:
        raise ValueError(f"Invalid remote address: {remote_address}")
    user_host, root = remote_address.split(":", 1)
    user, host = user_host.split("@", 1)
    return ParsedRemoteAddress(
        user=user,
        host=host,
        root=root,
        port=DEFAULT_REMOTE_PORT,
    )
