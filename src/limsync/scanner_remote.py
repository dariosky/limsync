from __future__ import annotations

import json
import shlex
from collections.abc import Callable
from pathlib import Path, PurePosixPath

from .config import RemoteConfig
from .models import FileRecord, NodeType
from .ssh_pool import pooled_ssh_client
from .text_utils import normalize_text


class RemoteScanner:
    def __init__(self, config: RemoteConfig) -> None:
        self.config = config

    def scan(
        self,
        progress_cb: Callable[[PurePosixPath, int, int], None] | None = None,
        subtree: PurePosixPath | None = None,
    ) -> dict[str, FileRecord]:
        records: dict[str, FileRecord] = {}
        done_payload: dict[str, object] | None = None
        error_messages: list[str] = []

        with pooled_ssh_client(
            host=self.config.host,
            user=self.config.user,
            port=self.config.port,
            compress=False,
            timeout=10,
        ) as client:
            helper_source = self._remote_helper_source()
            command = (
                "python3 -u - "
                f"--root {shlex.quote(self.config.root)} "
                f"--state-db {shlex.quote(self.config.state_db)} "
                "--progress-interval 0.2"
            )
            if subtree is not None and str(subtree) not in {"", "."}:
                command += f" --subtree {shlex.quote(subtree.as_posix())}"
            stdin, stdout, stderr = client.exec_command(command)
            stdin.write(helper_source)
            stdin.channel.shutdown_write()

            for raw in iter(stdout.readline, ""):
                line = raw.strip()
                if not line:
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    error_messages.append(f"invalid_json_event: {line[:120]}")
                    continue

                kind = event.get("event")
                if kind == "progress":
                    if progress_cb is not None:
                        relpath = PurePosixPath(str(event.get("relpath", ".")))
                        dirs_scanned = int(event.get("dirs_scanned", 0))
                        files_seen = int(event.get("files_seen", 0))
                        progress_cb(relpath, dirs_scanned, files_seen)
                    continue

                if kind == "record":
                    relpath = normalize_text(str(event["relpath"]))
                    node_type = NodeType(str(event.get("node_type", "file")))
                    records[relpath] = FileRecord(
                        relpath=relpath,
                        node_type=node_type,
                        size=int(event.get("size", 0)),
                        mtime_ns=int(event.get("mtime_ns", 0)),
                        mode=int(event.get("mode", 0)),
                        link_target=(
                            str(event["link_target"])
                            if event.get("link_target") is not None
                            else None
                        ),
                        link_target_key=(
                            str(event["link_target_key"])
                            if event.get("link_target_key") is not None
                            else None
                        ),
                        owner=None,
                        group=None,
                    )
                    continue

                if kind == "error":
                    msg = str(event.get("message", "remote helper error"))
                    path = event.get("path")
                    if path:
                        msg = f"{msg} ({path})"
                    error_messages.append(msg)
                    continue

                if kind == "done":
                    done_payload = event
                    continue

            exit_status = stdout.channel.recv_exit_status()
            stderr_output = stderr.read().decode("utf-8", errors="replace").strip()
            if stderr_output:
                error_messages.append(f"stderr: {stderr_output}")

            if progress_cb is not None and done_payload is not None:
                progress_cb(
                    PurePosixPath("."),
                    int(done_payload.get("dirs_scanned", 0)),
                    int(done_payload.get("files_seen", 0)),
                )

            if exit_status != 0:
                detail = (
                    "; ".join(error_messages[-5:])
                    if error_messages
                    else "remote helper failed"
                )
                raise RuntimeError(
                    f"Remote scan failed with exit code {exit_status}: {detail}"
                )

            return records

    def _remote_helper_source(self) -> str:
        helper_source = (
            Path(__file__).with_name("remote_helper.py").read_text(encoding="utf-8")
        )
        marker = "# [[IGNORE_RULES_SHARED]]"
        if marker not in helper_source:
            return helper_source
        shared_source = (
            Path(__file__)
            .with_name("ignore_rules_shared.py")
            .read_text(encoding="utf-8")
        )
        lines = helper_source.splitlines()
        for idx, line in enumerate(lines):
            if marker not in line:
                continue
            indent = line[: line.index(marker)]
            injected = [
                f"{indent}{shared}" if shared else ""
                for shared in shared_source.splitlines()
            ]
            lines[idx : idx + 1] = injected
            return "\n".join(lines) + ("\n" if helper_source.endswith("\n") else "")
        return helper_source
