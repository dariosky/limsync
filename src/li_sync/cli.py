from __future__ import annotations

import concurrent.futures
import json
import threading
import time
from pathlib import Path, PurePosixPath

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .compare import compare_records
from .config import (
    DEFAULT_LOCAL_ROOT,
    DEFAULT_REMOTE_HOST,
    DEFAULT_REMOTE_PORT,
    DEFAULT_REMOTE_ROOT,
    DEFAULT_REMOTE_STATE_DB,
    DEFAULT_REMOTE_USER,
    RemoteConfig,
)
from .models import ContentState, FileRecord, MetadataState
from .scanner_local import LocalScanner
from .scanner_remote import RemoteScanner

app = typer.Typer(help="Interactive Dropbox-like sync tooling over SSH")
console = Console()


class ScanProgressReporter:
    def __init__(
        self, progress: Progress, task_id: int, root_label: str, lock: threading.Lock
    ) -> None:
        self.progress = progress
        self.task_id = task_id
        self.root_label = root_label
        self.lock = lock
        self.started_at = time.monotonic()
        self.last_rendered = 0.0

    def _depth_limit(self) -> int:
        elapsed = time.monotonic() - self.started_at
        if elapsed < 8:
            return 1
        if elapsed < 20:
            return 2
        if elapsed < 45:
            return 3
        if elapsed < 90:
            return 4
        return 6

    def _format_path(self, relpath: PurePosixPath) -> str:
        if relpath == PurePosixPath("."):
            return self.root_label
        depth = self._depth_limit()
        parts = relpath.parts[:depth]
        return f"{self.root_label}/{'/'.join(parts)}"

    def update(
        self, relpath: PurePosixPath, dirs_scanned: int, files_seen: int
    ) -> None:
        now = time.monotonic()
        if (now - self.last_rendered) < 0.12:
            return
        label = self._format_path(relpath)
        with self.lock:
            self.progress.update(
                self.task_id,
                description=f"{label}  dirs={dirs_scanned} files={files_seen}",
            )
        self.last_rendered = now


def _format_seconds(seconds: float) -> str:
    return f"{seconds:.2f}s"


def _write_diff_jsonl(path: Path, diffs: list[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for diff in diffs:
            handle.write(
                json.dumps(
                    {
                        "relpath": diff.relpath,
                        "content_state": diff.content_state.value,
                        "metadata_state": diff.metadata_state.value,
                        "metadata_diff": list(diff.metadata_diff),
                    },
                    ensure_ascii=True,
                )
                + "\n"
            )


@app.command()
def scan(
    local_root: Path = typer.Option(DEFAULT_LOCAL_ROOT, help="Local root folder"),
    remote_host: str = typer.Option(DEFAULT_REMOTE_HOST, help="Remote SSH host"),
    remote_user: str = typer.Option(DEFAULT_REMOTE_USER, help="Remote SSH user"),
    remote_port: int = typer.Option(DEFAULT_REMOTE_PORT, help="Remote SSH port"),
    remote_root: str = typer.Option(DEFAULT_REMOTE_ROOT, help="Remote root folder"),
    remote_state_db: str = typer.Option(
        DEFAULT_REMOTE_STATE_DB,
        help="Remote SQLite cache path used by helper",
    ),
    save_diff: Path = typer.Option(
        Path("doc/last_scan_diff.jsonl"),
        help="Write full diff set as JSONL to this file",
    ),
    show: int = typer.Option(40, min=1, help="How many diff rows to print"),
) -> None:
    """Scan local and remote trees and print a first diff report."""
    local_root = local_root.expanduser().resolve()
    remote_cfg = RemoteConfig(
        host=remote_host,
        user=remote_user,
        port=remote_port,
        root=remote_root,
        state_db=remote_state_db,
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        console=console,
    ) as progress:
        progress_lock = threading.Lock()
        local_task = progress.add_task("Preparing local scan...", total=None)
        remote_task = progress.add_task(
            f"Preparing remote scan ({remote_cfg.address})...",
            total=None,
        )
        local_reporter = ScanProgressReporter(
            progress,
            local_task,
            local_root.name or str(local_root),
            progress_lock,
        )
        remote_label = f"remote:{Path(remote_cfg.root).name or remote_cfg.root}"
        remote_reporter = ScanProgressReporter(
            progress, remote_task, remote_label, progress_lock
        )

        def run_local_scan() -> tuple[dict[str, FileRecord], float]:
            started = time.perf_counter()
            records = LocalScanner(local_root).scan(progress_cb=local_reporter.update)
            return records, (time.perf_counter() - started)

        def run_remote_scan() -> tuple[dict[str, FileRecord], float]:
            started = time.perf_counter()
            records = RemoteScanner(remote_cfg).scan(progress_cb=remote_reporter.update)
            return records, (time.perf_counter() - started)

        local_records: dict[str, FileRecord]
        remote_records: dict[str, FileRecord]
        local_elapsed = 0.0
        remote_elapsed = 0.0

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            future_local = pool.submit(run_local_scan)
            future_remote = pool.submit(run_remote_scan)

            try:
                local_records, local_elapsed = future_local.result()
            except Exception as exc:
                progress.stop()
                console.print(f"[red]Local scan failed:[/red] {exc}")
                raise typer.Exit(1)

            try:
                remote_records, remote_elapsed = future_remote.result()
            except Exception as exc:
                progress.stop()
                console.print(f"[red]Remote scan failed:[/red] {exc}")
                raise typer.Exit(1)

        with progress_lock:
            progress.update(
                local_task,
                description=(
                    f"Local scan completed  files={len(local_records)}  "
                    f"time={_format_seconds(local_elapsed)}"
                ),
            )
            progress.update(
                remote_task,
                description=(
                    f"Remote scan completed  files={len(remote_records)}  "
                    f"time={_format_seconds(remote_elapsed)}"
                ),
            )

    diffs = compare_records(local_records, remote_records)
    _write_diff_jsonl(save_diff, diffs)

    counts = {
        ContentState.ONLY_LOCAL: 0,
        ContentState.ONLY_REMOTE: 0,
        ContentState.DIFFERENT: 0,
        ContentState.UNKNOWN: 0,
        ContentState.IDENTICAL: 0,
    }
    metadata_only = 0

    for diff in diffs:
        counts[diff.content_state] = counts.get(diff.content_state, 0) + 1
        if (
            diff.content_state == ContentState.IDENTICAL
            and diff.metadata_state == MetadataState.DIFFERENT
        ):
            metadata_only += 1

    console.print()
    console.print(f"Local scan time: {_format_seconds(local_elapsed)}")
    console.print(f"Remote scan time: {_format_seconds(remote_elapsed)}")
    console.print(f"Local files: {len(local_records)}")
    console.print(f"Remote files: {len(remote_records)}")
    console.print(f"Compared paths: {len(diffs)}")
    console.print(f"Only local: {counts[ContentState.ONLY_LOCAL]}")
    console.print(f"Only remote: {counts[ContentState.ONLY_REMOTE]}")
    console.print(f"Different content: {counts[ContentState.DIFFERENT]}")
    console.print(f"Uncertain (same size, mtime drift): {counts[ContentState.UNKNOWN]}")
    console.print(f"Metadata-only drift: {metadata_only}")
    console.print(f"Saved full diff: {save_diff}")

    table = Table(title=f"Top {min(show, len(diffs))} changes")
    table.add_column("Path", overflow="fold")
    table.add_column("Content")
    table.add_column("Metadata")
    table.add_column("Metadata Diff")

    displayed = 0
    for diff in diffs:
        if (
            diff.content_state == ContentState.IDENTICAL
            and diff.metadata_state == MetadataState.IDENTICAL
        ):
            continue
        table.add_row(
            diff.relpath,
            diff.content_state.value,
            diff.metadata_state.value,
            ", ".join(diff.metadata_diff) if diff.metadata_diff else "-",
        )
        displayed += 1
        if displayed >= show:
            break

    if displayed == 0:
        console.print("\nNo differences detected.")
        return

    if len(diffs) > displayed:
        console.print(f"\nShowing first {displayed} changes out of {len(diffs)} total.")
        console.print(f"Use `li-sync review --diff-file {save_diff}` for the full set.")

    console.print()
    console.print(table)


@app.command()
def review(
    diff_file: Path = typer.Option(
        Path("doc/last_scan_diff.jsonl"),
        help="Path to JSONL diff file generated by `scan`",
    ),
    content: str = typer.Option(
        "all",
        help="Filter content state: all/only_local/only_remote/different/unknown/identical",
    ),
    metadata_only: bool = typer.Option(
        False,
        help="Show only metadata-only drift entries",
    ),
    offset: int = typer.Option(0, min=0, help="Start offset in filtered results"),
    limit: int = typer.Option(80, min=1, help="Max rows to show"),
) -> None:
    """Review previously scanned differences from JSONL (non-TUI phase)."""
    if not diff_file.exists():
        console.print(f"[red]Diff file not found:[/red] {diff_file}")
        raise typer.Exit(1)

    allowed = {"all", "only_local", "only_remote", "different", "unknown", "identical"}
    if content not in allowed:
        console.print(f"[red]Invalid --content:[/red] {content}")
        console.print(f"Allowed values: {', '.join(sorted(allowed))}")
        raise typer.Exit(1)

    rows: list[dict[str, object]] = []
    with diff_file.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))

    filtered: list[dict[str, object]] = []
    for row in rows:
        c_state = str(row.get("content_state", ""))
        m_state = str(row.get("metadata_state", ""))
        if content != "all" and c_state != content:
            continue
        if metadata_only and not (c_state == "identical" and m_state == "different"):
            continue
        filtered.append(row)

    if not filtered:
        console.print("No matching records.")
        return

    start = min(offset, len(filtered))
    end = min(start + limit, len(filtered))
    page = filtered[start:end]

    table = Table(title=f"Review {start}-{end} of {len(filtered)} filtered records")
    table.add_column("Path", overflow="fold")
    table.add_column("Content")
    table.add_column("Metadata")
    table.add_column("Metadata Diff")
    for row in page:
        table.add_row(
            str(row.get("relpath", "")),
            str(row.get("content_state", "")),
            str(row.get("metadata_state", "")),
            ", ".join(row.get("metadata_diff", []))
            if row.get("metadata_diff")
            else "-",
        )

    console.print(table)
    if end < len(filtered):
        console.print(f"More rows available. Next page offset: {end}")


@app.command()
def apply() -> None:
    """Placeholder for upcoming apply engine."""
    console.print(
        "Apply engine is planned for Phase 3 (manual-delete-safe by default)."
    )


if __name__ == "__main__":
    app()
