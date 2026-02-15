from __future__ import annotations

import concurrent.futures
import threading
import time
from pathlib import Path, PurePosixPath

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .compare import compare_records
from .config import (
    DEFAULT_LOCAL_ROOT,
    DEFAULT_REMOTE_HOST,
    DEFAULT_REMOTE_PORT,
    DEFAULT_REMOTE_ROOT,
    DEFAULT_REMOTE_USER,
    DEFAULT_STATE_SUBPATH,
    RemoteConfig,
)
from .models import ContentState, FileRecord, MetadataState
from .review_tui import run_review_tui
from .scanner_local import LocalScanner
from .scanner_remote import RemoteScanner
from .state_db import (
    ScanStateSummary,
    get_state_context,
    get_ui_pref,
    save_current_state,
)

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


def _local_state_db_path(local_root: Path) -> Path:
    return local_root / Path(DEFAULT_STATE_SUBPATH)


def _remote_state_db_path(remote_root: str) -> str:
    return f"{remote_root.rstrip('/')}/{DEFAULT_STATE_SUBPATH}"


@app.command()
def scan(
    local_root: Path = typer.Option(DEFAULT_LOCAL_ROOT, help="Local root folder"),
    remote_host: str = typer.Option(DEFAULT_REMOTE_HOST, help="Remote SSH host"),
    remote_user: str = typer.Option(DEFAULT_REMOTE_USER, help="Remote SSH user"),
    remote_port: int = typer.Option(DEFAULT_REMOTE_PORT, help="Remote SSH port"),
    remote_root: str = typer.Option(DEFAULT_REMOTE_ROOT, help="Remote root folder"),
    local_state_db: Path | None = typer.Option(
        None,
        help="Local SQLite path for scan status (default: <local_root>/.li-sync/state.sqlite3)",
    ),
    remote_state_db: str | None = typer.Option(
        None,
        help="Remote SQLite path for scan status (default: <remote_root>/.li-sync/state.sqlite3)",
    ),
    open_review: bool = typer.Option(
        True,
        help="Open interactive review UI after scan",
    ),
) -> None:
    """Scan local and remote trees, store run status, and open review UI."""
    local_root = local_root.expanduser().resolve()
    local_db_path = (
        local_state_db.expanduser().resolve()
        if local_state_db is not None
        else _local_state_db_path(local_root)
    )
    remote_db_path = remote_state_db or _remote_state_db_path(remote_root)
    remote_cfg = RemoteConfig(
        host=remote_host,
        user=remote_user,
        port=remote_port,
        root=remote_root,
        state_db=remote_db_path,
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
    run_summary = ScanStateSummary(
        local_root=str(local_root),
        remote_address=remote_cfg.address,
        local_scan_seconds=local_elapsed,
        remote_scan_seconds=remote_elapsed,
        local_files=len(local_records),
        remote_files=len(remote_records),
        compared_paths=len(diffs),
        only_local=counts[ContentState.ONLY_LOCAL],
        only_remote=counts[ContentState.ONLY_REMOTE],
        different_content=counts[ContentState.DIFFERENT],
        uncertain=counts[ContentState.UNKNOWN],
        metadata_only=metadata_only,
    )
    save_current_state(local_db_path, run_summary, diffs)

    console.print()
    console.print(f"Local files: {len(local_records)}")
    console.print(f"Remote files: {len(remote_records)}")
    console.print(f"Compared paths: {len(diffs)}")
    console.print(f"Only local: {counts[ContentState.ONLY_LOCAL]}")
    console.print(f"Only remote: {counts[ContentState.ONLY_REMOTE]}")
    console.print(f"Different content: {counts[ContentState.DIFFERENT]}")
    console.print(f"Uncertain (same size, mtime drift): {counts[ContentState.UNKNOWN]}")
    console.print(f"Metadata-only drift: {metadata_only}")
    if open_review:
        pref_value = get_ui_pref(local_db_path, "review.hide_identical", "1")
        resolved_hide_identical = pref_value != "0"
        run_review_tui(
            db_path=local_db_path,
            local_root=local_root,
            remote_address=remote_cfg.address,
            hide_identical=resolved_hide_identical,
        )
    else:
        console.print(
            "Run `li-sync review` to inspect changes in the interactive tree UI."
        )


@app.command()
def review(
    local_root: Path = typer.Option(
        DEFAULT_LOCAL_ROOT,
        help="Local root folder; used for default DB location",
    ),
    db_path: Path | None = typer.Option(
        None,
        help="Path to local SQLite status DB (default: <local_root>/.li-sync/state.sqlite3)",
    ),
    hide_identical: bool | None = typer.Option(
        None,
        help="Hide folders that are completely identical (default: persisted preference)",
    ),
) -> None:
    """Open interactive tree review UI for the current saved scan state."""
    resolved_local_root = local_root.expanduser().resolve()
    resolved_db = (
        db_path.expanduser().resolve()
        if db_path is not None
        else _local_state_db_path(resolved_local_root)
    )
    if not resolved_db.exists():
        console.print(f"[red]State DB not found:[/red] {resolved_db}")
        raise typer.Exit(1)

    context = get_state_context(resolved_db)
    if context is None:
        console.print("No scan state recorded yet. Run `li-sync scan` first.")
        raise typer.Exit(1)

    if hide_identical is None:
        pref_value = get_ui_pref(resolved_db, "review.hide_identical", "1")
        resolved_hide_identical = pref_value != "0"
    else:
        resolved_hide_identical = hide_identical

    run_review_tui(
        db_path=resolved_db,
        local_root=resolved_local_root,
        remote_address=context.remote_address,
        hide_identical=resolved_hide_identical,
    )


if __name__ == "__main__":
    app()
