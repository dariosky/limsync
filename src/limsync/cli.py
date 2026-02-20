from __future__ import annotations

import concurrent.futures
import threading
import time
from pathlib import Path, PurePosixPath

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .compare import compare_records
from .config import RemoteConfig
from .deletion_intent import apply_intentional_deletion_hints
from .endpoints import (
    EndpointSpec,
    default_endpoint_state_db,
    default_review_db_path,
    endpoint_to_string,
    parse_endpoint,
)
from .models import ContentState, FileRecord, MetadataState
from .planner_apply import ApplySettings
from .review_tui import run_review_tui
from .scanner_local import LocalScanner
from .scanner_remote import RemoteScanner
from .state_db import (
    ScanStateSummary,
    get_state_context,
    get_ui_pref,
    load_current_diffs,
    save_current_state,
)

app = typer.Typer(
    help="Interactive Dropbox-like sync tooling over SSH",
    invoke_without_command=True,
)
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


def _endpoint_root_name(endpoint: EndpointSpec) -> str:
    root = Path(endpoint.root)
    name = root.name or endpoint.root
    prefix = "remote:" if endpoint.is_remote else "local:"
    return f"{prefix}{name}"


def _run_scan(
    source: str,
    destination: str,
    state_db: Path | None,
    open_review: bool,
    apply_ssh_compression: bool,
) -> None:
    try:
        source_endpoint = parse_endpoint(source)
        destination_endpoint = parse_endpoint(destination)
    except ValueError as exc:
        console.print(f"[red]Invalid endpoint:[/red] {exc}")
        raise typer.Exit(1)

    review_db_path = (
        state_db.expanduser().resolve()
        if state_db is not None
        else default_review_db_path(source_endpoint, destination_endpoint)
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        console=console,
    ) as progress:
        progress_lock = threading.Lock()
        source_task = progress.add_task("Preparing source scan...", total=None)
        destination_task = progress.add_task(
            "Preparing destination scan...", total=None
        )
        source_reporter = ScanProgressReporter(
            progress,
            source_task,
            _endpoint_root_name(source_endpoint),
            progress_lock,
        )
        destination_reporter = ScanProgressReporter(
            progress,
            destination_task,
            _endpoint_root_name(destination_endpoint),
            progress_lock,
        )

        def run_scan(
            endpoint: EndpointSpec,
            reporter: ScanProgressReporter,
        ) -> tuple[dict[str, FileRecord], float]:
            started = time.perf_counter()
            if endpoint.is_local:
                records = LocalScanner(Path(endpoint.root)).scan(
                    progress_cb=reporter.update
                )
            else:
                records = RemoteScanner(
                    RemoteConfig(
                        host=str(endpoint.host),
                        user=str(endpoint.user),
                        port=endpoint.port or 22,
                        root=endpoint.root,
                        state_db=default_endpoint_state_db(endpoint),
                    )
                ).scan(progress_cb=reporter.update)
            return records, (time.perf_counter() - started)

        source_records: dict[str, FileRecord]
        destination_records: dict[str, FileRecord]
        source_elapsed = 0.0
        destination_elapsed = 0.0

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            future_source = pool.submit(run_scan, source_endpoint, source_reporter)
            future_destination = pool.submit(
                run_scan, destination_endpoint, destination_reporter
            )

            try:
                source_records, source_elapsed = future_source.result()
            except Exception as exc:
                progress.stop()
                console.print(f"[red]Source scan failed:[/red] {exc}")
                raise typer.Exit(1)

            try:
                destination_records, destination_elapsed = future_destination.result()
            except Exception as exc:
                progress.stop()
                console.print(f"[red]Destination scan failed:[/red] {exc}")
                raise typer.Exit(1)

        with progress_lock:
            progress.update(
                source_task,
                description=(
                    f"Source scan completed  files={len(source_records)}  "
                    f"time={_format_seconds(source_elapsed)}"
                ),
            )
            progress.update(
                destination_task,
                description=(
                    f"Destination scan completed  files={len(destination_records)}  "
                    f"time={_format_seconds(destination_elapsed)}"
                ),
            )

    previous_content_states: dict[str, str] = {}
    state_context = get_state_context(review_db_path)
    if (
        state_context is not None
        and state_context.source_endpoint == endpoint_to_string(source_endpoint)
        and state_context.destination_endpoint
        == endpoint_to_string(destination_endpoint)
    ):
        for row in load_current_diffs(review_db_path):
            previous_content_states[str(row["relpath"])] = str(row["content_state"])

    diffs = compare_records(source_records, destination_records)
    diffs = apply_intentional_deletion_hints(diffs, previous_content_states)

    counts = {
        ContentState.ONLY_LEFT: 0,
        ContentState.ONLY_RIGHT: 0,
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
        source_endpoint=endpoint_to_string(source_endpoint),
        destination_endpoint=endpoint_to_string(destination_endpoint),
        source_scan_seconds=source_elapsed,
        destination_scan_seconds=destination_elapsed,
        source_files=len(source_records),
        destination_files=len(destination_records),
        compared_paths=len(diffs),
        only_source=counts[ContentState.ONLY_LEFT],
        only_destination=counts[ContentState.ONLY_RIGHT],
        different_content=counts[ContentState.DIFFERENT],
        uncertain=counts[ContentState.UNKNOWN],
        metadata_only=metadata_only,
    )
    save_current_state(review_db_path, run_summary, diffs)

    console.print()
    console.print(f"Source files: {len(source_records)}")
    console.print(f"Destination files: {len(destination_records)}")
    console.print(f"Compared paths: {len(diffs)}")
    console.print(f"Only source: {counts[ContentState.ONLY_LEFT]}")
    console.print(f"Only destination: {counts[ContentState.ONLY_RIGHT]}")
    console.print(f"Different content: {counts[ContentState.DIFFERENT]}")
    console.print(f"Uncertain (same size, mtime drift): {counts[ContentState.UNKNOWN]}")
    console.print(f"Metadata-only drift: {metadata_only}")
    console.print(f"Review DB: {review_db_path}")

    if open_review:
        pref_value = get_ui_pref(review_db_path, "review.hide_identical", "1")
        resolved_hide_identical = pref_value != "0"
        run_review_tui(
            db_path=review_db_path,
            source_endpoint=source_endpoint,
            destination_endpoint=destination_endpoint,
            hide_identical=resolved_hide_identical,
            apply_settings=ApplySettings(ssh_compression=apply_ssh_compression),
        )
    else:
        console.print("Run `limsync review --state-db <db_path>` to inspect changes.")


@app.callback()
def _default_command(
    ctx: typer.Context,
    source: str | None = typer.Option(
        None,
        help="Source endpoint (local path, local:/path, ssh://user@host/path, or user@host:path)",
    ),
    destination: str | None = typer.Option(
        None,
        help="Destination endpoint (local path, local:/path, ssh://user@host/path, or user@host:path)",
    ),
    state_db: Path | None = typer.Option(
        None,
        help="Local SQLite path for review state (default: ~/.limsync/<source>__<destination>.sqlite3)",
    ),
    open_review: bool = typer.Option(
        True,
        help="Open interactive review UI after scan",
    ),
    apply_ssh_compression: bool = typer.Option(
        False,
        "--apply-ssh-compression/--no-apply-ssh-compression",
        help="Enable SSH transport compression during apply operations in the review UI.",
    ),
) -> None:
    """Run scan when no subcommand is provided."""
    if ctx.invoked_subcommand is not None:
        return
    if source is None or destination is None:
        console.print(
            "[red]Missing required options.[/red] Use `--source` and `--destination`."
        )
        console.print(ctx.get_help())
        raise typer.Exit(2)
    _run_scan(source, destination, state_db, open_review, apply_ssh_compression)


@app.command()
def review(
    state_db: Path | None = typer.Option(
        None,
        help="Path to local SQLite review DB",
    ),
    source: str | None = typer.Option(
        None,
        help="Source endpoint used to infer default --state-db when omitted",
    ),
    destination: str | None = typer.Option(
        None,
        help="Destination endpoint used to infer default --state-db when omitted",
    ),
    hide_identical: bool | None = typer.Option(
        None,
        help="Hide folders that are completely identical (default: persisted preference)",
    ),
    apply_ssh_compression: bool = typer.Option(
        False,
        "--apply-ssh-compression/--no-apply-ssh-compression",
        help="Enable SSH transport compression during apply operations.",
    ),
) -> None:
    """Open interactive tree review UI for the current saved scan state."""
    if state_db is not None:
        resolved_db = state_db.expanduser().resolve()
    else:
        if source is None or destination is None:
            console.print(
                "[red]Missing review DB.[/red] Provide --state-db, or both --source and --destination."
            )
            raise typer.Exit(1)
        try:
            source_endpoint = parse_endpoint(source)
            destination_endpoint = parse_endpoint(destination)
        except ValueError as exc:
            console.print(f"[red]Invalid endpoint:[/red] {exc}")
            raise typer.Exit(1)
        resolved_db = default_review_db_path(source_endpoint, destination_endpoint)

    if not resolved_db.exists():
        console.print(f"[red]State DB not found:[/red] {resolved_db}")
        raise typer.Exit(1)

    context = get_state_context(resolved_db)
    if context is None:
        console.print("No scan state recorded yet. Run `limsync scan` first.")
        raise typer.Exit(1)

    try:
        source_endpoint = parse_endpoint(context.source_endpoint)
        destination_endpoint = parse_endpoint(context.destination_endpoint)
    except ValueError as exc:
        console.print(f"[red]Invalid state context:[/red] {exc}")
        raise typer.Exit(1)

    if hide_identical is None:
        pref_value = get_ui_pref(resolved_db, "review.hide_identical", "1")
        resolved_hide_identical = pref_value != "0"
    else:
        resolved_hide_identical = hide_identical

    run_review_tui(
        db_path=resolved_db,
        source_endpoint=source_endpoint,
        destination_endpoint=destination_endpoint,
        hide_identical=resolved_hide_identical,
        apply_settings=ApplySettings(ssh_compression=apply_ssh_compression),
    )


if __name__ == "__main__":
    app()
