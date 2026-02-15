# li-sync

Initial implementation of an interactive Dropbox-like bidirectional sync tool over SSH.

## Current status
- Phase 1 foundation in place.
- Local scanner with support for:
  - nested `.dropboxignore`
  - excluded folders (`node_modules`, `.tox`, cache dirs)
- Remote scanner over SSH helper (streamed JSONL) with nested `.dropboxignore` + excluded folders.
- Local and remote scan status are persisted in SQLite under each sync root:
  - `<local_root>/.li-sync/state.sqlite3`
  - `<remote_root>/.li-sync/state.sqlite3`
- First comparison report that separates:
  - content status
  - metadata status (mode + mtime)
- scan progress with adaptive path depth for long-running scans
- Always excludes `.DS_Store`, excluded folder families, and `.li-sync`.

## Install dependencies

```bash
uv sync
```

## Run

```bash
uv run li-sync --help
uv run li-sync scan
uv run li-sync review
```

### Useful scan overrides

```bash
uv run li-sync scan \
  --local-root /Users/dario.varotto/Dropbox \
  --remote-user dario \
  --remote-host 192.168.18.18 \
  --remote-root '~/Dropbox'

# Scan without opening the review UI immediately
uv run li-sync scan --no-open-review

# Review from local SQLite state (latest run)
uv run li-sync review \
  --local-root /Users/dario.varotto/Dropbox \
  --run-id 12
```

## Review UI keys
- Arrow keys: navigate tree
- Right: expand folder
- Left: collapse folder
- `h`: show/hide completely identical folders (preference persisted in SQLite)
- `q`: quit

## Notes
- `review` and `apply` commands are placeholders for upcoming phases.
- xattr-based exclusion is intentionally disabled in current scan path for performance.
- First-run destructive actions (deletes) are intentionally not implemented yet.
