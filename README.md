# li-sync

Initial implementation of an interactive Dropbox-like bidirectional sync tool over SSH.

## Current status
- Phase 1 foundation in place.
- Local scanner with support for:
  - nested `.dropboxignore`
  - excluded folders (`node_modules`, `.tox`, cache dirs)
- Remote scanner over SSH helper (streamed JSONL) with nested `.dropboxignore` + excluded folders.
- Remote helper persists scan snapshot metadata in SQLite (`~/.cache/li-sync/scan_state.sqlite3` by default).
- First comparison report that separates:
  - content status
  - metadata status (mode + mtime)
  - scan progress with adaptive path depth for long-running scans
- Always excludes `.DS_Store` and excluded folder families.

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
  --remote-root '~/Dropbox' \
  --remote-state-db '~/.cache/li-sync/scan_state.sqlite3'

# Review the full saved diff set (not only top N)
uv run li-sync review \
  --diff-file doc/last_scan_diff.jsonl \
  --content only_remote \
  --offset 0 \
  --limit 100
```

## Notes
- `review` and `apply` commands are placeholders for upcoming phases.
- xattr-based exclusion is intentionally disabled in current scan path for performance.
- First-run destructive actions (deletes) are intentionally not implemented yet.
