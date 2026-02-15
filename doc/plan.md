# Dropbox-like Bidirectional SSH Sync Tool - Plan

## Context and Constraints
- Goal: build an interactive Python tool to sync two diverged folders over SSH in a LAN.
- Local root: `/Users/dario.varotto/Dropbox`
- Remote root: `ssh://dario@192.168.18.18:~/Dropbox`
- Passwordless SSH is already configured.
- Prior state: folders were historically synced with Dropbox, then diverged.
- Safety: first run must be conservative; deletions are manual only.
- Owner usernames differ across hosts (`dario.varotto` vs `dario`) and owner differences should not be treated as actionable drift.

## Agreed Product Decisions
1. Engine + TUI architecture:
- Core sync engine in Python (scan, compare, plan, apply).
- Interactive terminal UI (tree-oriented) for review and approvals.

2. Metadata policy:
- File content and metadata are compared separately.
- Permission bits should be synced.
- Owner/group mismatches are expected across hosts and should not drive sync actions.

3. First-run behavior:
- No automatic deletions.
- Deletions require explicit user action in the review UI.
- Bulk handling at folder level must be supported.

4. Conflict behavior:
- No automatic conflict suffix files.
- UI must allow inspecting/opening local and remote versions before selecting action.

5. Exclusion behavior:
- Do not use local xattr exclusion in sync scan path (for performance on large trees).
- Respect `.dropboxignore` files in any subfolder (gitignore-like pattern semantics).
- Exclude folder names:
  - `CACHE_FOLDERS = {"__pycache__", ".pytest_cache", ".cache", ".ruff_cache"}`
  - `EXCLUDED_FOLDERS = {"node_modules", ".tox", ".li-sync"} | CACHE_FOLDERS`
- Always exclude `.DS_Store`.

## Proposed Architecture

### 1) Core modules
- `sync_core/models.py`
  - Data classes for nodes, file signatures, diff states, planned actions.
- `sync_core/excludes.py`
  - xattr check (local side), `.dropboxignore` parser/evaluator, static excluded folder names.
- `sync_core/scanner_local.py`
  - Local recursive walk with metadata/signature collection.
- `sync_core/scanner_remote.py`
  - Remote scan via SSH-executed helper script returning normalized JSON records.
- `sync_core/compare.py`
  - Two-phase compare: cheap checks first, hash-on-demand when uncertain.
- `sync_core/planner.py`
  - Build action plan with explicit conflict/manual-delete markers.
- `sync_core/apply.py`
  - Execute approved actions over local FS and SSH/SFTP remote operations.
- `sync_core/state_db.py`
  - SQLite snapshot history to improve delete detection in future runs.

### 2) Interfaces
- CLI (`Typer`): `scan`, `review`, `apply`, `run --dry-run`.
- TUI (`Textual`):
  - Tree view with folder-level rollups.
  - Filter toggles (conflicts, metadata-only, only-local, only-remote, deletes).
  - Action assignment at file/folder level.
  - "Open local" / "Open remote" affordances.

### 3) Transport
- SSH for control and remote scanning via streamed helper process.
- SFTP or SCP for file transfer operations.
- Optional future optimization: rsync-backed transfer execution while preserving planner/UI decisions.

### 4) State Persistence
- Store local scan/diff status in `<local_root>/.li-sync/state.sqlite3`.
- Store remote scan snapshot status in `<remote_root>/.li-sync/state.sqlite3`.

## Data Model (High-level)
- For each path:
  - `type`: file/dir/link
  - `content_state`: identical | different | only_local | only_remote | unknown
  - `metadata_state`: identical | different | not_applicable
  - `metadata_diff`: mode, mtime, xattrs subset, etc.
  - `conflict`: boolean
  - `recommended_action`: sync_left_to_right | sync_right_to_left | chmod_left | chmod_right | skip | manual_delete
  - `user_action`: nullable override from review UI

## Comparison Strategy
1. Scan both trees with excludes applied.
2. Fast equality check by `(type, size, mtime_ns)` where safe.
3. Mark uncertain cases when size matches but timestamps drift.
4. Detect metadata-only drift when metadata differs and content appears unchanged by cheap checks.
5. Mark delete candidates as manual on first run.

## Review UX Requirements
- Tree-first navigation with aggregate status counts per folder.
- Single action can apply to full subtree.
- Fast preview panel for selected node.
- "Open local" and "Open remote" commands before conflict resolution.
- Non-destructive by default until user confirms apply.

## Execution Policy
- Default mode: dry-run and review.
- Apply mode requires explicit confirmation.
- First run: deletions disabled unless explicitly chosen by the user.
- Logs/audit trail saved for each run.

## Phased Implementation Plan

### Phase 1 - Foundation
- Project scaffold (package layout, CLI entrypoint, config, logging).
- Local scanner + static excludes + `.dropboxignore` parser.
- Remote scanner (SSH helper) with normalized record format and remote SQLite snapshot persistence.

### Phase 2 - Diff Engine
- Implement compare states and metadata-only detection.
- Implement planner with conservative first-run defaults.
- Produce rich dry-run reports.

### Phase 3 - Apply Engine
- Implement upload/download/create-dir/update-perms.
- Implement guarded delete workflow (manual-only).
- Add run journaling and rollback-friendly logging.

### Phase 4 - Interactive TUI
- Tree browser and filters.
- Folder-level bulk actions.
- Conflict inspection hooks (open local/open remote).

### Phase 5 - Persistence and Reliability
- SQLite state snapshots.
- Better rename/move heuristics (future).
- Performance and correctness hardening.

## Testing Strategy
- Unit tests for excludes, compare logic, planner transitions.
- Integration tests using temporary local trees + SSH test target.
- Golden tests for `.dropboxignore` semantics.
- Safety tests for first-run delete suppression.

## Risks and Mitigations
- Diverged history may produce many conflicts:
  - Mitigation: tree-level bulk actions + robust filtering.
- Metadata portability differences across OS/filesystems:
  - Mitigation: configurable metadata policy and explicit ignore for owner mismatch.
- Large initial scan cost:
  - Mitigation: hash-on-demand, persisted state cache.

## Success Criteria
- Can scan both roots and produce accurate diff with excludes.
- Clearly identifies content-vs-metadata drift.
- Enables interactive review and subtree approvals.
- Performs safe, auditable, user-approved synchronization over SSH.
