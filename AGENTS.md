# AGENTS.md - Dropbox SSH Sync Project Guidance

This file defines project-specific operating instructions for future agentic coding sessions.

## Project Goal
Build a Python tool to bidirectionally sync:
- Local: `/Users/dario.varotto/Dropbox`
- Remote: `ssh://dario@192.168.18.18:~/Dropbox`

The tool must prioritize safe reconciliation of two trees that likely diverged after a long period without sync.

## Product Direction
- Language: Python.
- Architecture: core sync engine + interactive terminal UI.
- Primary interaction mode: review first, apply second.
- Transport: SSH-based remote operations.
- State model: single current workspace state (no run-history workflow).

## Core Requirements
1. Distinguish content differences from metadata differences.
- Users must see when two files are byte-identical but metadata differs.

2. Metadata handling.
- Sync permission bits.
- Do not treat owner/group mismatch as actionable drift by default.
  Context: usernames differ (`dario.varotto` locally, `dario` remotely).

3. Safety defaults.
- First run must be non-destructive.
- Deletions are manual-only on first run.
- No automatic conflict-suffix file generation.

4. Conflict workflow.
- UI must let user inspect left/local and right/remote content before choosing an action.

5. Tree-level operations.
- UI must support folder/subtree approvals to avoid per-file confirmation for large folders.

## Exclusion Rules (Mandatory)
Apply exclusions before diffing/planning.

### A) Local xattr exclusion
- Current policy: do not use `com.dropbox.ignored` xattr as a sync exclusion.
- Reason: xattr checks were too expensive on large scans.
- This may be reintroduced later as an optional mode.

### B) `.dropboxignore` files
- `.dropboxignore` may exist in any subfolder.
- Semantics should be gitignore-like for pattern behavior.
- Rules should apply within their directory scope, similar to nested ignore files.

### C) Excluded folder names
Use exactly:

```python
CACHE_FOLDERS = {"__pycache__", ".pytest_cache", ".cache", ".ruff_cache"}
EXCLUDED_FOLDERS = {"node_modules", ".tox", ".venv", ".li-sync"} | CACHE_FOLDERS
```

Any path under those directories must be excluded from sync planning and apply operations.

### D) Always-excluded filenames
- `.DS_Store` is always excluded on both sides.
- `Icon\\r` (Finder custom-icon marker files) is always excluded on both sides.

## Expected UX
- Interactive TUI (preferred stack: Textual + Rich).
- Tree view with folder rollups and status counts.
- Fast filtering: conflicts, metadata-only changes, only-local, only-remote, delete candidates.
- Per-item and per-subtree action assignment.
- Dry-run first; explicit confirmation before apply.
- Hide completely identical folders by default, with persisted UI preference in local SQLite.

## CLI Surface (Target)
- `li-sync scan`
- `li-sync review`

Apply is intentionally executed inside the review TUI (`a`) with explicit confirmation.
Avoid irreversible behavior in default commands.

## Data Modeling Guidance
Represent each path with independent states:
- `content_state` (identical/different/only_local/only_remote)
- `metadata_state` (identical/different/not_applicable)
- `metadata_diff` details (at least mode and mtime; owners informative only)
- `recommended_action`
- `user_action` override

This is required to satisfy "identical content but metadata drift" visibility.

## First-Run Policy
- Delete actions must be manual-only.
- If previous sync baseline is missing, treat deletes conservatively.
- Prefer explicit review queue for destructive operations.

## Implementation Notes
- Use a remote helper script over SSH for fast remote scans returning structured JSONL.
- Persist local scan status in `<local_root>/.li-sync/state.sqlite3`.
- Persist remote scan status in `<remote_root>/.li-sync/state.sqlite3`.
- Compare with cheap checks first.
- Do not hash file content during initial scan phase.
- Persist one current diff/worktree state in SQLite and preserve user action overrides when paths remain applicable.
- During apply, update UI state incrementally as operations complete; do not trigger a full rescan.

## Testing Priorities
1. Exclusion correctness (`xattr`, nested `.dropboxignore`, excluded dirs).
2. Compare engine correctness for content vs metadata-only diffs.
3. Planner safety (first-run delete suppression).
4. Integration with SSH target for scan/plan/apply loop.

## Non-goals (for initial versions)
- Automatic conflict file duplication with suffixes.
- Aggressive auto-delete logic without baseline/history.
- Owner/UID harmonization across hosts.

## Workflow Expectations for Future Agents
- Start by validating safety assumptions (dry-run first).
- Do not introduce destructive defaults.
- Preserve clear, inspectable decision points in UI and logs.
- Keep modules small and testable; separate scanner/compare/planner/apply concerns.
