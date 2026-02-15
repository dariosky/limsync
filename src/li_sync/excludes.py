from __future__ import annotations

from pathlib import Path, PurePosixPath

from pathspec import PathSpec

from .config import EXCLUDED_FILE_NAMES, EXCLUDED_FOLDERS


def _to_posix(path: PurePosixPath) -> str:
    return "." if str(path) == "." else path.as_posix()


class IgnoreRules:
    """Evaluates nested `.dropboxignore` files with gitignore-style patterns."""

    def __init__(self) -> None:
        self._specs: dict[str, PathSpec] = {}

    def add_spec(self, base_relpath: PurePosixPath, lines: list[str]) -> None:
        clean = [
            line.strip()
            for line in lines
            if line.strip() and not line.lstrip().startswith("#")
        ]
        if not clean:
            return
        base = _to_posix(base_relpath)
        self._specs[base] = PathSpec.from_lines("gitwildmatch", clean)

    def load_if_exists(self, root: Path, dir_relpath: PurePosixPath) -> None:
        candidate = (
            root
            / ("" if str(dir_relpath) == "." else dir_relpath.as_posix())
            / ".dropboxignore"
        )
        if not candidate.exists() or not candidate.is_file():
            return
        lines = candidate.read_text(encoding="utf-8", errors="replace").splitlines()
        self.add_spec(dir_relpath, lines)

    def is_ignored(self, relpath: PurePosixPath, is_dir: bool) -> bool:
        parts = relpath.parts
        ancestors = [PurePosixPath(".")]
        for idx in range(len(parts) - 1):
            ancestors.append(PurePosixPath(*parts[: idx + 1]))

        target = relpath.as_posix()
        if is_dir and not target.endswith("/"):
            target = f"{target}/"

        ignored: bool | None = None
        for ancestor in ancestors:
            anc_key = _to_posix(ancestor)
            spec = self._specs.get(anc_key)
            if spec is None:
                continue

            if anc_key == ".":
                local_target = target
            else:
                anc_prefix = f"{anc_key}/"
                if not target.startswith(anc_prefix):
                    continue
                local_target = target[len(anc_prefix) :]

            if spec.match_file(local_target):
                ignored = True

            # Handle negation patterns: pathspec internally supports it, but no state leak
            # exists between independent match calls, so we test raw form too.
            if is_dir and spec.match_file(local_target.rstrip("/")):
                ignored = True

        return bool(ignored)


def is_excluded_folder_name(name: str) -> bool:
    return name in EXCLUDED_FOLDERS


def is_excluded_file_name(name: str) -> bool:
    return name in EXCLUDED_FILE_NAMES
