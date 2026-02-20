import fnmatch
import os
from pathlib import PurePosixPath


def _to_posix(path: PurePosixPath) -> str:
    return "." if str(path) == "." else path.as_posix()


class IgnoreRules:
    """Evaluates nested `.dropboxignore` files with gitignore-like patterns."""

    def __init__(self) -> None:
        self._patterns: dict[str, list[str]] = {}

    def add_spec(self, base_relpath: PurePosixPath, lines: list[str]) -> None:
        patterns = []
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            patterns.append(line)
        if patterns:
            self._patterns[_to_posix(base_relpath)] = patterns

    def load_if_exists(self, root: str, dir_relpath: PurePosixPath) -> None:
        rel = "" if str(dir_relpath) == "." else dir_relpath.as_posix()
        candidate = os.path.join(root, rel, ".dropboxignore")
        if not os.path.isfile(candidate):
            return
        try:
            with open(candidate, encoding="utf-8", errors="replace") as f:
                lines = f.read().splitlines()
        except OSError:
            return
        self.add_spec(dir_relpath, lines)

    def _pattern_matches(self, local_target: str, pattern: str, anchored: bool) -> bool:
        target = local_target.rstrip("/")
        if anchored:
            return fnmatch.fnmatch(target, pattern)

        if "/" not in pattern:
            if fnmatch.fnmatch(target, pattern):
                return True
            parts = [p for p in target.split("/") if p]
            return any(fnmatch.fnmatch(part, pattern) for part in parts)

        if fnmatch.fnmatch(target, pattern):
            return True
        parts = [p for p in target.split("/") if p]
        for idx in range(1, len(parts)):
            suffix = "/".join(parts[idx:])
            if fnmatch.fnmatch(suffix, pattern):
                return True
        return False

    def _match_patterns(
        self, local_target: str, is_dir: bool, patterns: list[str]
    ) -> bool | None:
        result: bool | None = None
        for raw in patterns:
            negate = raw.startswith("!")
            pattern = raw[1:] if negate else raw
            if not pattern:
                continue

            dir_only = pattern.endswith("/")
            if dir_only:
                pattern = pattern.rstrip("/")

            anchored = pattern.startswith("/")
            if anchored:
                pattern = pattern.lstrip("/")

            if self._pattern_matches(local_target, pattern, anchored):
                result = not negate
        return result

    def is_ignored(self, relpath: PurePosixPath, is_dir: bool) -> bool:
        target = relpath.as_posix()
        if is_dir and not target.endswith("/"):
            target = f"{target}/"

        ancestors = [PurePosixPath(".")]
        parts = relpath.parts
        for idx in range(len(parts) - 1):
            ancestors.append(PurePosixPath(*parts[: idx + 1]))

        ignored = False
        for ancestor in ancestors:
            anc_key = _to_posix(ancestor)
            patterns = self._patterns.get(anc_key)
            if not patterns:
                continue

            if anc_key == ".":
                local_target = target
            else:
                prefix = f"{anc_key}/"
                if not target.startswith(prefix):
                    continue
                local_target = target[len(prefix) :]

            matched = self._match_patterns(local_target, is_dir, patterns)
            if matched is not None:
                ignored = matched

        return ignored
