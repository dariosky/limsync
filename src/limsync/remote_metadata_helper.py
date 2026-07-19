from __future__ import annotations

import argparse
import json
import os
import signal
import stat
import sys
from pathlib import PurePosixPath

SUPPORTED_FIELDS = {"mode", "mtime"}
_CANCEL_REQUESTED = False


def _request_cancel(_signum: int, _frame: object) -> None:
    global _CANCEL_REQUESTED
    _CANCEL_REQUESTED = True


def _emit(payload: dict[str, object], *, flush: bool = False) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True) + "\n")
    if flush:
        sys.stdout.flush()


def _safe_path(root: str, relpath: str) -> str:
    relative = PurePosixPath(relpath)
    if relative.is_absolute() or relpath in {"", "."} or ".." in relative.parts:
        raise ValueError(f"unsafe relative path: {relpath!r}")
    target = os.path.abspath(os.path.join(root, *relative.parts))
    if os.path.commonpath((root, target)) != root:
        raise ValueError(f"path escapes root: {relpath!r}")
    return target


def _fields(request: dict[str, object]) -> tuple[str, ...]:
    raw_fields = request.get("fields")
    if not isinstance(raw_fields, list):
        raise ValueError("fields must be a list")
    fields = tuple(str(field) for field in raw_fields)
    unsupported = set(fields) - SUPPORTED_FIELDS
    if unsupported:
        raise ValueError(f"unsupported metadata fields: {sorted(unsupported)}")
    if not fields:
        raise ValueError("at least one metadata field is required")
    return fields


def process_request(
    mode: str,
    root: str,
    request: dict[str, object],
) -> dict[str, object]:
    request_id = request.get("id")
    relpath = str(request.get("relpath", ""))
    response: dict[str, object] = {"id": request_id, "relpath": relpath}
    try:
        fields = _fields(request)
        target = _safe_path(root, relpath)
        target_stat = os.lstat(target)
        if stat.S_ISLNK(target_stat.st_mode):
            response.update(ok=True, noop=True)
            return response

        if mode == "read":
            if "mode" in fields:
                response["mode"] = stat.S_IMODE(target_stat.st_mode)
            if "mtime" in fields:
                response["mtime_ns"] = target_stat.st_mtime_ns
        elif mode == "apply":
            if "mode" in fields:
                requested_mode = request.get("mode")
                if (
                    not isinstance(requested_mode, int)
                    or not 0 <= requested_mode <= 0o7777
                ):
                    raise ValueError("mode must be an integer permission value")
                os.chmod(target, requested_mode)
            if "mtime" in fields:
                requested_mtime = request.get("mtime_ns")
                if not isinstance(requested_mtime, int):
                    raise ValueError("mtime_ns must be an integer")
                os.utime(target, ns=(target_stat.st_atime_ns, requested_mtime))
        else:
            raise ValueError(f"unsupported helper mode: {mode}")
        response["ok"] = True
    except Exception as exc:  # noqa: BLE001
        response.update(ok=False, error=str(exc))
    return response


def run(mode: str, root_arg: str) -> int:
    global _CANCEL_REQUESTED
    _CANCEL_REQUESTED = False
    root = os.path.abspath(os.path.expanduser(root_arg))
    if not os.path.isdir(root):
        _emit({"event": "fatal", "error": f"Root not found: {root}"}, flush=True)
        return 2

    _emit({"event": "ready", "pid": os.getpid()}, flush=True)
    processed = 0
    for raw_line in sys.stdin:
        if _CANCEL_REQUESTED:
            break
        line = raw_line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
            if not isinstance(request, dict):
                raise ValueError("request must be a JSON object")
            response = process_request(mode, root, request)
        except Exception as exc:  # noqa: BLE001
            response = {"id": None, "relpath": "", "ok": False, "error": str(exc)}
        processed += 1
        _emit(response, flush=(not bool(response.get("ok")) or processed % 100 == 0))

    _emit(
        {
            "event": "done",
            "processed": processed,
            "cancelled": _CANCEL_REQUESTED,
        },
        flush=True,
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LimSync remote metadata helper")
    parser.add_argument("--mode", choices=("read", "apply"), required=True)
    parser.add_argument("--root", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if hasattr(signal, "SIGUSR1"):
        signal.signal(signal.SIGUSR1, _request_cancel)
    return run(args.mode, args.root)


if __name__ == "__main__":
    raise SystemExit(main())
