from __future__ import annotations

import unicodedata


def normalize_text(value: str) -> str:
    """Return UTF-8 safe text by collapsing surrogate-escaped bytes.

    Filesystem paths may contain undecodable bytes represented as lone surrogates.
    SQLite text binding and terminal rendering reject those, so normalize them to
    replacement characters while keeping valid UTF-8 data untouched.
    Also canonicalize to NFC so macOS/Linux path forms match for unicode names.
    """
    safe = value.encode("utf-8", "surrogateescape").decode("utf-8", "replace")
    return unicodedata.normalize("NFC", safe)
