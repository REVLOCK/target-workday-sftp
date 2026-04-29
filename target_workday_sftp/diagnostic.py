"""Optional on-disk trace (stderr may be unusable after stdio detach / SIGPIPE)."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Optional, TextIO

_lock = threading.Lock()
_file: Optional[TextIO] = None


def diagnostic_active() -> bool:
    return _file is not None


def init_diagnostic(path: Any) -> None:
    """Append JSON lines to *path* if non-empty (str). Safe to call once per process."""
    global _file
    if _file is not None:
        return
    if path is None or path == "":
        return
    p = Path(str(path).strip()).expanduser()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        _file = p.open("a", encoding="utf-8")
        _write(
            "init",
            {"path": str(p.resolve())},
        )
    except OSError:
        _file = None


def close_diagnostic() -> None:
    global _file
    with _lock:
        if _file is not None:
            try:
                _file.close()
            except OSError:
                pass
            _file = None


def diag(phase: str, detail: Optional[dict[str, Any]] = None) -> None:
    """Record a phase marker (works after stdio is detached)."""
    _write(phase, detail or {})


def _write(phase: str, detail: dict[str, Any]) -> None:
    with _lock:
        if _file is None:
            return
        line = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "phase": phase,
            "detail": detail,
        }
        try:
            _file.write(json.dumps(line, ensure_ascii=False) + "\n")
            _file.flush()
        except OSError:
            pass
