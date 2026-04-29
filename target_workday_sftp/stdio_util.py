"""Detach process stdio from pipes (Hotglue / piped runners)."""

from __future__ import annotations

import os


def detach_stdio_from_pipes() -> None:
    """Point stdin/stdout/stderr at ``/dev/null``.

    Piped parents may close the read end after the last log line they consume. Any later
    write (including from ``ssh.close()`` / paramiko teardown) can SIGPIPE the process (141).
    """
    try:
        dn = os.open(os.devnull, os.O_RDWR)
    except OSError:
        return
    try:
        for fd in (0, 1, 2):
            try:
                os.dup2(dn, fd)
            except OSError:
                pass
    finally:
        if dn > 2:
            os.close(dn)
