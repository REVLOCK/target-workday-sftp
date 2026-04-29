"""SFTP config and upload."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import paramiko
from paramiko import SSHException

from target_workday_sftp.const import (
    SFTP_AUTH_TIMEOUT,
    SFTP_BANNER_TIMEOUT,
    SFTP_CONNECT_TIMEOUT,
)
from target_workday_sftp.diagnostic import diag
from target_workday_sftp.exceptions import SftpUploadError
from target_workday_sftp.stdio_util import detach_stdio_from_pipes

logger = logging.getLogger(__name__)

# hotgluexyz/target-sftp: silence paramiko's own loggers so teardown (transport/channel)
# does not spam stderr—helps piped runners that close the log pipe early (SIGPIPE / 141).
logging.getLogger("paramiko").setLevel(logging.CRITICAL)


@dataclass(frozen=True)
class SftpConnectionConfig:
    """SFTP connection settings."""

    host: str
    port: int
    username: str
    remote_path: str
    password: Optional[str]
    private_key_path: Optional[str]
    private_key_passphrase: Optional[str]
    connect_timeout: int = SFTP_CONNECT_TIMEOUT
    banner_timeout: int = SFTP_BANNER_TIMEOUT
    auth_timeout: int = SFTP_AUTH_TIMEOUT

    @classmethod
    def from_target_config(cls, config: Mapping[str, Any]) -> SftpConnectionConfig:
        """Load from flat target config."""
        port_raw = config.get("sftp_port")
        if port_raw in (None, ""):
            port = 22
        else:
            try:
                port = int(port_raw)
            except (TypeError, ValueError) as exc:
                raise SftpUploadError(f"Invalid sftp_port: {port_raw!r}") from exc
        if not 1 <= port <= 65535:
            raise SftpUploadError(f"sftp_port out of range: {port}")

        host = str(config["sftp_host"]).strip()
        username = str(config["sftp_username"]).strip()
        remote_path = str(config["sftp_remote_path"]).strip()
        if not host:
            raise SftpUploadError("sftp_host is empty")
        if not username:
            raise SftpUploadError("sftp_username is empty")
        if not remote_path:
            raise SftpUploadError("sftp_remote_path is empty")

        pwd = config.get("sftp_password")
        password = str(pwd) if pwd not in (None, "") else None

        key_raw = config.get("sftp_private_key_path")
        key_path = str(Path(key_raw).expanduser()) if key_raw not in (None, "") else None
        if not password and not key_path:
            raise SftpUploadError(
                "Set sftp_password and/or sftp_private_key_path for SFTP authentication."
            )

        phrase_raw = config.get("sftp_private_key_passphrase")
        passphrase = str(phrase_raw) if phrase_raw not in (None, "") else None

        return cls(
            host=host,
            port=port,
            username=username,
            remote_path=remote_path,
            password=password,
            private_key_path=key_path,
            private_key_passphrase=passphrase,
        )

    def resolve_remote_file_path(self, local_path: Path) -> str:
        """Remote path for put (dir or full path)."""
        remote = self.remote_path.replace("\\", "/").rstrip()
        if remote.endswith("/"):
            return remote + local_path.name
        return remote


def upload_file(local_path: Path, config: SftpConnectionConfig) -> None:
    """SFTP put file."""
    remote_path = config.resolve_remote_file_path(local_path)
    auth_parts = [
        label
        for label, ok in (
            ("password", bool(config.password)),
            ("private_key", bool(config.private_key_path)),
        )
        if ok
    ]
    auth_mode = "+".join(auth_parts) if auth_parts else "none"
    logger.info(
        "Begin remote file upload host=%s port=%s user=%s path=%s auth=%s bytes=%s",
        config.host,
        config.port,
        config.username,
        remote_path,
        auth_mode,
        local_path.stat().st_size if local_path.is_file() else 0,
    )
    # Piped runners (Hotglue) may stop reading stderr after this line; Paramiko/cryptography
    # still write during ssh.connect/teardown → SIGPIPE (141). Detach before any SSH I/O.
    diag("sftp", {"event": "before_stdio_detach", "remote_path": remote_path})
    detach_stdio_from_pipes()
    diag("sftp", {"event": "after_stdio_detach"})

    connect_kwargs: Dict[str, Any] = {
        "hostname": config.host,
        "port": config.port,
        "username": config.username,
        "look_for_keys": False,
        "allow_agent": False,
        "timeout": config.connect_timeout,
        "banner_timeout": config.banner_timeout,
        "auth_timeout": config.auth_timeout,
    }
    if config.password:
        connect_kwargs["password"] = config.password
    if config.private_key_path:
        connect_kwargs["key_filename"] = config.private_key_path
        if config.private_key_passphrase:
            connect_kwargs["passphrase"] = config.private_key_passphrase

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        try:
            diag("sftp", {"event": "before_ssh_connect", "host": config.host, "port": config.port})
            ssh.connect(**connect_kwargs)
            diag("sftp", {"event": "after_ssh_connect"})
        except SSHException as exc:
            diag("sftp", {"event": "ssh_connect_failed", "error": str(exc)})
            raise SftpUploadError(f"SSH handshake or transport failed: {exc}") from exc
        except OSError as exc:
            diag("sftp", {"event": "ssh_connect_oserror", "error": str(exc)})
            raise SftpUploadError(f"Network error connecting to {config.host!r}: {exc}") from exc

        try:
            diag("sftp", {"event": "before_open_sftp"})
            sftp = ssh.open_sftp()
            diag("sftp", {"event": "after_open_sftp"})
        except SSHException as exc:
            diag("sftp", {"event": "open_sftp_failed", "error": str(exc)})
            raise SftpUploadError(f"Failed to open SFTP subsystem: {exc}") from exc

        try:
            diag("sftp", {"event": "before_put", "local": str(local_path), "remote": remote_path})
            sftp.put(str(local_path), remote_path)
            diag("sftp", {"event": "after_put"})
        except (OSError, SSHException) as exc:
            diag("sftp", {"event": "put_failed", "error": str(exc)})
            raise SftpUploadError(
                f"Failed to put file to {remote_path!r}: {exc}"
            ) from exc
        finally:
            diag("sftp", {"event": "before_sftp_close"})
            sftp.close()
            diag("sftp", {"event": "after_sftp_close"})

        logger.info("Remote file upload complete path=%s", remote_path)
    finally:
        diag("sftp", {"event": "before_ssh_close"})
        ssh.close()
        diag("sftp", {"event": "after_ssh_close"})
