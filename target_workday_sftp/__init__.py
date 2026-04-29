"""Singer target: transform journal CSV, upload via SFTP."""

from __future__ import annotations

import signal

# Before any other imports: piped runners often close stdout early; logging/crypto
# during import or shutdown can otherwise terminate the process with SIGPIPE (141).
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)

import json
import os
from pathlib import Path
from typing import Any, Dict

import singer

from target_workday_sftp.const import (
    REQUIRED_CONFIG_KEYS,
    REQUIRED_FLATTENED_CONFIG_KEYS,
)
from target_workday_sftp.exceptions import SftpUploadError
from target_workday_sftp.sftp_upload import (
    SftpConnectionConfig,
    upload_file,
)
from target_workday_sftp.transform import transform_journal_summary

logger = singer.get_logger()


def _parse_config_fields_payload(raw: Any) -> Dict[str, Any]:
    if raw is None or raw == "":
        return {}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SftpUploadError(f"config_fields is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise SftpUploadError("config_fields JSON string must decode to an object")
        return parsed
    if isinstance(raw, dict):
        return dict(raw)
    raise SftpUploadError(
        f"config_fields must be a JSON object or JSON string, got {type(raw).__name__}"
    )


def _merge_config_fields_into_out(out: Dict[str, Any], raw: Any) -> None:
    """Merge non-empty config_fields values into out."""
    for name, val in _parse_config_fields_payload(raw).items():
        if val is None:
            continue
        vs = str(val).strip()
        if vs == "":
            continue
        out[str(name)] = vs


def flatten_config(config: Any) -> Dict[str, Any]:
    """Flatten config: ``config_fields`` merged first, then top-level keys (top-level wins)."""
    if not isinstance(config, dict):
        raise SftpUploadError("config must be a JSON object")

    out: Dict[str, Any] = {}

    _merge_config_fields_into_out(out, config.get("config_fields"))

    for key, value in config.items():
        if key in ("config_fields", "custom_fields"):
            continue
        out[key] = value

    return out


def normalize_target_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Alias for flatten_config."""
    return flatten_config(config)


def _cleanup_transform_output(out_path: Path) -> None:
    """Delete local output CSV (no logging: stderr may already be closed by the parent)."""
    try:
        if out_path.is_file():
            out_path.unlink()
    except OSError:
        pass


def require_flattened_config(config: Dict[str, Any]) -> None:
    """Require non-empty flattened keys."""
    missing: list[str] = []
    for key in REQUIRED_FLATTENED_CONFIG_KEYS:
        val = config.get(key)
        if val is None:
            missing.append(key)
        elif isinstance(val, str) and not val.strip():
            missing.append(key)
    if missing:
        raise SftpUploadError(
            "Missing or empty required config (after flattening): "
            + ", ".join(sorted(missing))
        )


@singer.utils.handle_top_exception(logger)
def main() -> None:
    """CLI entry: transform then SFTP."""
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = flatten_config(args.config)
    require_flattened_config(config)
    logger.info("Starting journal transform and SFTP upload.")
    out_path = transform_journal_summary(config)

    try:
        sftp_cfg = SftpConnectionConfig.from_target_config(config)
        upload_file(out_path, sftp_cfg)
        try:
            logger.info(
                "Finished successfully; remote received file: %s", out_path.name
            )
        except Exception:
            # Piped parents may close stderr as soon as upload completes; still exit 0.
            pass
    finally:
        _cleanup_transform_output(out_path)

    # Piped runners often close the read side as soon as they see the last log line; any
    # further stderr write (logging shutdown, cleanup logs) can SIGPIPE the process (141).
    if hasattr(signal, "SIGPIPE"):
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)
    os._exit(0)


__all__ = [
    "flatten_config",
    "main",
    "normalize_target_config",
    "require_flattened_config",
]
