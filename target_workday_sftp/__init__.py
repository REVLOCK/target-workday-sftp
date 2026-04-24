"""Singer entry: transform journal CSV, upload via SFTP."""

from __future__ import annotations

import shutil
from typing import Any, Dict

import singer

from target_workday_sftp.const import REQUIRED_CONFIG_KEYS
from target_workday_sftp.sftp_upload import (
    SftpConnectionConfig,
    parse_target_args,
    upload_file,
    validate_sftp_credentials,
)
from target_workday_sftp.transform import transform_journal_summary

logger = singer.get_logger()


@singer.utils.handle_top_exception(logger)
def main() -> None:
    """Parse config, run transform, upload output file."""
    args = parse_target_args(REQUIRED_CONFIG_KEYS)
    config: Dict[str, Any] = dict(args.config)

    validate_sftp_credentials(config)

    logger.info("Starting journal transform and SFTP upload.")
    out_path, temp_root = transform_journal_summary(config)
    try:
        sftp_cfg = SftpConnectionConfig.from_target_config(config)
        upload_file(out_path, sftp_cfg)
    finally:
        if temp_root is not None and temp_root.exists():
            shutil.rmtree(temp_root, ignore_errors=True)
            logger.debug("Removed temporary output directory: %s", temp_root)

    logger.info("Finished successfully; uploaded file name: %s", out_path.name)


__all__ = ["main"]
