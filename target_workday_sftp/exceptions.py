"""Errors for this target."""

from __future__ import annotations


class TargetWorkdaySftpError(Exception):
    """Base target error."""

    def __init__(self, msg: str, response: object = None) -> None:
        super().__init__(msg)
        self.message = msg
        self.response = response

    def __str__(self) -> str:
        return repr(self.message)


class InputError(TargetWorkdaySftpError):
    """Missing or unusable input file or columns (same role as Oracle ``InputError``)."""


class ValidationError(TargetWorkdaySftpError):
    """Row or field validation failed (same role as Oracle ``ValidationError``)."""


class TransformError(TargetWorkdaySftpError):
    """Transform failed."""


class SftpUploadError(TargetWorkdaySftpError):
    """SFTP upload failed."""
