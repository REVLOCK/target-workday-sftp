"""Target errors."""


class TargetWorkdaySftpError(Exception):
    """Base error."""


class TransformError(TargetWorkdaySftpError):
    """Transform or CSV I/O failed."""


class SftpUploadError(TargetWorkdaySftpError):
    """Remote upload failed."""
