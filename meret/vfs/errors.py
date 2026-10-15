"""Virtual FS errors."""


class VfsError(Exception):
    """Base virtual FS error."""


class NotFound(VfsError):
    """Node not found."""


class NotADirectory(VfsError):
    """Expected a directory, got a file."""


class IsADirectory(VfsError):
    """Expected a file, got a directory."""
