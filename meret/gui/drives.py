"""The drive letters, read from a bitmask so that no drive is ever opened."""

import ctypes
import string
import sys

#: Bound once, at import, on the main thread. ctypes resolves `windll.kernel32.X`
#: lazily on first attribute access, and a probe used to reach that loader from
#: several threads at once - which is an access violation, not an exception.
if sys.platform == "win32":
    _GetLogicalDrives = ctypes.windll.kernel32.GetLogicalDrives
    _GetLogicalDrives.argtypes = []
    _GetLogicalDrives.restype = ctypes.c_uint32
else:
    _GetLogicalDrives = None


def drive_roots() -> list[str]:
    """Every drive letter the system reports, as "C:/" - and nothing else.

    Not `QDir.drives()`: that builds a `QFileInfo` per drive, and on a disconnected
    mapped drive the volume query faults inside the network provider - the same
    native access violation that crashed the probe thread, and then the window's own
    constructor once the probes were gone. No `except` clause reaches a fault like
    that, so the only fix is to ask a question that does not touch a drive.

    `GetLogicalDrives()` is a bitmask read from the mount manager: no drive is
    opened, no volume is queried, no label is asked for. A dead drive therefore costs
    exactly nothing until someone selects it - and that is a deliberate click.
    """
    if _GetLogicalDrives is None:
        return ["/"]
    mask = _GetLogicalDrives()
    return [
        f"{letter}:/" for index, letter in enumerate(string.ascii_uppercase) if mask & (1 << index)
    ]
