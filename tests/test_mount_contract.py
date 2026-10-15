# tests/test_mount_contract.py
"""winfspy contract: open/create return the context, not (status, context)."""

import pytest

from meret.vfs.backends.mount import _norm_windows_path


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("\\seaside\\file.png", "/seaside/file.png"),
        ("/seaside/file.png", "/seaside/file.png"),
        ("file.png", "/file.png"),
        ("\\", "/"),
        ("", "/"),
    ],
)
def test_norm_windows_path(raw, expected):
    assert _norm_windows_path(raw) == expected