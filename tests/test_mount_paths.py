# tests/test_mount_paths.py
import pytest

from meret.vfs.backends.mount import _norm_windows_path


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("\\пляж\\file.png", "/пляж/file.png"),
        ("/пляж/file.png", "/пляж/file.png"),
        ("file.png", "/file.png"),
        ("\\", "/"),
        ("", "/"),
    ],
)
def test_norm_windows_path(raw, expected):
    assert _norm_windows_path(raw) == expected