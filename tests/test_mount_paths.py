# tests/test_mount_paths.py
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


def test_filetime_conversion():
    from meret.vfs.backends.mount import _filetime

    assert _filetime(0) == 0  # unknown stays unknown, not 1601
    assert _filetime(1.0) == 116444736000000000 + 10_000_000
    assert _filetime(1_700_000_000.5) == 116444736000000000 + 17_000_000_005_000_000
