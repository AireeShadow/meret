# tests/vfs/backends/test_fsspec.py
import pytest

fsspec = pytest.importorskip("fsspec")  # no package: tests get skipped, the run doesn't fail

from meret.core.models import FileEntry, FileType
from meret.vfs.backends.fsspec import MeretFileSystem, register


@pytest.fixture
def fs(mock_repo, tmp_path):
    real = tmp_path / "beach.png"
    real.write_bytes(b"PNGDATA")

    f = mock_repo.save_file(
        FileEntry(
            filename="beach.png",
            filepath=str(real),
            type=FileType.IMAGE,
            metadata={"size_bytes": 7},
        )
    )
    mock_repo.add_tag(f, "seaside")
    register()
    return MeretFileSystem(repo=mock_repo)


def test_ls_detail(fs):
    entries = fs.ls("/")
    assert entries[0]["name"] == "/seaside"
    assert entries[0]["type"] == "directory"


def test_ls_names_only(fs):
    assert fs.ls("/", detail=False) == ["seaside"]


def test_info_file(fs):
    info = fs.info("/seaside/beach.png")
    assert info["type"] == "file"
    assert info["size"] == 7


def test_info_directory(fs):
    assert fs.info("/seaside")["type"] == "directory"


def test_isdir(fs):
    assert fs.isdir("/seaside") is True
    assert fs.isdir("/seaside/beach.png") is False


def test_exists(fs):
    assert fs.exists("/seaside") is True
    assert fs.exists("/missing") is False


def test_cat_file(fs):
    assert fs.cat_file("/seaside/beach.png") == b"PNGDATA"


def test_open_context_manager(fs):
    with fs.open("/seaside/beach.png", "rb") as fh:
        assert fh.read() == b"PNGDATA"


def test_protocol_registered(fs):
    assert fsspec.get_filesystem_class("meret") is MeretFileSystem


def test_fsspec_open_url(mock_repo, tmp_path):
    real = tmp_path / "x.png"
    real.write_bytes(b"DATA")
    f = mock_repo.save_file(FileEntry(filename="x.png", filepath=str(real), type=FileType.IMAGE))
    mock_repo.add_tag(f, "tag")

    with fsspec.open("meret:///tag/x.png", "rb", repo=mock_repo) as fh:
        assert fh.read() == b"DATA"
