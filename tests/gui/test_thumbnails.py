"""Thumbnails: the cache key, the ffmpeg call, and every failure reason.

Nothing here touches the user's real cache directory: cache_dir() is patched, and
subprocess/shutil are patched per test.
"""

import io
from types import SimpleNamespace

import pytest
from PIL import Image

pytest.importorskip("PySide6")


@pytest.fixture(autouse=True)
def fresh_ffmpeg_probe():
    """ffmpeg_path is lru_cached, so a patched shutil.which needs a re-probe."""
    from meret.gui import thumbnails

    thumbnails.ffmpeg_path.cache_clear()
    yield
    thumbnails.ffmpeg_path.cache_clear()


@pytest.fixture
def cache(tmp_path, monkeypatch, qapp):
    """A cache writing into tmp_path, never into the user's cache directory."""
    from meret.gui import thumbnails

    directory = tmp_path / "thumbs"
    directory.mkdir()
    monkeypatch.setattr(thumbnails, "cache_dir", lambda: directory)
    return thumbnails.ThumbnailCache(size=64, scope="meret.db")


def _photo(tmp_path, name="photo.png", size=(40, 30), colour="red"):
    path = tmp_path / name
    Image.new("RGB", size, colour).save(path)
    return path


def _png_bytes(size=(40, 30), colour="blue"):
    buffer = io.BytesIO()
    Image.new("RGB", size, colour).save(buffer, "PNG")
    return buffer.getvalue()


def _no_ffmpeg(monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: None)


def _fake_ffmpeg(monkeypatch, outputs):
    """Pretend ffmpeg is installed and answers with `outputs`, in order."""
    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/ffmpeg")
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(stdout=outputs.pop(0) if outputs else b"")

    monkeypatch.setattr("subprocess.run", run)
    return calls


# ---------- the cache key ----------


def test_the_key_is_stable_for_the_same_input(cache):
    assert cache._target(1, 100.0, 64) == cache._target(1, 100.0, 64)


@pytest.mark.parametrize(
    "other",
    [
        (2, 100.0, 64),   # another file
        (1, 100.5, 64),   # edited: a stale thumbnail must not survive
        (1, 100.0, 128),  # another size
    ],
)
def test_the_key_changes_with_every_input(cache, other):
    assert cache._target(*other) != cache._target(1, 100.0, 64)


def test_two_databases_do_not_share_a_key(cache, monkeypatch):
    """A file id is unique only inside one database."""
    from meret.gui import thumbnails

    monkeypatch.setattr(thumbnails, "cache_dir", lambda: cache._dir)
    other = thumbnails.ThumbnailCache(size=64, scope="elsewhere.db")

    assert other._target(1, 100.0, 64) != cache._target(1, 100.0, 64)


def test_the_size_is_capped_at_the_maximum(tmp_path, monkeypatch, qapp):
    from meret.gui import thumbnails

    monkeypatch.setattr(thumbnails, "cache_dir", lambda: tmp_path)
    assert thumbnails.ThumbnailCache(size=thumbnails.MAX_SIZE * 4).size == thumbnails.MAX_SIZE


def test_a_huge_request_is_capped_too(tmp_path, cache):
    from meret.gui.thumbnails import MAX_SIZE

    photo = _photo(tmp_path, size=(2000, 10))
    cache.load_result(photo, file_id=1, mtime=1.0, size=MAX_SIZE * 3)

    assert cache._target(1, 1.0, MAX_SIZE).is_file()


def test_the_default_size_comes_from_the_config(tmp_path, monkeypatch, qapp):
    from meret.core.config import get_config
    from meret.gui import thumbnails

    monkeypatch.setattr(thumbnails, "cache_dir", lambda: tmp_path)
    monkeypatch.setattr(get_config(), "get", lambda key, default=None: 96)

    assert thumbnails.ThumbnailCache().size == 96


def test_cache_dir_is_created(tmp_path, monkeypatch):
    from meret.gui import thumbnails

    monkeypatch.setattr(
        thumbnails.platformdirs, "user_cache_dir", lambda name: str(tmp_path / "c")
    )

    path = thumbnails.cache_dir()

    assert path.is_dir()
    assert path.name == "thumbs"


# ---------- loading ----------


def test_an_image_is_rendered_and_cached(tmp_path, cache):
    photo = _photo(tmp_path)
    target = cache._target(1, 5.0, 64)

    result = cache.load_result(photo, file_id=1, mtime=5.0)

    assert result.ok
    assert target.is_file()
    assert result.image.width() <= 64


def test_a_second_load_comes_from_the_disk(tmp_path, cache, monkeypatch):
    photo = _photo(tmp_path)
    cache.load_result(photo, file_id=1, mtime=5.0)

    renders = []
    monkeypatch.setattr(cache, "_render", lambda *a, **k: renders.append(1) or "")

    again = cache.load_result(photo, file_id=1, mtime=5.0)

    assert again.ok
    assert renders == []


def test_a_truncated_thumbnail_is_rebuilt(tmp_path, cache):
    """A crash between save() and replace() leaves a PNG QImage cannot read, and
    without this it would stay unreadable forever."""
    photo = _photo(tmp_path)
    cache._target(1, 5.0, 64).write_bytes(b"")

    assert cache.load_result(photo, file_id=1, mtime=5.0).ok


def test_the_write_leaves_no_part_file(tmp_path, cache):
    photo = _photo(tmp_path)

    cache.load_result(photo, file_id=1, mtime=5.0)

    assert list(cache._dir.glob("*.part")) == []


def test_load_is_the_image_only(tmp_path, cache):
    photo = _photo(tmp_path)
    image = cache.load(photo, file_id=1, mtime=5.0)
    assert image is not None and not image.isNull()


def test_a_missing_source_is_unreadable(tmp_path, cache):
    from meret.gui.thumbnails import Reason

    result = cache.load_result(tmp_path / "gone.png", file_id=1)

    assert result.image is None
    assert result.reason == Reason.UNREADABLE


def test_a_file_pillow_cannot_open_has_no_decoder(tmp_path, cache):
    """Audio without an embedded cover: Pillow has nothing to open."""
    from meret.gui.thumbnails import Reason

    junk = tmp_path / "song.flac"
    junk.write_bytes(b"definitely not an image")

    assert cache.load_result(junk, file_id=1).reason == Reason.NO_DECODER


def test_the_exif_orientation_is_applied(tmp_path, cache):
    """A phone photo tagged 'rotate' must not come out sideways."""
    photo = tmp_path / "rotated.jpg"
    image = Image.new("RGB", (40, 20), "green")
    exif = image.getexif()
    exif[274] = 6  # Orientation: the 0th row is the visual right
    image.save(photo, exif=exif)

    result = cache.load_result(photo, file_id=1, mtime=1.0)

    assert result.ok
    assert result.image.height() > result.image.width()


def test_thumbnail_ok_needs_an_image():
    from meret.gui.thumbnails import Reason, Thumbnail

    assert not Thumbnail().ok
    assert not Thumbnail(None, Reason.NO_TOOL).ok


# ---------- video ----------


def test_a_video_without_ffmpeg_says_so(tmp_path, cache, monkeypatch):
    from meret.gui.thumbnails import Reason

    _no_ffmpeg(monkeypatch)
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"whatever")

    result = cache.load_result(video, file_id=1, mtime=1.0)

    assert result.image is None
    assert result.reason == Reason.NO_TOOL


def test_a_video_frame_is_decoded_from_the_pipe(tmp_path, cache, monkeypatch):
    calls = _fake_ffmpeg(monkeypatch, [_png_bytes()])
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"x")

    result = cache.load_result(video, file_id=1, mtime=1.0)

    assert result.ok
    assert result.image.width() <= 64
    assert len(calls) == 1


def test_the_seek_comes_before_the_input(tmp_path, cache, monkeypatch):
    """-ss before -i is a fast keyframe seek; after -i it decodes the whole head."""
    calls = _fake_ffmpeg(monkeypatch, [_png_bytes()])
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"x")

    cache.load_result(video, file_id=1, mtime=1.0)

    command = calls[0]
    assert command.index("-ss") < command.index("-i")
    assert "-nostdin" in command
    assert command[-1] == "-"  # the frame goes to stdout, not to a file


def test_a_short_clip_falls_back_to_the_first_frame(tmp_path, cache, monkeypatch):
    """A clip shorter than a second has nothing at the 1s seek point."""
    calls = _fake_ffmpeg(monkeypatch, [b"", _png_bytes()])
    video = tmp_path / "short.mp4"
    video.write_bytes(b"x")

    result = cache.load_result(video, file_id=1, mtime=1.0)

    assert result.ok
    assert [command[command.index("-ss") + 1] for command in calls] == ["1", "0"]


def test_ffmpeg_that_gives_nothing_is_a_no_frame(tmp_path, cache, monkeypatch):
    from meret.gui.thumbnails import Reason

    _fake_ffmpeg(monkeypatch, [b"", b""])
    video = tmp_path / "broken.mp4"
    video.write_bytes(b"x")

    assert cache.load_result(video, file_id=1, mtime=1.0).reason == Reason.NO_FRAME


def test_ffmpeg_that_cannot_start_is_a_no_frame(tmp_path, cache, monkeypatch):
    from meret.gui.thumbnails import Reason

    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/ffmpeg")

    def boom(command, **kwargs):
        raise OSError("cannot execute")

    monkeypatch.setattr("subprocess.run", boom)
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"x")

    assert cache.load_result(video, file_id=1, mtime=1.0).reason == Reason.NO_FRAME


def test_ffmpeg_output_that_is_not_a_png_falls_through(tmp_path, cache, monkeypatch):
    from meret.gui.thumbnails import Reason

    _fake_ffmpeg(monkeypatch, [b"garbage", b"garbage"])
    video = tmp_path / "clip.mp4"
    video.write_bytes(b"x")

    assert cache.load_result(video, file_id=1, mtime=1.0).reason == Reason.NO_FRAME