from __future__ import annotations

from pathlib import Path

from metadata.tagging_service import tag_file
from metadata.types import MusicMetadata


def _metadata(*, artwork: bytes | None = b"img", lyrics: str | None = "line one") -> MusicMetadata:
    return MusicMetadata(
        title="Test Title",
        artist="Test Artist",
        album="Test Album",
        album_artist="Test Artist",
        track_num=1,
        disc_num=1,
        date="2026-02-16",
        genre="Rock",
        isrc="USABC1234567",
        mbid="mbid-123",
        artwork=artwork,
        lyrics=lyrics,
    )


def test_tag_file_writes_expected_id3_frames(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "track.mp3"
    path.write_bytes(b"")

    import metadata.tagger as tagging

    class FakeAudio:
        def __init__(self) -> None:
            self.frames = []
            self.saved = None

        def add(self, frame) -> None:
            self.frames.append(frame)

        def getall(self, frame_id: str):
            return [frame for frame in self.frames if frame.name == frame_id]

        def delall(self, frame_id: str) -> None:
            self.frames = [frame for frame in self.frames if frame.name != frame_id]

        def save(self, save_path: str) -> None:
            self.saved = save_path

    class FakeFrame:
        def __init__(self, name: str, **kwargs) -> None:
            self.name = name
            for key, value in kwargs.items():
                setattr(self, key, value)

    def _factory(name: str):
        def _ctor(**kwargs):
            return FakeFrame(name, **kwargs)

        return _ctor

    audio = FakeAudio()

    monkeypatch.setattr(tagging, "ID3", lambda: audio)
    monkeypatch.setattr(tagging, "TIT2", _factory("TIT2"))
    monkeypatch.setattr(tagging, "TPE1", _factory("TPE1"))
    monkeypatch.setattr(tagging, "TALB", _factory("TALB"))
    monkeypatch.setattr(tagging, "TPE2", _factory("TPE2"))
    monkeypatch.setattr(tagging, "TRCK", _factory("TRCK"))
    monkeypatch.setattr(tagging, "TDRC", _factory("TDRC"))
    monkeypatch.setattr(tagging, "TCON", _factory("TCON"))
    monkeypatch.setattr(tagging, "TXXX", _factory("TXXX"))
    monkeypatch.setattr(tagging, "USLT", _factory("USLT"))
    monkeypatch.setattr(tagging, "APIC", _factory("APIC"))

    tag_file(str(path), _metadata())

    by_name = {frame.name: frame for frame in audio.frames}
    assert by_name["TIT2"].text[0] == "Test Title"
    assert by_name["TPE1"].text[0] == "Test Artist"
    assert by_name["TALB"].text[0] == "Test Album"
    assert by_name["TRCK"].text[0] == "1"
    txxx_descs = {frame.desc for frame in audio.frames if frame.name == "TXXX"}
    assert "SOURCE" in txxx_descs
    assert "MBID" in txxx_descs
    assert by_name["USLT"].text == "line one"
    assert by_name["APIC"].data == b"img"
    assert audio.saved == str(path)


def test_tag_file_lyrics_and_artwork_fail_non_fatally(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "track.mp3"
    path.write_bytes(b"")

    import metadata.tagger as tagging

    class FakeAudio:
        def __init__(self) -> None:
            self.frames = []
            self.saved = False

        def add(self, frame) -> None:
            self.frames.append(frame)

        def getall(self, frame_id: str):
            return [frame for frame in self.frames if frame.name == frame_id]

        def delall(self, frame_id: str) -> None:
            self.frames = [frame for frame in self.frames if frame.name != frame_id]

        def save(self, save_path: str) -> None:
            self.saved = True

    class FakeFrame:
        def __init__(self, name: str, **kwargs) -> None:
            self.name = name
            for key, value in kwargs.items():
                setattr(self, key, value)

    def _factory(name: str):
        def _ctor(**kwargs):
            return FakeFrame(name, **kwargs)

        return _ctor

    def _raise(*_args, **_kwargs):
        raise RuntimeError("frame failure")

    audio = FakeAudio()
    monkeypatch.setattr(tagging, "ID3", lambda: audio)
    monkeypatch.setattr(tagging, "TIT2", _factory("TIT2"))
    monkeypatch.setattr(tagging, "TPE1", _factory("TPE1"))
    monkeypatch.setattr(tagging, "TALB", _factory("TALB"))
    monkeypatch.setattr(tagging, "TPE2", _factory("TPE2"))
    monkeypatch.setattr(tagging, "TRCK", _factory("TRCK"))
    monkeypatch.setattr(tagging, "TDRC", _factory("TDRC"))
    monkeypatch.setattr(tagging, "TCON", _factory("TCON"))
    monkeypatch.setattr(tagging, "TXXX", _factory("TXXX"))
    monkeypatch.setattr(tagging, "USLT", _raise)
    monkeypatch.setattr(tagging, "APIC", _raise)

    # Should not raise even when lyrics/artwork frame construction fails.
    tag_file(str(path), _metadata())

    assert audio.saved is True
    assert any(frame.name == "TIT2" for frame in audio.frames)


def test_apply_tags_writes_track_and_disc_totals_for_id3(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "track.mp3"
    path.write_bytes(b"")

    import metadata.tagger as tagging

    class FakeAudio:
        def __init__(self) -> None:
            self.frames = []
            self.saved = None

        def add(self, frame) -> None:
            self.frames.append(frame)

        def getall(self, frame_id: str):
            return [frame for frame in self.frames if frame.name == frame_id]

        def delall(self, frame_id: str) -> None:
            self.frames = [frame for frame in self.frames if frame.name != frame_id]

        def save(self, save_path: str) -> None:
            self.saved = save_path

    class FakeFrame:
        def __init__(self, name: str, **kwargs) -> None:
            self.name = name
            for key, value in kwargs.items():
                setattr(self, key, value)

    def _factory(name: str):
        def _ctor(**kwargs):
            return FakeFrame(name, **kwargs)

        return _ctor

    audio = FakeAudio()
    monkeypatch.setattr(tagging, "ID3", lambda *_args, **_kwargs: audio)
    monkeypatch.setattr(tagging, "TIT2", _factory("TIT2"))
    monkeypatch.setattr(tagging, "TPE1", _factory("TPE1"))
    monkeypatch.setattr(tagging, "TALB", _factory("TALB"))
    monkeypatch.setattr(tagging, "TPE2", _factory("TPE2"))
    monkeypatch.setattr(tagging, "TRCK", _factory("TRCK"))
    monkeypatch.setattr(tagging, "TPOS", _factory("TPOS"))
    monkeypatch.setattr(tagging, "TDRC", _factory("TDRC"))
    monkeypatch.setattr(tagging, "TCON", _factory("TCON"))
    monkeypatch.setattr(tagging, "TXXX", _factory("TXXX"))
    monkeypatch.setattr(tagging, "USLT", _factory("USLT"))
    monkeypatch.setattr(tagging, "APIC", _factory("APIC"))

    tagging.apply_tags(
        str(path),
        {
            "artist": "Test Artist",
            "album": "Test Album",
            "title": "Test Title",
            "track_number": 1,
            "track_total": 10,
            "disc_number": 1,
            "disc_total": 2,
        },
        artwork=None,
        allow_overwrite=True,
    )

    by_name = {frame.name: frame for frame in audio.frames}
    assert by_name["TRCK"].text[0] == "1/10"
    assert by_name["TPOS"].text[0] == "1/2"


def test_apply_tags_writes_track_and_disc_totals_for_mp4(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "track.m4a"
    path.write_bytes(b"")

    import metadata.tagger as tagging

    instances = []

    class FakeMP4:
        def __init__(self, _file_path: str) -> None:
            self.tags = {}
            self.saved = False
            instances.append(self)

        def save(self) -> None:
            self.saved = True

    monkeypatch.setattr(tagging, "MP4", FakeMP4)

    tagging.apply_tags(
        str(path),
        {
            "artist": "Test Artist",
            "album": "Test Album",
            "title": "Test Title",
            "track_number": "1/10",
            "disc_number": "1/2",
        },
        artwork=None,
        allow_overwrite=True,
    )
    audio = instances[0]
    assert audio.tags.get("trkn", [(0, 0)])[0] == (1, 10)
    assert audio.tags.get("disk", [(0, 0)])[0] == (1, 2)
