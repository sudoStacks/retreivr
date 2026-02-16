from __future__ import annotations

from pathlib import Path

from metadata.music_files import build_music_filename, tag_music_file
from metadata.music_metadata import MusicMetadata


def _sample_metadata(*, artwork: str | None = None, title: str = 'Song:/Name*?') -> MusicMetadata:
    return MusicMetadata(
        title=title,
        artist="Artist One",
        album="Album One",
        album_artist="Artist One",
        track_num=1,
        disc_num=1,
        date="2026-02-16",
        genre="Rock",
        isrc="USABC1234567",
        mbid="mbid-123",
        artwork=artwork,
        lyrics="line one\nline two",
    )


def test_build_music_filename_sanitizes_and_zero_pads() -> None:
    filename = build_music_filename(_sample_metadata())
    assert filename == "01 - Song--Name--.mp3"


def test_tag_music_file_mp3_writes_id3v24_and_artwork(tmp_path: Path) -> None:
    mp3_path = tmp_path / "track.mp3"
    mp3_path.write_bytes(b"")
    art_path = tmp_path / "cover.jpg"
    art_path.write_bytes(b"\xff\xd8\xff\xe0" + b"jpeg-bytes")

    import metadata.music_files as music_files

    created_audio = {"obj": None}

    class FakeAudio:
        def __init__(self) -> None:
            self.frames = []
            self.saved = None

        def add(self, frame) -> None:
            self.frames.append(frame)

        def save(self, path: str, v2_version: int) -> None:
            self.saved = (path, v2_version)

    class FakeFrame:
        def __init__(self, frame_name: str, **kwargs) -> None:
            self.frame_name = frame_name
            for key, value in kwargs.items():
                setattr(self, key, value)

    def _factory(frame_name: str):
        def _ctor(**kwargs):
            return FakeFrame(frame_name, **kwargs)

        return _ctor

    def fake_id3_ctor():
        audio = FakeAudio()
        created_audio["obj"] = audio
        return audio

    # Patch mutagen constructors with fakes so tests do not require local mutagen install.
    music_files.ID3 = fake_id3_ctor
    music_files.TIT2 = _factory("TIT2")
    music_files.TPE1 = _factory("TPE1")
    music_files.TALB = _factory("TALB")
    music_files.TPE2 = _factory("TPE2")
    music_files.TRCK = _factory("TRCK")
    music_files.TPOS = _factory("TPOS")
    music_files.TDRC = _factory("TDRC")
    music_files.TCON = _factory("TCON")
    music_files.TSRC = _factory("TSRC")
    music_files.TXXX = _factory("TXXX")
    music_files.USLT = _factory("USLT")
    music_files.APIC = _factory("APIC")

    metadata = _sample_metadata(artwork=str(art_path), title="Test Title")
    tag_music_file(str(mp3_path), metadata)

    audio = created_audio["obj"]
    assert audio is not None
    assert audio.saved == (str(mp3_path), 4)

    frames_by_name = {frame.frame_name: frame for frame in audio.frames}
    assert frames_by_name["TIT2"].text[0] == "Test Title"
    assert frames_by_name["TPE1"].text[0] == "Artist One"
    assert frames_by_name["TRCK"].text[0] == "1"
    assert frames_by_name["TSRC"].text[0] == "USABC1234567"
    assert frames_by_name["TXXX"].desc == "MBID"
    assert frames_by_name["TXXX"].text[0] == "mbid-123"
    assert frames_by_name["USLT"].text == "line one line two"
    assert frames_by_name["APIC"].mime == "image/jpeg"


def test_tag_music_file_flac_writes_vorbis_and_artwork(monkeypatch, tmp_path: Path) -> None:
    flac_path = tmp_path / "track.flac"
    flac_path.write_bytes(b"fake")
    art_path = tmp_path / "cover.png"
    art_path.write_bytes(b"\x89PNG\r\n\x1a\npng-bytes")

    saved = {"called": False}

    class FakeFlac(dict):
        def clear_pictures(self) -> None:
            self["cleared"] = ["yes"]

        def add_picture(self, picture) -> None:
            self["picture_mime"] = [picture.mime]
            self["picture_type"] = [str(picture.type)]

        def save(self) -> None:
            saved["called"] = True

    fake_flac = FakeFlac()

    def fake_flac_ctor(path: str):
        assert path == str(flac_path)
        return fake_flac

    class FakePicture:
        def __init__(self) -> None:
            self.data = b""
            self.type = 0
            self.mime = ""
            self.desc = ""

    monkeypatch.setattr("metadata.music_files.FLAC", fake_flac_ctor)
    monkeypatch.setattr("metadata.music_files.Picture", FakePicture)

    metadata = _sample_metadata(artwork=str(art_path), title="FLAC Title")
    tag_music_file(str(flac_path), metadata)

    assert fake_flac["title"] == ["FLAC Title"]
    assert fake_flac["artist"] == ["Artist One"]
    assert fake_flac["tracknumber"] == ["1"]
    assert fake_flac["musicbrainz_trackid"] == ["mbid-123"]
    assert fake_flac["picture_mime"] == ["image/png"]
    assert fake_flac["picture_type"] == ["3"]
    assert saved["called"] is True
