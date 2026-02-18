from __future__ import annotations

from pathlib import Path

from playlist.export import write_m3u


def test_write_m3u_writes_relative_paths_skips_missing_and_overwrites(tmp_path, monkeypatch) -> None:
    music_root = tmp_path / "Music"
    monkeypatch.setenv("RETREIVR_MUSIC_ROOT", str(music_root))

    track_one = music_root / "Artist A" / "Album A (2020)" / "Disc 1" / "01 - Song One.mp3"
    track_two = music_root / "Artist A" / "Album A (2020)" / "Disc 1" / "02 - Song Two.mp3"
    missing = music_root / "Artist A" / "Album A (2020)" / "Disc 1" / "03 - Missing.mp3"
    track_one.parent.mkdir(parents=True, exist_ok=True)
    track_one.write_bytes(b"a")
    track_two.write_bytes(b"b")

    playlist_root = tmp_path / "playlists"

    first_path = write_m3u(
        playlist_root=playlist_root,
        playlist_name="My: Playlist",
        track_paths=[track_one, missing, track_two],
    )

    assert first_path.exists() is True
    assert first_path.name == "My Playlist.m3u"
    first_content = first_path.read_text(encoding="utf-8")
    assert "#EXTM3U" in first_content
    assert "Artist A/Album A (2020)/Disc 1/01 - Song One.mp3" in first_content
    assert "Artist A/Album A (2020)/Disc 1/02 - Song Two.mp3" in first_content
    assert "03 - Missing.mp3" not in first_content

    second_path = write_m3u(
        playlist_root=playlist_root,
        playlist_name="My: Playlist",
        track_paths=[track_two],
    )

    assert second_path == first_path
    second_content = second_path.read_text(encoding="utf-8")
    assert "01 - Song One.mp3" not in second_content
    assert "02 - Song Two.mp3" in second_content
