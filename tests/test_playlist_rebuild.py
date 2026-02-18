from __future__ import annotations

from pathlib import Path

from playlist.rebuild import rebuild_playlist_from_tracks


def test_rebuild_playlist_from_tracks_writes_existing_relative_entries(tmp_path) -> None:
    music_root = tmp_path / "Music"
    playlist_root = tmp_path / "Playlists"

    track_one = music_root / "Artist" / "Album (2020)" / "Disc 1" / "01 - Song One.mp3"
    track_two = music_root / "Artist" / "Album (2020)" / "Disc 1" / "02 - Song Two.mp3"
    missing = music_root / "Artist" / "Album (2020)" / "Disc 1" / "03 - Missing.mp3"
    track_one.parent.mkdir(parents=True, exist_ok=True)
    track_one.write_bytes(b"a")
    track_two.write_bytes(b"b")

    result_path = rebuild_playlist_from_tracks(
        playlist_name="My Playlist",
        playlist_root=playlist_root,
        music_root=music_root,
        track_file_paths=[str(track_one), str(missing), str(track_two)],
    )

    assert result_path.exists() is True
    content = result_path.read_text(encoding="utf-8")
    assert "#EXTM3U" in content
    assert "Artist/Album (2020)/Disc 1/01 - Song One.mp3" in content
    assert "Artist/Album (2020)/Disc 1/02 - Song Two.mp3" in content
    assert "03 - Missing.mp3" not in content
