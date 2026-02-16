from __future__ import annotations

from scheduler.jobs.spotify_playlist_watch import SPOTIFY_LIKED_SONGS_PLAYLIST_ID
from playlist.rebuild import rebuild_playlist_from_tracks


def test_liked_songs_virtual_playlist_rebuild_creates_m3u(tmp_path) -> None:
    assert SPOTIFY_LIKED_SONGS_PLAYLIST_ID == "__spotify_liked_songs__"

    music_root = tmp_path / "Music"
    playlist_root = tmp_path / "Playlists"
    track_one = music_root / "Artist A" / "Album A (2020)" / "Disc 1" / "01 - Song One.mp3"
    track_two = music_root / "Artist B" / "Album B (2021)" / "Disc 1" / "02 - Song Two.mp3"
    track_one.parent.mkdir(parents=True, exist_ok=True)
    track_two.parent.mkdir(parents=True, exist_ok=True)
    track_one.write_bytes(b"a")
    track_two.write_bytes(b"b")

    result_path = rebuild_playlist_from_tracks(
        playlist_name="Spotify - Liked Songs",
        playlist_root=playlist_root,
        music_root=music_root,
        track_file_paths=[str(track_one), str(track_two)],
    )

    assert result_path.exists() is True
    assert result_path.name == "Spotify - Liked Songs.m3u"
    content = result_path.read_text(encoding="utf-8")
    assert "Artist A/Album A (2020)/Disc 1/01 - Song One.mp3" in content
    assert "Artist B/Album B (2021)/Disc 1/02 - Song Two.mp3" in content
