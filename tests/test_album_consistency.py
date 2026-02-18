from __future__ import annotations

from metadata.normalize import normalize_music_metadata
from metadata.types import MusicMetadata


def test_album_download_metadata_normalization_consistency() -> None:
    track_three = MusicMetadata(
        title="Song Three - Topic",
        artist="Main Artist",
        album="Album Name",
        album_artist="Main Artist",
        track_num=3,
        disc_num=1,
        date="2024/07/11",
        genre="Pop",
        isrc="USAAA1111113",
    )
    track_three.album_artist = ""

    tracks = [
        MusicMetadata(
            title="Song One (Official Audio)",
            artist="Main Artist",
            album="Album Name",
            album_artist="Main Artist",
            track_num=1,
            disc_num=1,
            date="2024-07",
            genre="Pop",
            isrc="USAAA1111111",
        ),
        MusicMetadata(
            title="Song Two [HD]",
            artist="Main Artist",
            album="Album Name",
            album_artist="Main Artist, Guest Artist",
            track_num=2,
            disc_num=1,
            date="2024",
            genre="Pop",
            isrc="USAAA1111112",
        ),
        track_three,
    ]

    normalized = [normalize_music_metadata(track) for track in tracks]

    assert {track.album_artist for track in normalized} == {"Main Artist"}
    assert [track.title for track in normalized] == ["Song One", "Song Two", "Song Three"]
    assert [track.date for track in normalized] == ["2024", "2024", "2024-07-11"]
    assert [track.track_num for track in normalized] == [1, 2, 3]
