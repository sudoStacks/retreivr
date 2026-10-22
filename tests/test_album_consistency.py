from __future__ import annotations

from metadata.normalize import normalize_music_metadata
from metadata.types import MusicMetadata


def _track(**overrides) -> MusicMetadata:
    payload = {
        "title": "Song One (Official Audio)",
        "artist": "Main Artist",
        "album": "Album Name",
        "album_artist": "Main Artist",
        "track_num": 1,
        "disc_num": 1,
        "date": "2024-07",
        "genre": "Pop",
        "isrc": "USAAA1111111",
    }
    payload.update(overrides)
    return MusicMetadata(**payload)


def test_album_download_metadata_normalization_consistency() -> None:
    track_three = _track(
        title="Song Three - Topic",
        track_num=3,
        date="2024/07/11",
        isrc="USAAA1111113",
    )
    track_three.album_artist = ""

    tracks = [
        _track(),
        _track(
            title="Song Two [HD]",
            album_artist="Main Artist, Guest Artist",
            track_num=2,
            date="2024",
            isrc="USAAA1111112",
        ),
        track_three,
    ]

    normalized = [normalize_music_metadata(track) for track in tracks]

    # A release credit that names a guest ("Main Artist, Guest Artist") is kept
    # verbatim: album_artist is no longer split on commas, so names such as
    # "Hank Williams, Jr." and "Tyler, The Creator" survive intact.
    assert [track.album_artist for track in normalized] == [
        "Main Artist",
        "Main Artist, Guest Artist",
        "Main Artist",
    ]
    assert [track.title for track in normalized] == ["Song One", "Song Two", "Song Three"]
    assert [track.date for track in normalized] == ["2024", "2024", "2024-07-11"]
    assert [track.track_num for track in normalized] == [1, 2, 3]


def test_album_tracks_sharing_one_release_credit_share_one_album_artist() -> None:
    """The shape the album download path produces: one release-level credit.

    Every track of a release carries the same album artist, so the canonical
    folder layout keeps the album in a single directory.
    """

    tracks = [
        _track(track_num=1, isrc="USAAA1111111"),
        _track(title="Song Two", track_num=2, isrc="USAAA1111112"),
        _track(title="Song Three", track_num=3, isrc="USAAA1111113"),
    ]

    normalized = [normalize_music_metadata(track) for track in tracks]

    assert {track.album_artist for track in normalized} == {"Main Artist"}


def test_album_artist_keeps_comma_containing_names_intact() -> None:
    """Album artists whose own name contains a comma are not truncated."""

    for name in ("Hank Williams, Jr.", "Tyler, The Creator"):
        track = _track(artist=name, album_artist=name)
        normalized = normalize_music_metadata(track)
        assert normalized.album_artist == name
        assert normalized.artist == name
