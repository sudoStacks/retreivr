from typing import Protocol, TypedDict


class CanonicalArtwork(TypedDict, total=False):
    url: str
    width: int | None
    height: int | None


class CanonicalTrackEntry(TypedDict, total=False):
    title: str
    duration_sec: int | None
    track_number: int | None
    disc_number: int | None


class CanonicalTrack(TypedDict, total=False):
    kind: str
    provider: str
    artist: str
    album: str | None
    track: str
    release_year: str | None
    album_type: str | None
    duration_sec: int | None
    artwork: list[CanonicalArtwork]
    external_ids: dict[str, str | None]
    track_number: int | None
    disc_number: int | None
    album_track_count: int | None


class CanonicalAlbum(TypedDict, total=False):
    kind: str
    provider: str
    artist: str
    album: str
    release_year: str | None
    album_type: str | None
    artwork: list[CanonicalArtwork]
    external_ids: dict[str, str | None]
    track_count: int | None
    tracks: list[CanonicalTrackEntry]


class CanonicalMetadataProvider(Protocol):
    def resolve_track(self, artist, track, *, album=None) -> CanonicalTrack | None:
        raise NotImplementedError

    def resolve_album(self, artist, album) -> CanonicalAlbum | None:
        raise NotImplementedError
