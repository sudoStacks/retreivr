from metadata.services.musicbrainz_service import MUSICBRAINZ_USER_AGENT, get_musicbrainz_service


def search_release_groups(query: str, limit: int = 10):
    return get_musicbrainz_service().search_release_groups(query, limit=limit)


def pick_best_release_with_reason(release_group_id: str, *, prefer_country: str | None = None):
    return get_musicbrainz_service().pick_best_release_with_reason(release_group_id, prefer_country=prefer_country)


def pick_best_release(release_group_id: str):
    return get_musicbrainz_service().pick_best_release(release_group_id)


def fetch_release_tracks(release_id: str):
    return get_musicbrainz_service().fetch_release_tracks(release_id)

__all__ = [
    "MUSICBRAINZ_USER_AGENT",
    "search_release_groups",
    "pick_best_release",
    "pick_best_release_with_reason",
    "fetch_release_tracks",
]
