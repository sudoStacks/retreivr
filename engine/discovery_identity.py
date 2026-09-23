"""Conservative identity matching shared by playlist discovery and Jellyfin."""
import re
import unicodedata
from uuid import UUID
from rapidfuzz.fuzz import ratio
from engine.musicbrainz_binding import _extract_variant_triggers


def mbid(value):
    try:
        return str(UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        return None


def normalize(value):
    return ' '.join(re.sub(r'[^\w]+', ' ', unicodedata.normalize('NFKC', str(value or '')).casefold()).split())


def artist_credit(credit):
    return ''.join((entry.get('artist', {}).get('name') or entry.get('name') or '') + entry.get('joinphrase', '')
                   if isinstance(entry, dict) else str(entry) for entry in credit or [])


def score_recordings(intent, recordings):
    """Deduplicate recordings before margin testing; retain release identity/evidence."""
    ranked = {}
    for recording in recordings:
        rid = mbid(recording.get('id'))
        if not rid:
            continue
        artist = artist_credit(recording.get('artist-credit'))
        title = recording.get('title') or ''
        evidence = {'title': ratio(normalize(intent.title), normalize(title))/100,
                    'artist': ratio(normalize(intent.artist), normalize(artist))/100}
        releases = recording.get('release-list') or []
        releases = sorted(releases, key=lambda r: (-ratio(normalize(intent.album), normalize(r.get('title'))), str(r.get('id'))))
        release = releases[0] if releases else {}
        evidence['album'] = ratio(normalize(intent.album), normalize(release.get('title')))/100 if intent.album else None
        length = int(recording.get('length') or 0)
        delta = abs(length-intent.duration_ms) if length and intent.duration_ms else None
        evidence['duration_delta_ms'] = delta
        score = .55*evidence['title'] + .45*evidence['artist']
        if intent.album:
            score = .85*score + .15*evidence['album']
        variant_mismatch = bool(set(_extract_variant_triggers(title)) - set(_extract_variant_triggers((intent.title or '')+' '+(intent.album or ''))))
        evidence['variant_mismatch'] = variant_mismatch
        if variant_mismatch or (delta is not None and delta > 10000):
            score = min(score, .70)
        direct = mbid(intent.recording_mbid) == rid
        isrc = bool(intent.isrc and intent.isrc.upper() in [s.upper() for s in recording.get('isrc-list', [])])
        evidence.update(direct_mbid=direct, isrc=isrc)
        if not variant_mismatch and (direct or isrc) and evidence['title'] >= .9 and evidence['artist'] >= .9 and (delta is None or delta <= 10000):
            score = 1.0
        candidate = {'recording_mbid': rid, 'release_mbid': mbid(release.get('id')),
                     'release_group_mbid': mbid((release.get('release-group') or {}).get('id')),
                     'artist_mbid': next((mbid(c.get('artist', {}).get('id')) for c in recording.get('artist-credit', []) if isinstance(c, dict)), None),
                     'artist': artist, 'title': title, 'album': release.get('title') or intent.album,
                     'duration_ms': length or intent.duration_ms, 'release_date': release.get('date') or intent.release_date,
                     'track_number': intent.track_number, 'disc_number': intent.disc_number,
                     'score': round(score, 4), 'evidence': evidence}
        if rid not in ranked or score > ranked[rid]['score']:
            ranked[rid] = candidate
    candidates = sorted(ranked.values(), key=lambda c: (-c['score'], c['recording_mbid']))
    if not candidates or candidates[0]['score'] < .75:
        return 'unresolved', candidates
    top = candidates[0]
    if len(candidates) > 1 and top['score'] - candidates[1]['score'] < .06:
        return 'ambiguous', candidates
    return ('resolved' if top['score'] >= .94 else 'probable'), candidates


def match_jellyfin(identity, items):
    rid = identity.get('recording_mbid')
    release = identity.get('release_mbid')
    exact, metadata = [], []
    for item in items:
        providers = {k.lower(): str(v).lower() for k, v in (item.get('ProviderIds') or {}).items()}
        item_rid = providers.get('musicbrainztrack') or providers.get('musicbrainzrecording')
        item_release = providers.get('musicbrainzalbum')
        if rid and item_rid == rid:
            if release and item_release and item_release != release:
                continue
            exact.append(item)
            continue
        if item_rid and rid and item_rid != rid:
            continue
        artists = item.get('Artists') or []
        if (normalize(item.get('Name')) == normalize(identity.get('title'))
                and normalize(identity.get('artist')) in {normalize(a) for a in artists}
                and identity.get('album') and normalize(item.get('Album')) == normalize(identity['album'])):
            length = item.get('RunTimeTicks', 0)/10000
            if identity.get('duration_ms') and length and abs(length-identity['duration_ms']) > 10000:
                continue
            metadata.append(item)
    matches = {item['Id']: item for item in exact or metadata}
    return ('matched', next(iter(matches))) if len(matches) == 1 else ('ambiguous' if matches else 'unmatched', None)
