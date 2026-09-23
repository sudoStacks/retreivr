"""Discovery adapters feed canonical playlist tracks, then the existing acquisition queue."""
from collections import Counter
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import time

from engine.discovery_identity import mbid, score_recordings
from engine.import_pipeline import _classify_library_duplicate, _load_library_duplicate_index, _enqueue_music_track_job
from engine.job_queue import build_download_job_payload
from engine.download_defaults import resolve_effective_download_settings
from metadata.importers.base import TrackIntent
from metadata.importers.dispatcher import detect_format


class MusicBrainzGenreProvider:
    id = 'musicbrainz_genre'
    def preview(self, service, query, limit):
        rows = service.discover_artists(query, genre=True, limit=limit)
        unique = {mbid(row.get('id')): {'artist_mbid': mbid(row.get('id')), 'name': row['name'],
                  'score': int(row.get('ext:score') or 0), 'disambiguation': row.get('disambiguation', '')}
                  for row in rows if mbid(row.get('id'))}
        return sorted(unique.values(), key=lambda r: (-r['score'], r['name']))[:limit]


PROVIDERS = {MusicBrainzGenreProvider.id: MusicBrainzGenreProvider()}


class DiscoveryService:
    def __init__(self, store, musicbrainz, *, queue=None, config=None):
        self.store, self.mb, self.queue = store, musicbrainz, queue
        self.config = config or {}

    def import_file(self, content, filename):
        adapter = detect_format(filename, content)
        if hasattr(adapter, 'parse_playlists'):
            playlists = adapter.parse_playlists(content)
        else:
            playlists = [{'name': Path(filename).stem, 'source_id': None, 'tracks': adapter.parse(content)}]
        if sum(len(p['tracks']) for p in playlists) > 10000 or len(playlists) > 100:
            raise ValueError('Import limit: 100 playlists and 10000 track memberships per file')
        result = []
        for playlist in playlists:
            name = playlist['name'] if playlist['name'] != 'Imported playlist' else Path(filename).stem
            intents = [asdict(track) for track in playlist['tracks']]
            # Apple stable playlist ID survives membership changes; other formats use source/name.
            key = hashlib.sha256(f"{type(adapter).__name__}:{playlist.get('source_id') or name}".encode()).hexdigest()[:32]
            fingerprint = hashlib.sha256(json.dumps(intents, sort_keys=True).encode()).hexdigest()
            with self.store.connect() as conn:
                conn.execute('BEGIN IMMEDIATE')
                old = conn.execute('SELECT fingerprint FROM discovery_playlists WHERE id=?', (key,)).fetchone()
                if old and old['fingerprint'] != fingerprint:
                    active = conn.execute("SELECT 1 FROM discovery_jobs WHERE id=? AND state IN ('pending','running')", ('resolve:'+key,)).fetchone()
                    if active:
                        raise ValueError('Playlist resolution is active; retry the changed import after it finishes')
                if not old or old['fingerprint'] != fingerprint:
                    conn.execute('INSERT OR REPLACE INTO discovery_playlists VALUES (?,?,?,?,?)', (key, name, type(adapter).__name__, fingerprint, time.time()))
                    conn.execute('DELETE FROM discovery_tracks WHERE playlist_id=?', (key,))
                    conn.executemany('INSERT INTO discovery_tracks(playlist_id,position,intent) VALUES (?,?,?)',
                                     [(key, i, json.dumps(track)) for i, track in enumerate(intents)])
            result.append(self.playlist(key))
        return result

    def playlist(self, key):
        rows = self.store.rows('SELECT * FROM discovery_playlists WHERE id=?', (key,))
        if not rows:
            raise ValueError('Playlist not found')
        tracks = self.store.rows('SELECT * FROM discovery_tracks WHERE playlist_id=? ORDER BY position', (key,))
        for track in tracks:
            for field in ('intent', 'identity', 'candidates'):
                track[field] = json.loads(track[field]) if track[field] else None
        return {**rows[0], 'tracks': tracks, 'summary': dict(Counter(t['state'] for t in tracks))}

    def refresh_library(self, key):
        playlist = self.playlist(key)
        library = _load_library_duplicate_index(self.store.path)
        for row in playlist['tracks']:
            if row['identity']:
                state = 'existing' if self.owned(row['identity'], library) else 'resolved'
                if state != row['state']:
                    self.store.execute('UPDATE discovery_tracks SET state=? WHERE playlist_id=? AND position=?', (state, key, row['position']))
        return self.playlist(key)

    def resolve_playlist(self, key, stop=None):
        playlist = self.playlist(key)
        library = _load_library_duplicate_index(self.store.path)
        memo = {}
        for track in playlist['tracks']:
            if stop and stop.is_set():
                raise InterruptedError()
            if track['state'] != 'pending':
                continue
            intent = TrackIntent(**track['intent'])
            signature = json.dumps(track['intent'], sort_keys=True)
            if signature not in memo:
                if mbid(intent.recording_mbid):
                    recording = (self.mb.get_recording(intent.recording_mbid, includes=['artists', 'releases', 'isrcs']) or {}).get('recording')
                    recordings = [recording] if recording else []
                elif intent.isrc:
                    recordings = self.mb.recordings_by_isrc(intent.isrc)
                elif intent.artist and intent.title:
                    recordings = self.mb.search_recordings(intent.artist, intent.title, album=intent.album, limit=10).get('recording-list', [])
                else:
                    recordings = []
                memo[signature] = score_recordings(intent, recordings)
            state, candidates = memo[signature]
            identity = candidates[0] if state == 'resolved' else None
            if identity and self.owned(identity, library):
                state = 'existing'
            self.store.execute('UPDATE discovery_tracks SET state=?,candidates=?,identity=? WHERE playlist_id=? AND position=?',
                               (state, json.dumps(candidates), json.dumps(identity) if identity else None, key, track['position']))
        return self.playlist(key)['summary']

    def owned(self, identity, library=None):
        return _classify_library_duplicate(library if library is not None else _load_library_duplicate_index(self.store.path),
                    artist=identity.get('artist'), title=identity.get('title'), album=identity.get('album'),
                    recording_mbid=identity.get('recording_mbid'), mb_release_id=identity.get('release_mbid'),
                    mb_release_group_id=identity.get('release_group_mbid'))

    def choose(self, key, position, recording_id):
        playlist = self.playlist(key)
        row = next((t for t in playlist['tracks'] if t['position'] == position), None)
        candidate = next((c for c in (row or {}).get('candidates', []) if c['recording_mbid'] == recording_id), None)
        if not candidate:
            raise ValueError('Choose a recording from the reviewed candidates')
        if row['job_id']:
            raise ValueError('This track has already been submitted to acquisition')
        self.store.execute('UPDATE discovery_tracks SET identity=?,state=? WHERE playlist_id=? AND position=?',
                           (json.dumps(candidate), 'existing' if self.owned(candidate) else 'resolved', key, position))
        return self.playlist(key)

    def acquire(self, key, positions=None):
        if self.queue is None:
            raise ValueError('Acquisition is disabled in resolver-only mode')
        playlist = self.playlist(key)
        defaults = resolve_effective_download_settings(self.config, media_mode='music', fallback_destination='/downloads')
        counts = Counter()
        library = _load_library_duplicate_index(self.store.path)
        for row in playlist['tracks']:
            if positions is not None and row['position'] not in positions:
                continue
            identity = row['identity']
            if not identity or not identity.get('release_mbid'):
                counts['needs_review'] += 1
                continue
            if self.owned(identity, library):
                counts['existing'] += 1
                continue
            if row['job_id']:
                counts['already_submitted'] += 1
                continue
            try:
                # Imported track/disc numbers belong to the source edition. Bind positions and
                # dates to the selected MusicBrainz release before the existing queue validates it.
                release_tracks = self.mb.fetch_release_tracks(identity['release_mbid'])
                matches = [t for t in release_tracks if t.get('recording_mbid') == identity['recording_mbid']]
                if not matches:
                    counts['needs_review'] += 1
                    continue
                if len(matches) > 1:
                    matches = [t for t in matches if t.get('track_number') == identity.get('track_number')
                               and t.get('disc_number') == identity.get('disc_number')]
                if len(matches) != 1:
                    counts['needs_review'] += 1
                    continue
                identity = {**identity, **matches[0]}
                self.store.execute('UPDATE discovery_tracks SET identity=? WHERE playlist_id=? AND position=?',
                                   (json.dumps(identity), key, row['position']))
                job_id, created, reason = _enqueue_music_track_job(self.queue, build_download_job_payload,
                    runtime_config=self.config, base_dir='/downloads', destination=defaults.get('destination'),
                    final_format_override=defaults.get('final_format'), import_batch_id=key, playlist_name=playlist['name'],
                    source_index=row['position'], recording_mbid=identity['recording_mbid'], release_mbid=identity.get('release_mbid'),
                    release_group_mbid=identity.get('release_group_mbid'), artist=identity['artist'], title=identity['title'],
                    album=identity.get('album'), release_date=identity.get('release_date'), track_number=identity.get('track_number'),
                    disc_number=identity.get('disc_number'), disc_total=identity.get('disc_total'), duration_ms=identity.get('duration_ms'))
            except Exception:
                counts['failed'] += 1
                continue
            if job_id:
                self.store.execute('UPDATE discovery_tracks SET job_id=? WHERE playlist_id=? AND position=?', (job_id, key, row['position']))
            counts['enqueued' if created else 'duplicate' if job_id else 'rejected'] += 1
        return dict(counts)

    def subscribe(self, artist_id, name, policy=None):
        artist_id = mbid(artist_id)
        if not artist_id or not isinstance(name, str) or not name.strip():
            raise ValueError('Artist MBID and name are required')
        policy = policy or {'types': ['Album'], 'secondary_types': [], 'max_releases': 25}
        if not isinstance(policy, dict):
            raise ValueError('Subscription policy must be an object')
        types = policy.get('types', ['Album'])
        secondary = policy.get('secondary_types', [])
        if not isinstance(types, list) or not types or not all(isinstance(t, str) for t in types) or set(types)-{'Album', 'EP', 'Single'}:
            raise ValueError('Supported types: Album, EP, Single')
        if not isinstance(secondary, list) or not all(isinstance(t, str) for t in secondary) or set(secondary)-{'Live', 'Compilation', 'Remix', 'Soundtrack', 'Demo', 'DJ-mix', 'Mixtape/Street'}:
            raise ValueError('Unsupported secondary release type')
        policy = {'types': types, 'secondary_types': secondary, 'max_releases': max(1, min(100, int(policy.get('max_releases', 25))))}
        self.store.execute('INSERT INTO artist_subscriptions(artist_mbid,name,policy) VALUES (?,?,?) ON CONFLICT(artist_mbid) DO UPDATE SET name=excluded.name,policy=excluded.policy', (artist_id, name, json.dumps(policy)))
        return artist_id

    def refresh_artist(self, artist_id, stop=None):
        rows = self.store.rows('SELECT * FROM artist_subscriptions WHERE artist_mbid=?', (artist_id,))
        if not rows or not rows[0]['enabled']:
            return {'skipped': True}
        subscription = rows[0]
        policy = json.loads(subscription['policy'])
        groups = {}
        # Bounded browse; exact MBIDs avoid name collisions and one row per release group avoids editions.
        for offset in range(0, 1000, 100):
            if stop and stop.is_set():
                raise InterruptedError()
            page = self.mb.browse_artist_groups(artist_id, offset=offset)
            for group in page:
                if group.get('primary-type') not in policy['types']:
                    continue
                if set(group.get('secondary-type-list', []))-set(policy['secondary_types']):
                    continue
                if mbid(group.get('id')):
                    groups[group['id']] = group
            if len(page) < 100:
                break
        selected = sorted(groups.values(), key=lambda g: (g.get('first-release-date', ''), g['id']), reverse=True)[:policy['max_releases']]
        for group in selected:
            active = self.store.rows('SELECT enabled FROM artist_subscriptions WHERE artist_mbid=?', (artist_id,))
            if not active or not active[0]['enabled']:
                return {'skipped': True}
            self.store.execute('INSERT INTO subscription_releases(artist_mbid,release_group_mbid,payload) VALUES (?,?,?) ON CONFLICT(artist_mbid,release_group_mbid) DO UPDATE SET payload=excluded.payload', (artist_id, group['id'], json.dumps(group)))
        self.store.execute('UPDATE artist_subscriptions SET last_checked=?,error=NULL WHERE artist_mbid=?', (time.time(), artist_id))
        return {'discovered': len(selected), 'browse_limit': 1000}

    def expand_release(self, artist_id, group_id):
        rows = self.store.rows('SELECT * FROM subscription_releases WHERE artist_mbid=? AND release_group_mbid=?', (artist_id, group_id))
        if not rows:
            raise ValueError('Preview the subscription releases first')
        if rows[0]['playlist_id']:
            return self.playlist(rows[0]['playlist_id'])
        release_id = self.mb.pick_best_release(group_id)
        if not release_id:
            raise ValueError('No MusicBrainz release available')
        tracks = self.mb.fetch_release_tracks(release_id)
        if not tracks:
            raise ValueError('No release tracks available')
        key = 'release:'+group_id
        with self.store.connect() as conn:
            conn.execute('INSERT OR IGNORE INTO discovery_playlists VALUES (?,?,?,?,?)', (key, tracks[0].get('album') or group_id, 'artist_subscription', release_id, time.time()))
            for position, track in enumerate(tracks):
                if not mbid(track.get('recording_mbid')):
                    continue
                identity = {**track, 'release_mbid': release_id, 'release_group_mbid': group_id, 'artist_mbid': artist_id}
                intent = asdict(TrackIntent(artist=track['artist'], title=track['title'], album=track['album'], raw_line='', source_format='musicbrainz', recording_mbid=track['recording_mbid']))
                conn.execute("INSERT OR IGNORE INTO discovery_tracks(playlist_id,position,intent,state,identity) VALUES (?,?,?,'resolved',?)", (key, position, json.dumps(intent), json.dumps(identity)))
            conn.execute('UPDATE subscription_releases SET playlist_id=? WHERE artist_mbid=? AND release_group_mbid=?', (key, artist_id, group_id))
        return self.playlist(key)

    def preview_discography(self, artist_id, group_ids, stop=None):
        """Materialize a bounded selection for review; never enqueue acquisition here."""
        groups = sorted(set(group_ids))
        if not groups or len(groups) > 25 or not all(mbid(group) for group in groups):
            raise ValueError('Select 1–25 release groups')
        subscriptions = self.store.rows('SELECT name FROM artist_subscriptions WHERE artist_mbid=?', (artist_id,))
        if not subscriptions:
            raise ValueError('Subscription not found')
        key = 'discography:'+hashlib.sha256((artist_id+':'+','.join(groups)).encode()).hexdigest()[:32]
        existing = self.store.rows('SELECT id FROM discovery_playlists WHERE id=?', (key,))
        if existing:
            return self.refresh_library(key)
        members = []
        for group in groups:
            if stop and stop.is_set():
                raise InterruptedError()
            members.extend(self.expand_release(artist_id, group)['tracks'])
        with self.store.connect() as conn:
            conn.execute('INSERT OR IGNORE INTO discovery_playlists VALUES (?,?,?,?,?)',
                         (key, subscriptions[0]['name']+' — selected releases', 'artist_subscription', ','.join(groups), time.time()))
            for position, track in enumerate(members):
                conn.execute('INSERT OR IGNORE INTO discovery_tracks(playlist_id,position,intent,state,identity) VALUES (?,?,?,?,?)',
                             (key, position, json.dumps(track['intent']), track['state'], json.dumps(track['identity'])))
        return self.refresh_library(key)
