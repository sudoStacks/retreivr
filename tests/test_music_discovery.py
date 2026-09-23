"""Discovery contracts with in-memory external services, no live credentials/services."""
from dataclasses import asdict
import json
import plistlib
from types import SimpleNamespace
from unittest.mock import Mock
import sqlite3
import time

import pytest
import requests

from engine.discovery import DiscoveryService, MusicBrainzGenreProvider
from engine.discovery_identity import score_recordings, normalize, match_jellyfin
from engine.discovery_store import DiscoveryStore, ensure_discovery_tables
from engine.discovery_worker import DiscoveryWorker
from engine.jellyfin_playlists import JellyfinClient, JellyfinError, synchronize
from engine.resolution_evidence import EvidenceCache
from metadata.importers.apple_xml_importer import AppleXMLImporter
from metadata.importers.base import TrackIntent

RID = '11111111-1111-4111-8111-111111111111'
RID2 = '22222222-2222-4222-8222-222222222222'
RELEASE = '33333333-3333-4333-8333-333333333333'
ARTIST = '44444444-4444-4444-8444-444444444444'
GROUP = '55555555-5555-4555-8555-555555555555'


def xml(order=(2, 1, 2), title='Country'):
    return plistlib.dumps({'Tracks': {'1': {'Name': 'One', 'Artist': 'Singer', 'Album': 'Album', 'Persistent ID': 'a', 'Store ID': 123, 'Year': 2000, 'Track Number': 1, 'Disc Number': 1, 'Total Time': 200000},
                                       '2': {'Name': 'Two', 'Artist': 'Singer', 'Album': 'Album', 'Year': 2000, 'Total Time': 200000}},
                          'Playlists': [{'Name': title, 'Playlist Persistent ID': 'p123', 'Playlist Items': [{'Track ID': n} for n in order]}]}, fmt=plistlib.FMT_XML)


def recording(rid=RID, title='One', **extra):
    return {'id': rid, 'title': title, 'artist-credit': [{'artist': {'name': 'Singer', 'id': ARTIST}}],
            'release-list': [{'id': RELEASE, 'title': 'Album', 'release-group': {'id': GROUP}}], 'length': '200000', **extra}


def identity():
    return {'recording_mbid': RID, 'release_mbid': RELEASE, 'artist': 'Singer', 'title': 'One', 'album': 'Album'}


@pytest.fixture
def service(tmp_path):
    mb = Mock()
    mb.search_recordings.side_effect = lambda artist, title, **kw: {'recording-list': [recording(title=title)]}
    mb.fetch_release_tracks.return_value = [{**identity(), 'release_group_mbid': GROUP, 'release_date': '2000', 'track_number': 1, 'disc_number': 1, 'duration_ms': 200000}]
    return DiscoveryService(DiscoveryStore(tmp_path/'db.sqlite'), mb)


def resolved_playlist(service):
    key = service.import_file(xml((1, 1)), 'Country.xml')[0]['id']
    service.resolve_playlist(key)
    return key


def test_apple_streaming_order_duplicates_and_identity():
    rows = AppleXMLImporter().parse_playlists(xml())
    assert rows[0]['name'] == 'Country'
    tracks = rows[0]['tracks']
    assert [r.title for r in tracks] == ['Two', 'One', 'Two']
    assert tracks[1].persistent_id == 'a'
    assert tracks[1].apple_music_id == '123'
    assert tracks[0].release_date == '2000'
    assert tracks[0].duration_ms == 200000


def test_normalize_unicode_and_whitespace():
    assert normalize('  ＳINGER—One! ') == 'singer one'


def test_high_confidence_and_duplicate_recordings():
    intent = TrackIntent('Singer', 'One', 'Album', '', 'test')
    state, candidates = score_recordings(intent, [recording(), recording()])
    assert state == 'resolved' and len(candidates) == 1
    assert candidates[0]['release_mbid'] == RELEASE


def test_ambiguous_never_selects():
    state, candidates = score_recordings(TrackIntent('Singer', 'One', 'Album', '', 'test'), [recording(), recording(RID2)])
    assert state == 'ambiguous' and len(candidates) == 2


def test_probable_and_duration_mismatch():
    state, _ = score_recordings(TrackIntent('Singer', 'One', 'Another Album', '', 'test'), [recording()])
    assert state == 'probable'
    state, _ = score_recordings(TrackIntent('Singer', 'One', 'Album', '', 'test', duration_ms=90000), [recording()])
    assert state == 'unresolved'


def test_direct_mbid_does_not_override_wrong_metadata():
    state, _ = score_recordings(TrackIntent('Other', 'Different', None, '', 'test', recording_mbid=RID), [recording()])
    assert state == 'unresolved'


def test_repeated_import_preserves_review_and_order(service):
    key = resolved_playlist(service)
    assert service.import_file(xml((1, 1)), 'Country.xml')[0]['id'] == key
    assert service.playlist(key)['summary'] == {'resolved': 2}
    assert service.mb.search_recordings.call_count == 1
    assert len(service.store.rows('SELECT * FROM discovery_playlists')) == 1


def test_changed_membership_same_playlist(service):
    key = resolved_playlist(service)
    assert service.import_file(xml((2, 1, 2)), 'Country.xml')[0]['id'] == key
    assert service.playlist(key)['summary'] == {'pending': 3}


def test_changed_import_rejected_during_resolution(service):
    key = resolved_playlist(service)
    service.store.schedule('resolve:'+key, 'resolve', {'playlist_id': key})
    with pytest.raises(ValueError):
        service.import_file(xml((2,)), 'Country.xml')
    assert len(service.playlist(key)['tracks']) == 2


def test_ambiguous_manual_review(service):
    service.mb.search_recordings.side_effect = None
    service.mb.search_recordings.return_value = {'recording-list': [recording(), recording(RID2)]}
    key = resolved_playlist(service)
    assert service.playlist(key)['tracks'][0]['identity'] is None
    service.choose(key, 0, RID2)
    assert service.playlist(key)['tracks'][0]['identity']['recording_mbid'] == RID2
    with pytest.raises(ValueError):
        service.choose(key, 0, ARTIST)


def test_resolver_only_service_refuses_acquisition(service):
    with pytest.raises(ValueError, match='disabled'):
        service.acquire(resolved_playlist(service))


def test_acquisition_uses_existing_queue_and_is_repeatable(service):
    from engine.job_queue import DownloadJobStore
    service.queue = DownloadJobStore(service.store.path)
    from engine.core import init_db
    init_db(service.store.path)
    key = resolved_playlist(service)
    first = service.acquire(key)
    assert first['enqueued'] == 1
    assert service.acquire(key) == {'already_submitted': 2}
    assert len(service.store.rows('SELECT * FROM download_jobs')) == 1


def test_local_library_rechecked_before_acquisition(service, tmp_path):
    key = resolved_playlist(service)
    path = tmp_path/'one.mp3'; path.write_bytes(b'a')
    with service.store.connect() as conn:
        conn.execute('CREATE TABLE music_library_index(path,title,artist,album,recording_mbid,mb_release_id,mb_release_group_id)')
        conn.execute('INSERT INTO music_library_index VALUES (?,?,?,?,?,?,?)', (str(path), 'One', 'Singer', 'Album', RID, RELEASE, GROUP))
    service.queue = Mock()
    assert service.acquire(key) == {'existing': 2}
    service.queue.enqueue_job.assert_not_called()


def test_migration_reentrant_preserves_existing_data(tmp_path):
    conn = sqlite3.connect(tmp_path/'db')
    conn.execute('CREATE TABLE old_table(value)'); conn.execute("INSERT INTO old_table VALUES ('keep')")
    ensure_discovery_tables(conn); ensure_discovery_tables(conn)
    assert conn.execute('SELECT value FROM old_table').fetchone()[0] == 'keep'
    assert conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    conn.close()


def test_artist_subscription_refresh_deduplicates_editions(service):
    service.subscribe(ARTIST, 'Singer')
    service.subscribe(ARTIST, 'Singer')
    group = {'id': GROUP, 'title': 'Album', 'primary-type': 'Album', 'first-release-date': '2000'}
    service.mb.browse_artist_groups.return_value = [group, group, {**group, 'id': RELEASE, 'secondary-type-list': ['Live']}]
    service.refresh_artist(ARTIST); service.refresh_artist(ARTIST)
    assert len(service.store.rows('SELECT * FROM artist_subscriptions')) == 1
    assert len(service.store.rows('SELECT * FROM subscription_releases')) == 1
    service.mb.pick_best_release.return_value = RELEASE
    service.mb.fetch_release_tracks.return_value = [identity()]
    first = service.expand_release(ARTIST, GROUP)
    assert service.expand_release(ARTIST, GROUP)['id'] == first['id']
    service.mb.pick_best_release.assert_called_once()
    service.store.execute('UPDATE artist_subscriptions SET enabled=0')
    assert service.refresh_artist(ARTIST) == {'skipped': True}


def test_provider_deduplicates_and_preview_never_subscribes(service):
    service.mb.discover_artists.return_value = [{'id': ARTIST, 'name': 'Singer', 'ext:score': '100'}]*2
    assert len(MusicBrainzGenreProvider().preview(service.mb, 'Country', 20)) == 1
    assert not service.store.rows('SELECT * FROM artist_subscriptions')


def test_cache_hit_miss_release_scope_expiry_and_dead(service):
    cache = EvidenceCache(service.store)
    assert cache.lookup(identity()) is None
    candidate = {'video_id': 'abcdefghijk', 'final_score': 98}
    assert cache.put(identity(), candidate)
    assert cache.lookup(identity())['video_id'] == 'abcdefghijk'
    assert cache.lookup({**identity(), 'release_mbid': GROUP}) is None
    assert not cache.put(identity(), {**candidate, 'final_score': 20})
    cache.invalidate(identity(), 'abcdefghijk')
    assert cache.lookup(identity()) is None
    cache.put(identity(), candidate)
    service.store.execute('UPDATE resolution_evidence SET verified_at=?', (time.time()-31*86400,))
    assert cache.lookup(identity()) is None


def test_builder_dedup_persistence_and_no_download(service):
    key = resolved_playlist(service)
    service.acquire = Mock(side_effect=AssertionError('download forbidden'))
    resolver = Mock()
    resolver.search_music_track_best_match.return_value = {'video_id': 'abcdefghijk', 'final_score': 98}
    worker = DiscoveryWorker(service, resolver)
    worker.seed(key); worker.seed(key)
    assert len(service.store.rows('SELECT * FROM discovery_jobs')) == 1
    assert worker.run_once()
    service.acquire.assert_not_called()
    assert service.store.rows('SELECT state FROM discovery_jobs')[0]['state'] == 'done'
    # Resume with a new worker; no duplicate source resolution.
    assert not DiscoveryWorker(service, resolver).run_once()
    resolver.search_music_track_best_match.assert_called_once()


def test_builder_skip_fresh_cache(service):
    key = resolved_playlist(service)
    worker = DiscoveryWorker(service, Mock())
    worker.cache.put(service.playlist(key)['tracks'][0]['identity'], {'video_id': 'abcdefghijk', 'final_score': 99})
    worker.seed(key); worker.run_once()
    worker.resolver.search_music_track_best_match.assert_not_called()


def test_builder_failures_bounded_and_secrets_not_retained(service):
    key = resolved_playlist(service)
    resolver = Mock(); resolver.search_music_track_best_match.side_effect = RuntimeError('secret-token')
    worker = DiscoveryWorker(service, resolver, max_attempts=2)
    worker.seed(key); worker.run_once()
    row = service.store.rows('SELECT * FROM discovery_jobs')[0]
    assert row['state'] == 'pending' and row['next_run'] > time.time()
    service.store.execute('UPDATE discovery_jobs SET next_run=0')
    worker.run_once(); worker.seed(key)
    row = service.store.rows('SELECT * FROM discovery_jobs')[0]
    assert row['state'] == 'failed' and row['attempts'] == 2 and 'secret' not in json.dumps(row)
    assert not worker.run_once()


def test_expired_worker_lease_resumes(service):
    key = resolved_playlist(service)
    worker = DiscoveryWorker(service, Mock())
    worker.seed(key)
    service.store.execute("UPDATE discovery_jobs SET state='running',lease_until=?", (time.time()-1,))
    assert service.store.claim()['attempts'] == 1
    assert service.store.claim() is None


class FakeJellyfin:
    url = 'http://jellyfin'; user = 'user'
    def __init__(self):
        self.remote = None; self.items = []; self.creates = 0; self.writes = 0
    def require_owner(self):
        pass
    def audio_items(self):
        return [{'Id': 'item1', 'Name': 'One', 'Artists': ['Singer'], 'Album': 'Album', 'ProviderIds': {'MusicBrainzTrack': RID, 'MusicBrainzAlbum': RELEASE}}]
    def paged(self, *args):
        return [self.remote] if self.remote else []
    def playlist_items(self, remote):
        return [{'Id': item, 'PlaylistItemId': str(i)} for i, item in enumerate(self.items)]
    def request(self, method, path, **kw):
        self.writes += 1
        if path == '/Playlists':
            self.creates += 1; self.remote = {'Id': 'remote', 'Name': kw['json']['Name']}; return self.remote
        if method == 'DELETE':
            self.items = []
        elif method == 'POST':
            self.items.extend(kw['params']['Ids'].split(','))
        return {}


def test_jellyfin_preview_and_idempotent_order_duplicates(service):
    key = resolved_playlist(service); client = FakeJellyfin()
    report = synchronize(service.store, service, key, client)
    assert report['matched'] == 2 and client.writes == 0
    assert report['source_repetitions'] == 1 and report['jellyfin_entries'] == 1
    synchronize(service.store, service, key, client, preview=False)
    writes = client.writes
    assert client.items == ['item1']
    synchronize(service.store, service, key, client, preview=False)
    assert client.creates == 1 and client.writes == writes


def test_jellyfin_ambiguous_and_mismatched_ids():
    items = FakeJellyfin().audio_items()
    assert match_jellyfin(identity(), items)[0] == 'matched'
    assert match_jellyfin(identity(), items+[{**items[0], 'Id': 'item2'}])[0] == 'ambiguous'
    assert match_jellyfin({**identity(), 'recording_mbid': RID2}, items)[0] == 'unmatched'


def test_jellyfin_recovers_lost_create_response(service):
    key = resolved_playlist(service); client = FakeJellyfin()
    original = client.request
    def fail_once(method, path, **kw):
        result = original(method, path, **kw)
        if path == '/Playlists':
            raise JellyfinError('timeout')
        return result
    client.request = fail_once
    with pytest.raises(JellyfinError):
        synchronize(service.store, service, key, client, preview=False)
    client.request = original
    synchronize(service.store, service, key, client, preview=False)
    assert client.creates == 1 and client.items == ['item1']


def test_jellyfin_unavailable_redacts_credentials(caplog):
    session = Mock(); session.request.side_effect = requests.ConnectionError('secret-token')
    client = JellyfinClient({'base_url': 'http://jellyfin', 'api_key': 'secret-token', 'user_id': ARTIST}, session)
    with pytest.raises(JellyfinError, match='unavailable') as error:
        client.test()
    assert 'secret-token' not in str(error.value) + caplog.text
    assert session.request.call_args.kwargs['allow_redirects'] is False


def test_jellyfin_http_error_omits_response_body():
    session = Mock(); session.request.return_value = SimpleNamespace(status_code=401, content=b'secret')
    client = JellyfinClient({'base_url': 'http://jellyfin', 'api_key': 'token', 'user_id': ARTIST}, session)
    with pytest.raises(JellyfinError, match='HTTP 401'):
        client.test()


def test_jellyfin_pagination():
    client = JellyfinClient({'base_url': 'http://jellyfin', 'api_key': 'token', 'user_id': ARTIST})
    client.request = Mock(side_effect=[{'Items': [{'Id': '1'}], 'TotalRecordCount': 2}, {'Items': [{'Id': '2'}], 'TotalRecordCount': 2}])
    assert len(client.audio_items()) == 2
    assert client.request.call_args.kwargs['params']['StartIndex'] == 1


def test_variant_never_silently_accepted():
    intent = TrackIntent('Singer', 'A Very Long Song Title', 'Album', '', 'test')
    state, _ = score_recordings(intent, [recording(title='A Very Long Song Title (Live)')])
    assert state == 'unresolved'


def test_remote_cache_cannot_overwrite_stronger_local(monkeypatch):
    import engine.community_cache as cache
    cache._CACHE.clear()
    monkeypatch.setattr(cache, 'lookup_recording_local', lambda *a, **k: {'video_id': 'abcdefghijk', 'confidence': .99})
    monkeypatch.setattr(cache, 'fetch_community_record', lambda *a: {'sources': [{'video_id': '12345678901', 'confidence': .80}]})
    persist = Mock(); monkeypatch.setattr(cache, 'persist_local_community_record', persist)
    assert cache.cached_lookup(RID, dataset_root='/unused')['video_id'] == 'abcdefghijk'
    persist.assert_not_called()


def test_remote_cache_fresh_local_short_circuits(monkeypatch):
    from datetime import datetime, timezone
    import engine.community_cache as cache
    cache._CACHE.clear()
    monkeypatch.setattr(cache, 'lookup_recording_local', lambda *a, **k: {'video_id': 'abcdefghijk', 'confidence': .99, 'last_verified_at': datetime.now(timezone.utc).isoformat()})
    remote = Mock(); monkeypatch.setattr(cache, 'fetch_community_record', remote)
    assert cache.cached_lookup(RID)['video_id'] == 'abcdefghijk'
    remote.assert_not_called()


def test_remote_cache_dead_stale_and_low_confidence():
    from engine.community_cache import extract_best_candidate
    assert extract_best_candidate({'sources': [{'video_id': 'abcdefghijk', 'confidence': .99, 'last_verified_at': '2000-01-01T00:00:00Z'}]}) is None
    assert extract_best_candidate({'sources': [{'video_id': 'abcdefghijk', 'confidence': .99, 'dead': True}]}) is None
    assert extract_best_candidate({'sources': [{'video_id': 'abcdefghijk', 'confidence': .2}]}) is None


def test_route_upload_resolve_and_preview(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api.music_discovery import create_router
    app = FastAPI()
    app.state.paths = SimpleNamespace(db_path=str(tmp_path/'api.sqlite'))
    app.include_router(create_router(app, lambda: {}))
    client = TestClient(app)
    response = client.post('/api/music/discovery/playlists', files={'file': ('Country.xml', xml(), 'text/xml')})
    assert response.status_code == 200
    key = response.json()['playlists'][0]['id']
    assert client.get('/api/music/discovery/playlists/'+key).json()['summary'] == {'pending': 3}
    assert client.post('/api/music/discovery/playlists/'+key+'/resolve', json={}).status_code == 200
    assert client.post('/api/music/discovery/playlists/'+key+'/acquire', json={}).status_code == 400
    assert client.post('/api/music/discovery/jellyfin/test', json={}).status_code == 400
    assert client.post('/api/music/discovery/playlists', files={'file': ('broken.xml', b'not xml', 'text/xml')}).status_code == 400
    state = client.get('/api/music/discovery/state').json()
    assert len(state['jobs']) == 1 and len(state['playlists']) == 1


def test_real_resolver_only_has_no_acquisition_store(tmp_path):
    from engine.search_engine import SearchResolutionService
    resolver = SearchResolutionService(search_db_path=str(tmp_path/'search.db'), queue_db_path=str(tmp_path/'queue.db'), adapters=[], config={}, resolution_only=True)
    assert resolver.queue_store is None
    assert not (tmp_path/'queue.db').exists()
    with pytest.raises(ValueError, match='disabled'):
        resolver.enqueue_item_candidate('item', 'candidate')


def test_missing_apple_track_reference_remains_in_playlist():
    tracks = AppleXMLImporter().parse(xml((1, 99, 1)))
    assert len(tracks) == 3
    assert tracks[1].title is None and tracks[1].raw_line == '99'


def test_stale_cache_can_be_revalidated_at_slightly_lower_score(service):
    cache = EvidenceCache(service.store)
    cache.put(identity(), {'video_id': 'abcdefghijk', 'final_score': 99})
    service.store.execute('UPDATE resolution_evidence SET verified_at=0')
    cache.put(identity(), {'video_id': 'abcdefghijk', 'final_score': 98})
    assert cache.lookup(identity())['confidence'] == .98


def test_selected_release_preview_is_idempotent_and_never_acquires(service):
    service.subscribe(ARTIST, 'Singer')
    service.mb.browse_artist_groups.return_value = [{'id': GROUP, 'title': 'Album', 'primary-type': 'Album'}]
    service.refresh_artist(ARTIST)
    service.mb.pick_best_release.return_value = RELEASE
    service.mb.fetch_release_tracks.return_value = [identity()]
    service.acquire = Mock(side_effect=AssertionError('preview must not acquire'))
    first = service.preview_discography(ARTIST, [GROUP, GROUP])
    second = service.preview_discography(ARTIST, [GROUP])
    assert first['id'] == second['id'] and len(second['tracks']) == 1
    service.acquire.assert_not_called()


def test_acquisition_uses_release_positions_not_import_positions(service):
    from engine.core import init_db
    from engine.job_queue import DownloadJobStore
    init_db(service.store.path)
    service.queue = DownloadJobStore(service.store.path)
    key = resolved_playlist(service)
    service.mb.fetch_release_tracks.return_value[0]['track_number'] = 7
    result = service.acquire(key, [0])
    assert result == {'enqueued': 1}
    assert service.playlist(key)['tracks'][0]['identity']['track_number'] == 7


def test_non_member_recording_cannot_be_acquired(service):
    key = resolved_playlist(service)
    service.queue = Mock()
    service.mb.fetch_release_tracks.return_value = []
    assert service.acquire(key) == {'needs_review': 2}
    service.queue.enqueue_job.assert_not_called()


@pytest.mark.parametrize('seed_accepted', [True, False])
def test_cache_seed_gate_short_circuits_or_falls_back(tmp_path, monkeypatch, seed_accepted):
    from engine.search_engine import SearchResolutionService, MusicTrackSelectionResult
    resolver = SearchResolutionService(search_db_path=str(tmp_path/'search.db'), queue_db_path=str(tmp_path/'queue.db'), adapters={}, config={}, resolution_only=True)
    candidate = {'candidate_id': 'cached', 'source': 'youtube', 'url': 'https://www.youtube.com/watch?v=abcdefghijk',
                 'title': 'Singer - One', 'community_confidence': .99, 'community_seeded': True, 'final_score': .99}
    monkeypatch.setattr(resolver, '_resolve_mb_relationship_candidates', lambda **kw: ([], {}))
    monkeypatch.setattr(resolver, '_resolve_community_cache_candidates', lambda **kw: ([candidate], {}))
    retrieve = Mock(return_value=[{**candidate, 'candidate_id': 'fallback', 'community_seeded': False}])
    monkeypatch.setattr(resolver, 'retrieve_candidates', retrieve)
    def gate(ctx, candidates):
        selected = candidates[0] if seed_accepted or candidates[0]['candidate_id'] == 'fallback' else None
        return MusicTrackSelectionResult(selected=selected, selected_pass='a' if selected else None, ranked=candidates,
            failure_reason='', coherence_boost_applied=0, mb_injected_rejections={}, community_seeded_rejections={},
            rejected_candidates=[], accepted_selection=None, final_rejection=None, candidate_variant_distribution={},
            selected_candidate_variant_tags=[], top_rejected_variant_tags=[])
    monkeypatch.setattr(resolver, 'rank_and_gate', gate)
    selected = resolver.search_music_track_best_match('Singer', 'One', album='Album', recording_mbid=RID, release_mbid=RELEASE)
    assert selected['candidate_id'] == ('cached' if seed_accepted else 'fallback')
    assert retrieve.call_count == (0 if seed_accepted else 1)
    assert resolver.queue_store is None


def test_jellyfin_write_requires_owner_token_before_mutation(service):
    key = resolved_playlist(service)
    client = FakeJellyfin()
    client.require_owner = Mock(side_effect=JellyfinError('owner token required'))
    with pytest.raises(JellyfinError):
        synchronize(service.store, service, key, client, preview=False)
    assert client.writes == 0
    assert not service.store.rows('SELECT * FROM jellyfin_playlist_links')


def test_jellyfin_validates_user_token_and_prefers_it_to_server_key():
    session = Mock()
    session.request.return_value = SimpleNamespace(status_code=200, content=b'{}', json=lambda: {'Id': ARTIST})
    client = JellyfinClient({'base_url': 'http://jellyfin', 'api_key': 'server-secret', 'access_token': 'user-secret', 'user_id': ARTIST}, session)
    client.require_owner()
    assert session.request.call_args.args[1].endswith('/Users/Me')
    assert session.request.call_args.kwargs['headers'] == {'X-Emby-Token': 'user-secret'}
    client.user = RID
    with pytest.raises(JellyfinError, match='does not belong'):
        client.require_owner()


def test_jellyfin_transient_read_before_create_is_retryable(service):
    key = resolved_playlist(service)
    client = FakeJellyfin()
    client.paged = Mock(side_effect=JellyfinError('unavailable'))
    with pytest.raises(JellyfinError):
        synchronize(service.store, service, key, client, preview=False)
    assert client.creates == 0
    assert service.store.rows('SELECT state FROM jellyfin_playlist_links')[0]['state'] == 'new'
    client.paged = Mock(return_value=[])
    synchronize(service.store, service, key, client, preview=False)
    assert client.creates == 1
