"""Music discovery routes. Uses the application's existing config, DB and queue."""
import json
from fastapi import APIRouter, Body, File, HTTPException, UploadFile
from engine.discovery import DiscoveryService, PROVIDERS
from engine.discovery_store import DiscoveryStore
from engine.discovery_worker import DiscoveryWorker
from engine.jellyfin_playlists import JellyfinClient, JellyfinError, synchronize
from metadata.services.musicbrainz_service import get_musicbrainz_service


def create_router(app, config_getter):
    router = APIRouter(prefix='/api/music/discovery', tags=['music-discovery'])

    def service():
        engine = getattr(app.state, 'worker_engine', None)
        return DiscoveryService(DiscoveryStore(app.state.paths.db_path), get_musicbrainz_service(),
                                queue=getattr(engine, 'store', None), config=config_getter())

    def invoke(callback):
        try:
            return callback()
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc)) from None
        except JellyfinError as exc:
            raise HTTPException(502, str(exc)) from None

    @router.get('/state')
    def state():
        store = service().store
        subscriptions = store.rows('SELECT * FROM artist_subscriptions ORDER BY name')
        for subscription in subscriptions:
            subscription['policy'] = json.loads(subscription['policy'])
            releases = store.rows('SELECT * FROM subscription_releases WHERE artist_mbid=?', (subscription['artist_mbid'],))
            subscription['releases'] = [{**r, 'payload': json.loads(r['payload'])} for r in releases]
        return {'playlists': store.rows('SELECT * FROM discovery_playlists ORDER BY updated_at DESC LIMIT 100'),
                'subscriptions': subscriptions, 'providers': list(PROVIDERS),
                'jobs': store.rows('SELECT id,kind,state,attempts,error,updated_at,result FROM discovery_jobs ORDER BY updated_at DESC LIMIT 100'),
                'job_counts': store.rows('SELECT kind,state,count(*) AS count FROM discovery_jobs GROUP BY kind,state'),
                'cache': store.rows('SELECT status,count(*) AS count,round(avg(confidence),3) AS confidence,max(verified_at) AS last_activity FROM resolution_evidence GROUP BY status'),
                'metrics': store.rows('SELECT * FROM discovery_metrics')}

    @router.post('/playlists')
    async def upload(file: UploadFile = File(...)):
        content = await file.read(10*1024*1024+1)
        if len(content) > 10*1024*1024:
            raise HTTPException(413, 'Playlist exceeds 10 MiB')
        # Parser/SQLite are blocking; keep the async event loop available.
        from starlette.concurrency import run_in_threadpool
        def parse():
            try:
                return {'playlists': service().import_file(content, file.filename or 'playlist.xml')}
            except (ValueError, SyntaxError):
                raise HTTPException(400, 'Invalid playlist or an updated import is already resolving') from None
        return await run_in_threadpool(parse)

    @router.get('/playlists/{key}')
    def playlist(key: str):
        return invoke(lambda: service().refresh_library(key))

    @router.post('/playlists/{key}/resolve')
    def resolve(key: str):
        svc = service()
        invoke(lambda: svc.playlist(key))
        return {'job_id': svc.store.schedule('resolve:'+key, 'resolve', {'playlist_id': key}, repeat=True)}

    @router.post('/playlists/{key}/choose')
    def choose(key: str, payload: dict = Body(...)):
        return invoke(lambda: service().choose(key, int(payload.get('position', -1)), payload.get('recording_mbid')))

    @router.post('/playlists/{key}/acquire')
    def acquire(key: str, payload: dict = Body(default={})):
        positions = payload.get('positions')
        if positions is not None and (not isinstance(positions, list) or not all(isinstance(p, int) for p in positions)):
            raise HTTPException(400, 'positions must be a list of integers')
        return invoke(lambda: service().acquire(key, positions))

    def jellyfin():
        return JellyfinClient((config_getter().get('arr') or {}).get('jellyfin') or {})

    @router.post('/jellyfin/test')
    def test_jellyfin():
        return invoke(lambda: jellyfin().test())

    @router.post('/playlists/{key}/jellyfin')
    def sync_jellyfin(key: str, payload: dict = Body(default={})):
        svc = service()
        return invoke(lambda: synchronize(svc.store, svc, key, jellyfin(), preview=payload.get('preview', True) is not False))

    @router.post('/artists/search')
    def artist_search(payload: dict = Body(...)):
        query = str(payload.get('query') or '').strip()
        if not query or len(query) > 200:
            raise HTTPException(400, 'An artist name of 1–200 characters is required')
        return {'artists': service().mb.discover_artists(query, limit=10)}

    @router.post('/preview')
    def preview(payload: dict = Body(...)):
        provider = PROVIDERS.get(payload.get('provider', 'musicbrainz_genre'))
        query = str(payload.get('query') or '').strip()
        if not provider or not query or len(query) > 200:
            raise HTTPException(400, 'Choose a provider and enter a query of 1–200 characters')
        return invoke(lambda: {'artists': provider.preview(service().mb, query, min(50, max(1, int(payload.get('limit', 20))))), 'dry_run': True})

    @router.post('/subscriptions')
    def subscribe(payload: dict = Body(...)):
        return invoke(lambda: {'artist_mbid': service().subscribe(payload.get('artist_mbid'), payload.get('name', ''), payload.get('policy'))})

    @router.post('/subscriptions/{artist_id}')
    def change_subscription(artist_id: str, payload: dict = Body(...)):
        svc = service()
        if not isinstance(payload.get('enabled'), bool):
            raise HTTPException(400, 'enabled must be true or false')
        svc.store.execute('UPDATE artist_subscriptions SET enabled=? WHERE artist_mbid=?', (int(payload['enabled']), artist_id))
        return {'updated': True}

    @router.delete('/subscriptions/{artist_id}')
    def remove_subscription(artist_id: str):
        with service().store.connect() as conn:
            conn.execute('DELETE FROM artist_subscriptions WHERE artist_mbid=?', (artist_id,))
            conn.execute('DELETE FROM subscription_releases WHERE artist_mbid=?', (artist_id,))
        return {'removed': True}

    @router.post('/subscriptions/{artist_id}/refresh')
    def refresh(artist_id: str):
        return {'job_id': service().store.schedule('subscription:'+artist_id, 'subscription', {'artist_mbid': artist_id}, repeat=True)}

    @router.post('/subscriptions/{artist_id}/preview')
    def preview_releases(artist_id: str, payload: dict = Body(...)):
        from engine.discovery_identity import mbid
        import hashlib
        groups = payload.get('release_group_mbids')
        if not isinstance(groups, list) or not groups or len(groups) > 25 or not all(mbid(g) for g in groups):
            raise HTTPException(400, 'Select 1–25 release groups')
        groups = sorted(set(groups))
        key = 'discography:'+hashlib.sha256((artist_id+':'+','.join(groups)).encode()).hexdigest()[:32]
        return {'job_id': service().store.schedule(key, 'discography', {'artist_mbid': artist_id, 'groups': groups}, repeat=True)}

    @router.post('/subscriptions/{artist_id}/releases/{group_id}')
    def expand(artist_id: str, group_id: str):
        return invoke(lambda: service().expand_release(artist_id, group_id))

    @router.post('/playlists/{key}/builder')
    def seed_builder(key: str):
        svc = service()
        worker = DiscoveryWorker(svc, app.state.search_service)
        return invoke(lambda: worker.seed(key))

    return router
