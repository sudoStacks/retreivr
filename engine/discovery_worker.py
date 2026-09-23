"""One bounded persistent worker. Resolver-only jobs never call acquisition methods."""
import hashlib
import json
import logging
import threading
import time
from engine.resolution_evidence import EvidenceCache

logger = logging.getLogger(__name__)


class DiscoveryWorker:
    def __init__(self, service, resolver, *, interval=10, max_attempts=3):
        self.service, self.store, self.resolver = service, service.store, resolver
        self.interval = max(2, float(interval))
        self.max_attempts = max(1, min(5, int(max_attempts)))
        self.stop = threading.Event()
        self.cache = EvidenceCache(self.store)

    def seed(self, key):
        count = 0
        for row in self.service.playlist(key)['tracks']:
            identity = row['identity']
            if not identity:
                continue
            identity_key = identity['recording_mbid']+':'+str(identity.get('release_mbid') or '')
            job_key = 'builder:'+hashlib.sha256(identity_key.encode()).hexdigest()
            # Terminal failures stay terminal; successful old records can be explicitly reseeded.
            self.store.schedule(job_key, 'builder', identity)
            self.store.execute("UPDATE discovery_jobs SET state='pending',attempts=0,next_run=0 WHERE id=? AND state='done' AND updated_at<?", (job_key, time.time()-self.cache.max_age))
            count += 1
        return {'seeded': count}

    def run_once(self):
        job = self.store.claim()
        if not job:
            return False
        logger.info('discovery_job_started kind=%s id=%s attempt=%s', job['kind'], job['id'], job['attempts'])
        lease_stop = threading.Event()
        def heartbeat():
            while not lease_stop.wait(30):
                self.store.execute("UPDATE discovery_jobs SET lease_until=? WHERE id=? AND state='running'", (time.time()+120, job['id']))
        lease = threading.Thread(target=heartbeat, daemon=True)
        lease.start()
        try:
            payload = json.loads(job['payload'])
            if job['kind'] == 'resolve':
                result = self.service.resolve_playlist(payload['playlist_id'], self.stop)
                if payload.get('seed_builder'):
                    self.seed(payload['playlist_id'])
            elif job['kind'] == 'subscription':
                result = self.service.refresh_artist(payload['artist_mbid'], self.stop)
                if payload.get('seed_builder') and not result.get('skipped'):
                    for release in self.store.rows('SELECT release_group_mbid FROM subscription_releases WHERE artist_mbid=?', (payload['artist_mbid'],)):
                        if self.stop.is_set():
                            raise InterruptedError()
                        playlist = self.service.expand_release(payload['artist_mbid'], release['release_group_mbid'])
                        self.seed(playlist['id'])
            elif job['kind'] == 'discography':
                playlist = self.service.preview_discography(payload['artist_mbid'], payload['groups'], self.stop)
                result = {'playlist_id': playlist['id'], 'tracks': len(playlist['tracks'])}
            elif job['kind'] == 'builder':
                existing = self.cache.lookup(payload)
                if existing:
                    result = {'cache_hit': True}
                else:
                    candidate = self.resolver.search_music_track_best_match(payload['artist'], payload['title'],
                        album=payload.get('album'), duration_ms=payload.get('duration_ms'), recording_mbid=payload['recording_mbid'], release_mbid=payload.get('release_mbid'))
                    if not candidate or not self.cache.put(payload, candidate):
                        raise ValueError('No high-confidence source')
                    result = {'cached': True, 'downloaded': False}
            else:
                raise ValueError('Unknown discovery job kind')
            self.store.execute("UPDATE discovery_jobs SET state='done',result=?,error=NULL,updated_at=?,lease_until=0 WHERE id=?", (json.dumps(result), time.time(), job['id']))
            logger.info('discovery_job_completed kind=%s id=%s', job['kind'], job['id'])
        except InterruptedError:
            self.store.execute("UPDATE discovery_jobs SET state='pending',lease_until=0 WHERE id=?", (job['id'],))
        except Exception as exc:
            # Exception type only: provider exception messages can contain credential-bearing URLs.
            state = 'failed' if job['attempts'] >= self.max_attempts else 'pending'
            logger.warning('discovery_job_retry kind=%s id=%s state=%s error_type=%s', job['kind'], job['id'], state, type(exc).__name__)
            self.store.execute('UPDATE discovery_jobs SET state=?,error=?,next_run=?,lease_until=0,updated_at=? WHERE id=?',
                               (state, type(exc).__name__, time.time()+min(3600, 30*2**job['attempts']), time.time(), job['id']))
            if job['kind'] == 'subscription':
                self.store.execute('UPDATE artist_subscriptions SET error=? WHERE artist_mbid=?', (type(exc).__name__, json.loads(job['payload'])['artist_mbid']))
        finally:
            lease_stop.set()
            lease.join(timeout=1)
        return True

    def run(self):
        while not self.stop.is_set():
            self.run_once()
            self.stop.wait(self.interval)
