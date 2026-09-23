"""Release-scoped evidence alongside the existing community cache's recording records."""
import json
import re
import time
from engine.discovery_identity import mbid


class EvidenceCache:
    def __init__(self, store, *, max_age_days=30, min_confidence=.94):
        self.store = store
        self.max_age = max_age_days*86400
        self.minimum = min_confidence

    def lookup(self, identity):
        rows = self.store.rows('SELECT * FROM resolution_evidence WHERE recording_mbid=? AND release_mbid=? AND status=? AND confidence>=? AND verified_at>=? ORDER BY confidence DESC,verified_at DESC',
            (identity['recording_mbid'], identity.get('release_mbid') or '', 'matched', self.minimum, time.time()-self.max_age))
        self.store.metric('cache_hit' if rows else 'cache_miss')
        return json.loads(rows[0]['payload']) if rows else None

    def put(self, identity, candidate):
        score = float(candidate.get('final_score') or candidate.get('confidence') or 0)
        if score > 1:
            score /= 100
        source_id = str(candidate.get('video_id') or candidate.get('source_id') or '')
        if not re.fullmatch(r'[\w-]{11}', source_id):
            match = re.search(r'(?:v=|youtu\.be/)([\w-]{11})(?:&|$)', candidate.get('url') or '')
            source_id = match.group(1) if match else ''
        if not mbid(identity.get('recording_mbid')) or not source_id or score < self.minimum or candidate.get('rejection_reason'):
            return False
        payload = {'identity': identity, 'video_id': source_id, 'url': 'https://www.youtube.com/watch?v='+source_id,
                   'source': candidate.get('source') or 'youtube', 'confidence': score,
                   'evidence': candidate.get('score_breakdown') or {}, 'resolver': 'retreivr-search-v1',
                   'verification_status': 'metadata_matched', 'media_verified': False}
        now = time.time()
        self.store.execute('''INSERT INTO resolution_evidence VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(recording_mbid,release_mbid,source_id) DO UPDATE SET
            payload=excluded.payload,confidence=excluded.confidence,status=excluded.status,verified_at=excluded.verified_at
            WHERE excluded.confidence>=resolution_evidence.confidence OR resolution_evidence.status='dead' OR resolution_evidence.verified_at<? ''',
            (identity['recording_mbid'], identity.get('release_mbid') or '', source_id, json.dumps(payload), score, 'matched', now, now, now-self.max_age))
        return True

    def invalidate(self, identity, source_id):
        self.store.execute("UPDATE resolution_evidence SET status='dead' WHERE recording_mbid=? AND release_mbid=? AND source_id=?", (identity['recording_mbid'], identity.get('release_mbid') or '', source_id))
