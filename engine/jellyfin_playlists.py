"""Jellyfin 10.10 API playlist integration, using item IDs and entry IDs only."""
import hashlib
from urllib.parse import urlsplit
import requests
from engine.discovery_identity import match_jellyfin, mbid


class JellyfinError(RuntimeError):
    pass


class JellyfinClient:
    def __init__(self, config, session=None):
        self.url = str(config.get('base_url') or '').rstrip('/')
        parsed = urlsplit(self.url)
        if parsed.scheme not in {'http', 'https'} or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError('Jellyfin requires an HTTP(S) server URL without credentials or query parameters')
        self.user = str(config.get('user_id') or '')
        token = config.get('access_token') or config.get('api_key')
        if not self.user or not token:
            raise ValueError('Jellyfin API key and playlist owner user_id are required')
        self.library = str(config.get('library_id') or '')
        if not mbid(self.user) or (self.library and not mbid(self.library)):
            raise ValueError('Jellyfin user_id and optional library_id must be UUIDs')
        self.session = session or requests.Session()
        self.headers = {'X-Emby-Token': token}

    def request(self, method, path, **kwargs):
        # No URLs, response bodies, headers or upstream exception text reach logs/errors.
        try:
            response = self.session.request(method, self.url+path, headers=self.headers, timeout=(5, 30), allow_redirects=False, **kwargs)
            if not 200 <= response.status_code < 300:
                raise JellyfinError(f'Jellyfin request failed (HTTP {response.status_code})')
            return response.json() if response.content else {}
        except (requests.RequestException, ValueError):
            raise JellyfinError('Jellyfin unavailable or returned invalid JSON') from None

    def require_owner(self):
        try:
            owner = self.request('GET', '/Users/Me')
        except JellyfinError:
            raise JellyfinError('Playlist sync requires the owner user access token; configure Jellyfin access_token and verify the connection') from None
        if mbid(owner.get('Id')) != mbid(self.user):
            raise JellyfinError('Jellyfin access token does not belong to the configured playlist owner')

    def test(self):
        info = self.request('GET', '/System/Info/Public')
        self.require_owner()
        return {'connected': True, 'version': info.get('Version'), 'playlist_owner_verified': True}

    def paged(self, path, params):
        rows, start = [], 0
        while True:
            payload = self.request('GET', path, params={**params, 'StartIndex': start, 'Limit': 500})
            page = payload.get('Items', [])
            rows.extend(page)
            start += len(page)
            if not page or start >= int(payload.get('TotalRecordCount', start)):
                return rows
            if start >= 200000:
                raise JellyfinError('Jellyfin library exceeds the configured safe scan bound')

    def audio_items(self):
        params = {'UserId': self.user, 'Recursive': True, 'IncludeItemTypes': 'Audio', 'Fields': 'ProviderIds', 'EnableUserData': False}
        if self.library:
            params['ParentId'] = self.library
        return self.paged('/Items', params)

    def playlist_items(self, remote_id):
        return self.paged('/Playlists/'+remote_id+'/Items', {'UserId': self.user})


def synchronize(store, service, key, client, *, preview=True):
    playlist = service.playlist(key)
    items = client.audio_items()
    desired, report = [], []
    for track in playlist['tracks']:
        identity = track['identity'] or track['intent']
        state, item_id = match_jellyfin(identity, items)
        report.append({'position': track['position'], 'title': identity.get('title'), 'state': state, 'item_id': item_id})
        if item_id:
            desired.append(item_id)  # Count source occurrences before applying Jellyfin's limit.
    source_repetitions = len(desired)-len(set(desired))
    # Jellyfin 10.10 PlaylistManager explicitly applies Distinct(); retain source
    # repetitions in Retreivr, report this API limitation, and synchronize first occurrences.
    desired = list(dict.fromkeys(desired))
    result = {'tracks': report, 'matched': sum(r['state'] == 'matched' for r in report),
              'jellyfin_entries': len(desired), 'source_repetitions': source_repetitions, 'unmatched': sum(r['state'] == 'unmatched' for r in report),
              'ambiguous': sum(r['state'] == 'ambiguous' for r in report), 'preview': preview}
    if preview:
        return result
    client.require_owner()
    link_key = (key, client.url, client.user)
    # Reserve a durable, deterministic name before create. A lost response is recovered by name,
    # never by issuing an unconditional second create. Only this managed playlist is modified.
    remote_name = playlist['name']+' [Retreivr '+hashlib.sha256(key.encode()).hexdigest()[:8]+']'
    with store.connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('INSERT OR IGNORE INTO jellyfin_playlist_links(playlist_id,server,user_id,remote_name) VALUES (?,?,?,?)', (*link_key, remote_name))
        link = dict(conn.execute('SELECT * FROM jellyfin_playlist_links WHERE playlist_id=? AND server=? AND user_id=?', link_key).fetchone())
        if link['state'] in {'syncing', 'creating'}:
            raise JellyfinError('A sync is already in progress; wait for it to finish')
        conn.execute("UPDATE jellyfin_playlist_links SET state='syncing' WHERE playlist_id=? AND server=? AND user_id=?", link_key)
    try:
        remote_id = link['remote_id']
        if not remote_id:
            existing = client.paged('/Items', {'UserId': client.user, 'Recursive': True, 'IncludeItemTypes': 'Playlist', 'SearchTerm': remote_name})
            matches = [i for i in existing if i.get('Name') == remote_name]
            if len(matches) > 1:
                raise JellyfinError('Multiple managed playlists found; reconcile them in Jellyfin before retrying')
            if matches:
                remote_id = matches[0]['Id']
            elif link['state'] in {'uncertain', 'creating'}:
                raise JellyfinError('Previous creation outcome is uncertain; verify in Jellyfin before retrying')
            else:
                store.execute("UPDATE jellyfin_playlist_links SET state='creating' WHERE playlist_id=? AND server=? AND user_id=?", link_key)
                response = client.request('POST', '/Playlists', json={'Name': remote_name, 'Ids': [], 'UserId': client.user, 'MediaType': 'Audio', 'IsPublic': False})
                remote_id = response.get('Id')
                if not remote_id:
                    raise JellyfinError('Jellyfin did not return a playlist ID')
            store.execute('UPDATE jellyfin_playlist_links SET remote_id=? WHERE playlist_id=? AND server=? AND user_id=?', (remote_id, *link_key))
        current = client.playlist_items(remote_id)
        if [i['Id'] for i in current] != desired:
            # Rebuild only membership. Persisted remote ID + read-before-write makes retries repair partial writes.
            entry_ids = [i.get('PlaylistItemId') for i in current]
            if not all(entry_ids):
                raise JellyfinError('Jellyfin omitted playlist entry IDs; refusing an unsafe update')
            for offset in range(0, len(entry_ids), 100):
                client.request('DELETE', '/Playlists/'+remote_id+'/Items', params={'EntryIds': ','.join(entry_ids[offset:offset+100])})
            for offset in range(0, len(desired), 100):
                client.request('POST', '/Playlists/'+remote_id+'/Items', params={'Ids': ','.join(desired[offset:offset+100]), 'UserId': client.user})
            if [i['Id'] for i in client.playlist_items(remote_id)] != desired:
                raise JellyfinError('Jellyfin playlist order/membership verification failed; retry sync')
        result.update(remote_id=remote_id, name=remote_name)
        store.execute("UPDATE jellyfin_playlist_links SET state='ready' WHERE playlist_id=? AND server=? AND user_id=?", link_key)
        return result
    except Exception:
        # A failed read before creation is safely retryable. Only a request that
        # actually crossed the create boundary can have an unknown remote outcome.
        recovery_state = link['state'] if link['state'] not in {'syncing', 'creating'} else 'new'
        store.execute("UPDATE jellyfin_playlist_links SET state=CASE WHEN remote_id IS NOT NULL THEN 'ready' WHEN state='creating' THEN 'uncertain' ELSE ? END WHERE playlist_id=? AND server=? AND user_id=?", (recovery_state, *link_key))
        raise
