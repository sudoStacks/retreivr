#!/usr/bin/env python3
"""Dedicated resolver-only process. No DownloadWorkerEngine or acquisition queue."""
import argparse
import signal
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from engine.core import load_config, validate_config
from engine.discovery import DiscoveryService
from engine.discovery_store import DiscoveryStore
from engine.discovery_worker import DiscoveryWorker
from engine.search_engine import SearchResolutionService
from metadata.services.musicbrainz_service import get_musicbrainz_service


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--db', required=True, help='Dedicated discovery SQLite database')
    parser.add_argument('--import-file', help='Optional playlist to import and resolve before seeding source jobs')
    parser.add_argument('--artist-mbid', help='Optional canonical artist to refresh and expand')
    parser.add_argument('--artist-name', default='Subscribed artist')
    parser.add_argument('--once', action='store_true', help='Run one persisted job and exit')
    args = parser.parse_args()
    config = load_config(args.config)
    errors = validate_config(config)
    if errors:
        parser.error('; '.join(errors))
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    service = DiscoveryService(DiscoveryStore(args.db), get_musicbrainz_service(), config=config)
    resolver = SearchResolutionService(search_db_path=args.db, queue_db_path=args.db, config=config, resolution_only=True)
    settings = config.get('music_discovery') or {}
    worker = DiscoveryWorker(service, resolver, interval=settings.get('interval_seconds', 10), max_attempts=settings.get('max_attempts', 3))
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: worker.stop.set())
    if args.import_file:
        path = Path(args.import_file)
        if path.stat().st_size > 10*1024*1024:
            parser.error('Playlist exceeds 10 MiB')
        for playlist in service.import_file(path.read_bytes(), path.name):
            service.store.schedule('resolve:'+playlist['id'], 'resolve', {'playlist_id': playlist['id'], 'seed_builder': True}, repeat=True)
    if args.artist_mbid:
        service.subscribe(args.artist_mbid, args.artist_name)
        service.store.schedule('subscription:'+args.artist_mbid, 'subscription', {'artist_mbid': args.artist_mbid, 'seed_builder': True}, repeat=True)
    if args.once:
        worker.run_once()
    else:
        worker.run()


if __name__ == '__main__':
    main()
