"""Additive discovery persistence; acquisition remains in DownloadJobStore."""
from contextlib import contextmanager
import json
import sqlite3
import time


def ensure_discovery_tables(conn):
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS discovery_playlists (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, source TEXT NOT NULL,
            fingerprint TEXT NOT NULL, updated_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS discovery_tracks (
            playlist_id TEXT NOT NULL, position INTEGER NOT NULL, intent TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending', candidates TEXT NOT NULL DEFAULT '[]',
            identity TEXT, job_id TEXT, PRIMARY KEY(playlist_id,position)
        );
        CREATE TABLE IF NOT EXISTS artist_subscriptions (
            artist_mbid TEXT PRIMARY KEY, name TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
            policy TEXT NOT NULL, last_checked REAL, error TEXT
        );
        CREATE TABLE IF NOT EXISTS subscription_releases (
            artist_mbid TEXT NOT NULL, release_group_mbid TEXT NOT NULL,
            payload TEXT NOT NULL, playlist_id TEXT,
            PRIMARY KEY(artist_mbid,release_group_mbid)
        );
        CREATE TABLE IF NOT EXISTS discovery_jobs (
            id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
            next_run REAL NOT NULL DEFAULT 0, lease_until REAL NOT NULL DEFAULT 0,
            result TEXT, error TEXT, updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS discovery_jobs_ready ON discovery_jobs(state,next_run);
        CREATE TABLE IF NOT EXISTS jellyfin_playlist_links (
            playlist_id TEXT NOT NULL, server TEXT NOT NULL, user_id TEXT NOT NULL,
            remote_id TEXT, remote_name TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'new',
            PRIMARY KEY(playlist_id,server,user_id)
        );
        CREATE TABLE IF NOT EXISTS resolution_evidence (
            recording_mbid TEXT NOT NULL, release_mbid TEXT NOT NULL DEFAULT '',
            source_id TEXT NOT NULL, payload TEXT NOT NULL, confidence REAL NOT NULL,
            status TEXT NOT NULL, created_at REAL NOT NULL, verified_at REAL NOT NULL,
            PRIMARY KEY(recording_mbid,release_mbid,source_id)
        );
        CREATE TABLE IF NOT EXISTS discovery_metrics (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
    ''')
    conn.commit()


class DiscoveryStore:
    def __init__(self, path):
        self.path = str(path)
        with self.connect() as conn:
            ensure_discovery_tables(conn)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def rows(self, sql, args=()):
        with self.connect() as conn:
            return [dict(row) for row in conn.execute(sql, args)]

    def execute(self, sql, args=()):
        with self.connect() as conn:
            return conn.execute(sql, args).rowcount

    def metric(self, name):
        self.execute('INSERT INTO discovery_metrics VALUES (?,1) ON CONFLICT(name) DO UPDATE SET value=value+1', (name,))

    def schedule(self, key, kind, payload, *, repeat=False):
        with self.connect() as conn:
            conn.execute('INSERT OR IGNORE INTO discovery_jobs(id,kind,payload,updated_at) VALUES (?,?,?,?)',
                         (key, kind, json.dumps(payload), time.time()))
            if repeat:
                conn.execute("UPDATE discovery_jobs SET state='pending', attempts=0, next_run=0, payload=?, error=NULL WHERE id=? AND state IN ('done','failed')",
                             (json.dumps(payload), key))
        return key

    def claim(self):
        now = time.time()
        with self.connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            row = conn.execute("SELECT * FROM discovery_jobs WHERE (state='pending' AND next_run<=?) OR (state='running' AND lease_until<?) ORDER BY updated_at LIMIT 1", (now, now)).fetchone()
            if row is None:
                return None
            conn.execute("UPDATE discovery_jobs SET state='running',attempts=attempts+1,lease_until=?,updated_at=? WHERE id=?", (now+120, now, row['id']))
            return {**dict(row), 'attempts': row['attempts']+1}
