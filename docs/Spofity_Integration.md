Spotify Integration (2026) – Architecture, Requirements, and Configuration

Overview

Retreivr integrates with Spotify for two primary purposes:
	1.	Retrieve track lists from Spotify playlists, albums, and user libraries.
	2.	Retrieve metadata (artist, album, track name, etc.) to enrich downloaded media.

As of February 2026, Spotify’s Web API access rules significantly impact how this integration works. This document explains:
	•	What works without OAuth
	•	What requires OAuth
	•	What may require Spotify Premium
	•	How Retreivr is wired
	•	How to configure it correctly

⸻

1️⃣ Spotify Web API Reality (Feb 2026)

Spotify introduced major restrictions to the Web API.
https://developer.spotify.com/documentation/web-api/references/changes/february-2026

Public Metadata (No OAuth Required)

These endpoints still work using Client Credentials (App-only token):
	•	GET /albums/{id}
	•	GET /tracks/{id}
	•	GET /artists/{id}
	•	GET /search
	•	GET /playlists/{id} (metadata only, not track list)

What this means:
	•	You can fetch album metadata.
	•	You can fetch track metadata.
	•	You can fetch playlist metadata (name, owner, description).
	•	You CANNOT fetch full playlist track lists using client credentials.

⸻

Playlist Track Lists (OAuth Required)

To retrieve playlist tracks:
GET /playlists/{id}/items

You must use:
	•	OAuth user token
	•	Correct scopes
	•	App must not be restricted

Required Scopes

For public and private playlists:
playlist-read-private
playlist-read-collaborative

For user libraries (optional features):
user-library-read


⸻

Premium Requirement Confusion

Spotify’s dashboard may show:

“Your application is blocked from accessing the Web API since you do not have a Spotify Premium subscription.”

In Development Mode, this restriction typically applies only to:
	•	Player endpoints
	•	Playback control
	•	Some advanced personal endpoints

For Retreivr’s purposes (reading playlists and metadata):
	•	Premium is NOT required
	•	OAuth is required for playlist track lists
	•	Public metadata works without OAuth

If your app is:
	•	In Development Mode
	•	Using Web API only
	•	Not using playback endpoints

It should function correctly after OAuth approval.

⸻

2️⃣ What Retreivr Uses Spotify For

Retreivr uses Spotify in two modes:

⸻

A) Public Metadata Mode (No OAuth Required)

Used for:
	•	Metadata enrichment during music downloads
	•	Album structure creation
	•	Search resolution
	•	Validation

Works with:
	•	Client ID
	•	Client Secret

No OAuth required.

⸻

B) Playlist / Library Sync Mode (OAuth Required)

Used for:
	•	Scheduled Spotify playlist polling
	•	Liked Songs sync
	•	Saved Albums sync
	•	User Playlists sync

Requires:
	•	OAuth user token
	•	Valid scopes
	•	Working redirect URI

⸻

3️⃣ Retreivr Spotify Modes

Mode 1 – Metadata Only

You provide:
"spotify": {
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET"
}

Retreivr can:
	•	Fetch album metadata
	•	Fetch track metadata
	•	Use Spotify search
	•	Structure music folders properly

No OAuth required.

⸻

Mode 2 – Playlist Sync

You provide:
"spotify": {
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET",
  "redirect_uri": "http://127.0.0.1:8090/api/spotify/oauth/callback",
  "sync_user_playlists": true,
  "watch_playlists": [
    "0oy3UMfOAENX9X7haGdRRv",
    "https://open.spotify.com/playlist/5EZkoiqOms6HvUGPd0vMxy"
  ]
}

Then:
	1.	Click Connect Spotify in Config page
	2.	Complete OAuth authorization
	3.	Retreivr stores token
	4.	Scheduler begins polling playlists

⸻

4️⃣ How Playlist Polling Works in Retreivr
	1.	Scheduler tick fires
	2.	Spotify OAuth token validated
	3.	Playlist IDs normalized
	4.	/playlists/{id}/items called
	5.	Track list diffed against snapshot
	6.	New tracks enqueued
	7.	M3U rebuilt (best-effort)
	8.	Completion logged

If OAuth is invalid:
	•	Playlist sync fails
	•	Metadata-only mode still works

⸻

5️⃣ Development Mode Requirements

Spotify app settings must include:

APIs Used
	•	✅ Web API

Redirect URI

Must exactly match config:
http://127.0.0.1:8090/api/spotify/oauth/callback

App Status
Development Mode

This is acceptable.

You must re-authorize after:
	•	Changing scopes
	•	Changing redirect URI
	•	Resetting token store

⸻

6️⃣ What Does NOT Require Premium

Feature
Premium Required?
Album metadata
❌
Track metadata
❌
Public playlist metadata
❌
Public playlist track list (via OAuth)
❌ (in Dev Mode)
Liked Songs
❌
Saved Albums
❌


mium is required primarily for:
	•	Playback endpoints
	•	Player control APIs

Retreivr does not use these.

⸻

7️⃣ Recommended Configuration Patterns

Minimal (Metadata Only)
"spotify": {
  "client_id": "xxx",
  "client_secret": "xxx"
}

Playlist Sync (Public Playlists)
"spotify": {
  "client_id": "xxx",
  "client_secret": "xxx",
  "redirect_uri": "http://127.0.0.1:8090/api/spotify/oauth/callback",
  "sync_user_playlists": true,
  "watch_playlists": [
    "PLAYLIST_ID",
    "https://open.spotify.com/playlist/PLAYLIST_ID"
  ],
  "user_playlists_sync_interval_minutes": 15
}


⸻

8️⃣ Important Notes
	•	Downtime window does NOT block manual runs (by design).
	•	Scheduler respects downtime.
	•	OAuth failures log explicitly.
	•	Playlist track fetching requires OAuth — not client credentials.
	•	Metadata enrichment works independently of playlist polling.

⸻

9️⃣ Summary

As of 2026:
	•	OAuth is required for playlist track lists.
	•	Client credentials are sufficient for metadata.
	•	Premium is NOT required for Retreivr’s use case.
	•	Development Mode is acceptable.
	•	Redirect URI must match exactly.
	•	Proper scopes must be requested.

Retreivr supports both:
	•	Lightweight metadata-only mode
	•	Full playlist synchronization mode

Depending on user configuration
