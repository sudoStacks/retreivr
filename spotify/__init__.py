"""Spotify integration modules."""

from spotify.client import SpotifyPlaylistClient
from spotify.diff import diff_playlist

__all__ = ["SpotifyPlaylistClient", "diff_playlist"]
