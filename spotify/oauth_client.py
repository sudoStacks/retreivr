"""Spotify OAuth client helpers."""

from __future__ import annotations

import os
import time
from urllib.parse import urlencode

import requests

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"


def build_auth_url(client_id: str, redirect_uri: str, scope: str, state: str) -> str:
    """Build Spotify authorization URL."""
    params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state,
    }
    return f"{SPOTIFY_AUTH_URL}?{urlencode(params)}"


def refresh_access_token(
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> dict:
    """Exchange refresh token for a new Spotify access token payload.

    Returns:
        Parsed JSON token response from Spotify.

    Raises:
        Exception: When request fails or response code is non-200.
    """
    response = requests.post(
        SPOTIFY_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=20,
    )
    if response.status_code != 200:
        detail = (response.text or "").strip() or f"status={response.status_code}"
        raise Exception(f"spotify refresh failed: {detail}")
    return response.json()
