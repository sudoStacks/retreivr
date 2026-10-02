import sys
import types
import unittest

if "google" not in sys.modules:
    sys.modules["google"] = types.ModuleType("google")
if "google.auth" not in sys.modules:
    sys.modules["google.auth"] = types.ModuleType("google.auth")
if "google.auth.exceptions" not in sys.modules:
    google_auth_exc_mod = types.ModuleType("google.auth.exceptions")
    google_auth_exc_mod.RefreshError = Exception
    sys.modules["google.auth.exceptions"] = google_auth_exc_mod
if "google.auth.transport" not in sys.modules:
    sys.modules["google.auth.transport"] = types.ModuleType("google.auth.transport")
if "google.auth.transport.requests" not in sys.modules:
    google_auth_transport_requests = types.ModuleType("google.auth.transport.requests")
    google_auth_transport_requests.Request = object
    sys.modules["google.auth.transport.requests"] = google_auth_transport_requests
if "google.oauth2" not in sys.modules:
    sys.modules["google.oauth2"] = types.ModuleType("google.oauth2")
if "google.oauth2.credentials" not in sys.modules:
    google_oauth2_credentials = types.ModuleType("google.oauth2.credentials")
    google_oauth2_credentials.Credentials = object
    sys.modules["google.oauth2.credentials"] = google_oauth2_credentials
if "googleapiclient" not in sys.modules:
    sys.modules["googleapiclient"] = types.ModuleType("googleapiclient")
if "googleapiclient.discovery" not in sys.modules:
    googleapiclient_discovery = types.ModuleType("googleapiclient.discovery")
    googleapiclient_discovery.build = lambda *args, **kwargs: None
    sys.modules["googleapiclient.discovery"] = googleapiclient_discovery
if "googleapiclient.errors" not in sys.modules:
    googleapiclient_errors = types.ModuleType("googleapiclient.errors")
    googleapiclient_errors.HttpError = Exception
    sys.modules["googleapiclient.errors"] = googleapiclient_errors
if "rapidfuzz" not in sys.modules:
    rapidfuzz_mod = types.ModuleType("rapidfuzz")
    rapidfuzz_mod.fuzz = types.SimpleNamespace(ratio=lambda *_args, **_kwargs: 0)
    sys.modules["rapidfuzz"] = rapidfuzz_mod
if "metadata.queue" not in sys.modules:
    metadata_queue_mod = types.ModuleType("metadata.queue")
    metadata_queue_mod.enqueue_metadata = lambda *_args, **_kwargs: None
    sys.modules["metadata.queue"] = metadata_queue_mod
if "musicbrainzngs" not in sys.modules:
    sys.modules["musicbrainzngs"] = types.ModuleType("musicbrainzngs")

from engine.job_queue import resolve_source


class ResolveSourceTests(unittest.TestCase):
    def test_empty_url_is_unknown(self):
        self.assertEqual(resolve_source(""), "unknown")
        self.assertEqual(resolve_source(None), "unknown")

    def test_youtube_watch_url(self):
        self.assertEqual(resolve_source("https://www.youtube.com/watch?v=abc123"), "youtube")

    def test_youtube_short_url(self):
        self.assertEqual(resolve_source("https://youtu.be/abc123"), "youtube")

    def test_youtube_music_url(self):
        self.assertEqual(resolve_source("https://music.youtube.com/watch?v=abc123"), "youtube_music")

    def test_facebook_watch_url(self):
        self.assertEqual(resolve_source("https://www.facebook.com/watch/?v=1101200022274142"), "facebook")

    def test_facebook_share_reel_url(self):
        # The share-link shape (as opposed to /watch/ or /<page>/videos/<id>/)
        # was previously falling through to "unknown", which the web UI then
        # rendered as "Open in Unknown" / "Source: Unknown".
        self.assertEqual(resolve_source("https://www.facebook.com/share/r/19MtnLyi2J/"), "facebook")

    def test_facebook_page_video_url(self):
        self.assertEqual(
            resolve_source("https://www.facebook.com/hwccliverpool/videos/692502946151290/"), "facebook"
        )

    def test_fb_watch_short_domain(self):
        self.assertEqual(resolve_source("https://fb.watch/abc123/"), "facebook")

    def test_unrecognized_domain_is_unknown(self):
        self.assertEqual(resolve_source("https://example.com/video/1"), "unknown")


if __name__ == "__main__":
    unittest.main()
