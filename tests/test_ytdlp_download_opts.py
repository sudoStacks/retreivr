import unittest
from unittest.mock import patch
import sys
import types

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

from engine.job_queue import build_ytdlp_opts, _enforce_video_codec_container_rules


class YtdlpDownloadOptsTests(unittest.TestCase):
    def test_download_opts_no_suppressors(self):
        context = {
            "operation": "download",
            "audio_mode": False,
            "final_format": None,
            "audio_only": False,
            "config": {},
            "overrides": {},
        }
        opts = build_ytdlp_opts(context)
        for key in ("download", "skip_download", "extract_flat", "simulate"):
            self.assertNotIn(key, opts)

    def test_download_opts_dropped_keys_warning(self):
        context = {
            "operation": "download",
            "audio_mode": False,
            "final_format": None,
            "audio_only": False,
            "config": {},
            "overrides": {
                "skip_download": True,
                "extract_flat": True,
                "socket_timeout": 10,
            },
        }
        with self.assertLogs(level="WARNING") as logs:
            opts = build_ytdlp_opts(context)
        self.assertTrue(
            any("Dropping unsafe yt_dlp_opts for download" in msg for msg in logs.output)
        )
        for key in ("download", "skip_download", "extract_flat"):
            self.assertNotIn(key, opts)
        self.assertEqual(opts.get("socket_timeout"), 10)

    def test_video_mp4_target_sets_postprocess_conversion(self):
        context = {
            "operation": "download",
            "audio_mode": False,
            "media_type": "video",
            "media_intent": "episode",
            "final_format": "mp4",
            "audio_only": False,
            "config": {},
            "overrides": {},
        }
        opts = build_ytdlp_opts(context)
        self.assertEqual(opts.get("merge_output_format"), "mp4")
        self.assertEqual(opts.get("recodevideo"), "mp4")
        self.assertNotIn("vcodec^=avc1", str(opts.get("format") or ""))

    def test_video_mkv_and_mp4_targets_share_same_download_selector(self):
        mkv_context = {
            "operation": "download",
            "audio_mode": False,
            "media_type": "video",
            "media_intent": "episode",
            "final_format": "mkv",
            "audio_only": False,
            "config": {},
            "overrides": {},
        }
        mp4_context = dict(mkv_context, final_format="mp4")
        mkv_opts = build_ytdlp_opts(mkv_context)
        mp4_opts = build_ytdlp_opts(mp4_context)
        self.assertEqual(mkv_opts.get("format"), mp4_opts.get("format"))
        self.assertIsNone(mkv_opts.get("recodevideo"))
        self.assertEqual(mp4_opts.get("recodevideo"), "mp4")

    def test_mp4_target_forces_aac_when_probe_reports_opus(self):
        with patch(
            "engine.job_queue._probe_media_profile",
            side_effect=[
                {
                    "final_container": "mp4",
                    "final_video_codec": "h264",
                    "final_audio_codec": "opus",
                },
                {
                    "final_container": "mp4",
                    "final_video_codec": "h264",
                    "final_audio_codec": "aac",
                },
            ],
        ), patch("engine.job_queue.subprocess.run") as mock_run, patch("engine.job_queue.os.replace"):
            _path, profile = _enforce_video_codec_container_rules(
                "/tmp/source.mp4",
                target_container="mp4",
            )

        self.assertEqual(profile.get("final_audio_codec"), "aac")
        self.assertEqual(mock_run.call_count, 1)
        ffmpeg_args = mock_run.call_args[0][0]
        self.assertIn("-c:a", ffmpeg_args)
        self.assertIn("aac", ffmpeg_args)

    def test_mkv_target_preserves_opus_without_transcode(self):
        with patch(
            "engine.job_queue._probe_media_profile",
            return_value={
                "final_container": "matroska",
                "final_video_codec": "h264",
                "final_audio_codec": "opus",
            },
        ), patch("engine.job_queue.subprocess.run") as mock_run:
            _path, profile = _enforce_video_codec_container_rules(
                "/tmp/source.mkv",
                target_container="mkv",
            )

        self.assertEqual(profile.get("final_audio_codec"), "opus")
        self.assertEqual(mock_run.call_count, 0)
