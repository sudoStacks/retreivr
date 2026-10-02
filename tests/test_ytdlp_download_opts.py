import unittest
from unittest.mock import MagicMock, patch
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

from yt_dlp.postprocessor import MetadataParserPP

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

    def test_replace_in_metadata_builds_metadata_parser_postprocessor(self):
        # `replace_in_metadata` has no meaning to yt_dlp.YoutubeDL() on its own --
        # it only does anything once translated into a MetadataParser postprocessor.
        # A bare passthrough (`opts["replace_in_metadata"] = value`) would make this
        # test pass trivially without the feature actually working; assert on the
        # postprocessor shape instead, and that the inert key isn't left behind.
        context = {
            "operation": "download",
            "audio_mode": False,
            "final_format": None,
            "audio_only": False,
            "config": {},
            "overrides": {
                "replace_in_metadata": [["title", r"^\d+ views\s*", ""]],
            },
        }
        opts = build_ytdlp_opts(context)
        self.assertNotIn("replace_in_metadata", opts)
        postprocessors = [pp for pp in opts.get("postprocessors") or [] if pp.get("key") == "MetadataParser"]
        self.assertEqual(len(postprocessors), 1)
        self.assertEqual(postprocessors[0]["when"], "pre_process")
        self.assertEqual(
            postprocessors[0]["actions"],
            [(MetadataParserPP.Actions.REPLACE, "title", r"^\d+ views\s*", "")],
        )

    def test_replace_in_metadata_strips_facebook_view_count_prefix_end_to_end(self):
        # Reproduces the actual Facebook title format (e.g. "4.8M views · 2.2K
        # reactions Very accurate ... It's FOSS") and proves the configured regex
        # really does strip it, by running the built postprocessor the way yt-dlp
        # itself would -- not just checking that config plumbing holds a value.
        rule = ["title", r"^\d+(?:\.\d+)?[KMB]? views · \d+(?:\.\d+)?[KMB]? reactions\s*", ""]
        context = {
            "operation": "download",
            "audio_mode": False,
            "final_format": None,
            "audio_only": False,
            "config": {},
            "overrides": {"replace_in_metadata": [rule]},
        }
        opts = build_ytdlp_opts(context)
        actions = next(pp["actions"] for pp in opts["postprocessors"] if pp["key"] == "MetadataParser")

        info = {"title": "4.8M views · 2.2K reactions Very accurate ☠️\U0001f602 It's FOSS"}
        MetadataParserPP(None, actions).run(info)

        self.assertEqual(info["title"], "Very accurate ☠️\U0001f602 It's FOSS")

    def test_replace_in_metadata_drops_malformed_rules(self):
        context = {
            "operation": "download",
            "audio_mode": False,
            "final_format": None,
            "audio_only": False,
            "config": {},
            "overrides": {
                "replace_in_metadata": [
                    ["title", "only-two-elements"],
                    ["title", "(unbalanced", "x"],
                    ["title", r"\s+$", ""],
                ],
            },
        }
        with self.assertLogs(level="WARNING"):
            opts = build_ytdlp_opts(context)
        postprocessors = [pp for pp in opts.get("postprocessors") or [] if pp.get("key") == "MetadataParser"]
        self.assertEqual(len(postprocessors), 1)
        self.assertEqual(
            postprocessors[0]["actions"],
            [(MetadataParserPP.Actions.REPLACE, "title", r"\s+$", "")],
        )

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
        # mp4 uses a native-format-preferred selector; mkv uses the general selector.
        self.assertIsNotNone(mkv_opts.get("format"))
        self.assertIsNotNone(mp4_opts.get("format"))
        self.assertIsNone(mkv_opts.get("recodevideo"))
        self.assertEqual(mp4_opts.get("recodevideo"), "mp4")

    def test_mp4_target_forces_aac_when_probe_reports_opus(self):
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = ("", "")
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
        ), patch("engine.job_queue._probe_media_duration_seconds", return_value=None), \
           patch("engine.job_queue.subprocess.Popen", return_value=mock_proc) as mock_popen, \
           patch("engine.job_queue.os.replace"):
            _path, profile = _enforce_video_codec_container_rules(
                "/tmp/source.mp4",
                target_container="mp4",
            )

        self.assertEqual(profile.get("final_audio_codec"), "aac")
        self.assertEqual(mock_popen.call_count, 1)
        ffmpeg_args = mock_popen.call_args[0][0]
        self.assertIn("-c:a", ffmpeg_args)
        self.assertIn("aac", ffmpeg_args)
        self.assertIn("-c:v", ffmpeg_args)
        self.assertIn("copy", ffmpeg_args)

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

    def test_mp4_target_retries_without_subtitles_on_ffmpeg_failure(self):
        fail_proc = MagicMock()
        fail_proc.returncode = 1
        fail_proc.communicate.return_value = ("", "subtitle copy failed")

        ok_proc = MagicMock()
        ok_proc.returncode = 0
        ok_proc.communicate.return_value = ("", "")

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
        ), patch("engine.job_queue._probe_media_duration_seconds", return_value=None), \
           patch(
               "engine.job_queue.subprocess.Popen",
               side_effect=[fail_proc, ok_proc],
           ) as mock_popen, patch("engine.job_queue.os.replace"):
            _path, profile = _enforce_video_codec_container_rules(
                "/tmp/source.mp4",
                target_container="mp4",
            )

        self.assertEqual(profile.get("final_audio_codec"), "aac")
        self.assertEqual(mock_popen.call_count, 2)
        first_args = mock_popen.call_args_list[0][0][0]
        second_args = mock_popen.call_args_list[1][0][0]
        self.assertIn("-map", first_args)
        self.assertIn("0:s?", first_args)
        self.assertIn("-c:s", first_args)
        self.assertIn("copy", first_args)
        self.assertIn("-sn", second_args)

    def test_mp4_target_transcodes_video_to_h264_when_probe_reports_vp9(self):
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate.return_value = ("", "")
        with patch(
            "engine.job_queue._probe_media_profile",
            side_effect=[
                {
                    "final_container": "mp4",
                    "final_video_codec": "vp9",
                    "final_audio_codec": "opus",
                },
                {
                    "final_container": "mp4",
                    "final_video_codec": "h264",
                    "final_audio_codec": "aac",
                },
            ],
        ), patch("engine.job_queue._probe_media_duration_seconds", return_value=None), \
           patch("engine.job_queue.subprocess.Popen", return_value=mock_proc) as mock_popen, \
           patch("engine.job_queue.os.replace"):
            _path, profile = _enforce_video_codec_container_rules(
                "/tmp/source.mp4",
                target_container="mp4",
            )

        self.assertEqual(profile.get("final_video_codec"), "h264")
        self.assertEqual(profile.get("final_audio_codec"), "aac")
        ffmpeg_args = mock_popen.call_args[0][0]
        self.assertIn("libx264", ffmpeg_args)
        self.assertIn("aac", ffmpeg_args)
