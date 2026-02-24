import unittest

from engine.job_queue import build_ytdlp_opts


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

    def test_video_mp4_target_sets_container_only_not_strict_selector(self):
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
