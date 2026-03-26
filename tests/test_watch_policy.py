import unittest
from datetime import datetime, timezone

from api import main as api_main


class WatchPolicyTests(unittest.TestCase):
    def test_normalize_watch_policy_invalid_uses_last_good(self):
        last_good = dict(api_main._default_watch_policy())
        last_good["min_interval_minutes"] = 9
        api_main.app.state.watch_policy = last_good

        policy = api_main.normalize_watch_policy({"watch_policy": {"min_interval_minutes": 5}})

        self.assertEqual(policy["min_interval_minutes"], 9)
        self.assertFalse(api_main.normalize_watch_policy.valid)

    def test_downtime_crosses_midnight(self):
        start = "23:00"
        end = "09:00"
        now_late = datetime(2026, 1, 1, 23, 30, tzinfo=timezone.utc)
        now_early = datetime(2026, 1, 2, 8, 30, tzinfo=timezone.utc)

        in_window, next_allowed = api_main.in_downtime(now_late, start, end)
        self.assertTrue(in_window)
        self.assertEqual(next_allowed, datetime(2026, 1, 2, 9, 0, tzinfo=timezone.utc))

        in_window, next_allowed = api_main.in_downtime(now_early, start, end)
        self.assertTrue(in_window)
        self.assertEqual(next_allowed, datetime(2026, 1, 2, 9, 0, tzinfo=timezone.utc))

    def test_downtime_same_day_start_inclusive_end_exclusive(self):
        start = "08:00"
        end = "10:00"
        at_start = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
        before_end = datetime(2026, 1, 1, 9, 59, tzinfo=timezone.utc)
        at_end = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)

        in_window, next_allowed = api_main.in_downtime(at_start, start, end)
        self.assertTrue(in_window)
        self.assertEqual(next_allowed, datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc))

        in_window, next_allowed = api_main.in_downtime(before_end, start, end)
        self.assertTrue(in_window)
        self.assertEqual(next_allowed, datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc))

        in_window, next_allowed = api_main.in_downtime(at_end, start, end)
        self.assertFalse(in_window)
        self.assertIsNone(next_allowed)
