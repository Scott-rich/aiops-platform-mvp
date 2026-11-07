import time
import unittest
from infra.submit_job import command, wait_ready, wait_running


class SubmitterTests(unittest.TestCase):
    def test_restore_is_explicit_cli_argument_without_shell(self):
        args = command({"FLINK_RESTORE_PATH": "file:///checkpoints/job/chk-3", "FLINK_REST_URL": "http://127.0.0.1:28081"})
        self.assertEqual(args[args.index("-s")+1], "file:///checkpoints/job/chk-3")
        self.assertEqual(args[args.index("-m")+1], "http://127.0.0.1:28081")
        self.assertNotIn("--allowNonRestoredState", args)
        self.assertNotIn("-s", command({}))

    def test_active_job_skips_new_submission(self):
        job = {"name": "detector", "state": "RESTARTING", "jid": "one"}
        def fetch(base, path):
            self.assertEqual(path, "/jobs/overview")
            return {"jobs": [job]}
        self.assertEqual(wait_ready("unused", "detector", time.monotonic()+1, fetch=fetch), job)

    def test_taskmanager_readiness(self):
        def fetch(base, path):
            return {"jobs": []} if "jobs" in path else {"taskmanagers": 1}
        self.assertIsNone(wait_ready("unused", "detector", time.monotonic()+1, fetch=fetch))

    def test_duplicate_active_jobs_fail_closed(self):
        def fetch(base, path):
            return {"jobs": [{"name": "detector", "state": "RUNNING"}]*2}
        with self.assertRaisesRegex(RuntimeError, "multiple active"):
            wait_ready("unused", "detector", time.monotonic()+1, fetch=fetch)

    def test_timeout_is_finite(self):
        with self.assertRaises(TimeoutError):
            wait_ready("unused", "detector", time.monotonic()-1)

    def test_running_wait_tolerates_transient_rest_failure(self):
        states = iter([ConnectionError("not ready"), {"jobs": [{"name": "detector", "state": "INITIALIZING"}]},
                       {"jobs": [{"name": "detector", "state": "RUNNING", "jid": "abc", "tasks": {"total": 6, "running": 6}}]}])
        def fetch(base, path):
            value = next(states)
            if isinstance(value, Exception):
                raise value
            return value
        self.assertEqual(wait_running("unused", "detector", time.monotonic()+1, fetch, lambda _: None)["jid"], "abc")

    def test_running_wait_fails_on_failed_job(self):
        with self.assertRaisesRegex(RuntimeError, "detector failed"):
            wait_running("unused", "detector", time.monotonic()+1,
                         lambda *_: {"jobs": [{"name": "detector", "state": "FAILED"}]})

    def test_top_level_running_does_not_mean_operators_running(self):
        states = iter([{"jobs": [{"name": "detector", "state": "RUNNING", "jid": "abc", "tasks": {"total": 6, "deploying": 6}}]},
                       {"jobs": [{"name": "detector", "state": "RUNNING", "jid": "abc", "tasks": {"total": 6, "running": 6}}]}])
        calls = []
        def fetch(*_):
            calls.append(1)
            return next(states)
        self.assertEqual(wait_running("unused", "detector", time.monotonic()+1, fetch, lambda _: None)["jid"], "abc")
        self.assertEqual(len(calls), 2)
