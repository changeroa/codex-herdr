from pathlib import Path
import os
import runpy
from unittest.mock import patch
import unittest

launcher = runpy.run_path(str(Path(__file__).with_name("codex")))
lookup = launcher["pane_for_process"]


class TestLaunch(unittest.TestCase):
    def setUp(self):
        self.panes = [{"pane_id": k, "terminal_id": "term-" + k, "tab_id": "t", "workspace_id": "w"}
                      for k in ("old", "new")]
        self.pids = {"old": 11, "new": 22}

    def call(self, method, args):
        if method == "pane.current":
            matches = [p for p in self.panes if p["pane_id"] == args["caller_pane_id"]]
            if not matches:
                raise ValueError("gone")
            return {"pane": matches[0]}
        if method == "pane.list":
            return {"panes": self.panes}
        self.assertEqual(method, "pane.process_info")
        key = args["pane_id"]
        return {"process_info": {"pane_id": key, "foreground_processes": [{"pid": self.pids[key]}]}}

    def test_verified_foreground(self):
        for hint in ("new", "old", "deleted", None):
            with self.subTest(hint=hint):
                self.assertEqual(lookup(self.call, hint, 22), self.panes[1])

    def test_unverified_or_ambiguous(self):
        self.assertIsNone(lookup(self.call, "old", 999))
        self.pids["old"] = 22
        self.assertIsNone(lookup(self.call, None, 22))

    def test_verified_launch_and_missing_owner(self):
        options = launcher["launch_options"]
        env = {"HERDR_ENV": "1", "HERDR_SOCKET_PATH": "/local/socket", "HERDR_PANE_ID": "old"}
        def request(_endpoint, method, params, _deadline):
            return self.call(method, params)
        with patch.dict(os.environ, env), patch("sys.stdin.isatty", return_value=True), \
                patch("os.tcgetpgrp", return_value=os.getpgrp()), patch("os.getpid", return_value=22), \
                patch.dict(options.__globals__, {"request": request}):
            args = options(["--yolo"])
            self.assertIn('shell_environment_policy.set.HERDR_PANE_ID="new"', args)
            self.assertIn('shell_environment_policy.set.HERDR_TAB_ID="t"', args)
            self.assertEqual(options(["app-server", "daemon", "version"]), [])
            self.pids["new"] = 33
            with patch("sys.stderr"):
                self.assertEqual(options([]), ["-c", 'shell_environment_policy.set.HERDR_ENV="0"'])

    def test_cli_passthrough(self):
        for args in (["app-server", "daemon", "version"], ["--remote=ws://test"], ["exec"], ["--help"]):
            self.assertFalse(launcher["interactive_launch"](args))
        for args in ([], ["--yolo"], ["resume", "some-id"], ["fork"], ["-m", "gpt-6-luna"]):
            self.assertTrue(launcher["interactive_launch"](args))


if __name__ == "__main__":
    unittest.main()
