# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


"""Local watchdog tests; no IoTDB build or remote Actions calls required."""

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("ut-watchdog.py")
SPEC = importlib.util.spec_from_file_location("watchdog", SCRIPT)
watchdog = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(watchdog)


class WatchdogTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ut-watchdog-")
        self.root = Path(self.temp.name)
        self.state = self.root / "state"
        self.env = os.environ.copy()
        self.children = []

    def tearDown(self):
        if self.state.exists():
            watchdog.publish(self.state / "stop", "stop")
        for child in self.children:
            if child.poll() is None:
                watchdog.stop_tree(child)
        self.temp.cleanup()

    def cli(self, mode, *args, timeout=15):
        return subprocess.run([sys.executable, str(SCRIPT), mode, "--state", str(self.state), *args],
                              env=self.env, capture_output=True, timeout=timeout)

    def start(self, code, threshold="0.1", interval="0.1", attach="0.3"):
        return self.cli("start", "--threshold", threshold, "--interval", interval,
                        "--attach-timeout", attach, "--", sys.executable, "-c", code)

    def sleeper(self):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                 start_new_session=os.name != "nt")
        self.children.append(child)
        return child

    def test_early_success(self):
        started = time.monotonic()
        self.assertEqual(self.start("print('early')", threshold="60").returncode, 0)
        result = self.cli("wait")
        self.assertEqual(result.returncode, 0)
        self.assertLess(time.monotonic() - started, 3)
        self.assertFalse((self.state / "diagnostics").exists())

    @unittest.skipUnless(os.name == "posix", "POSIX signal exit status")
    def test_signal_exit_status(self):
        self.assertEqual(self.start("import os,signal; os.kill(os.getpid(), signal.SIGTERM)", threshold="60").returncode, 0)
        self.assertEqual(self.cli("wait").returncode, 143)

    def test_original_failure(self):
        self.assertEqual(self.start("raise SystemExit(37)", threshold="60").returncode, 0)
        self.assertEqual(self.cli("wait").returncode, 37)

    def test_upload_boundary_while_command_still_running(self):
        self.env["JAVA_HOME"] = str(self.root / "missing-jdk")
        self.assertEqual(self.start("import time; time.sleep(2); raise SystemExit(23)").returncode, 0)
        self.assertTrue((self.state / "ready").exists())
        self.assertFalse((self.state / "exit").exists())
        discovery = (self.state / "diagnostics" / "discovery.txt").read_text()
        self.assertEqual(discovery.count("round="), 3)
        self.assertIn("Unable to start", discovery)
        self.assertEqual(self.cli("wait").returncode, 23)

    def test_rediscover_multiple_and_changed_pids(self):
        self.state.mkdir()
        test = self.sleeper()
        args = types.SimpleNamespace(state=self.state, interval=0, attach_timeout=0.2)
        marker = "-Diotdb.ut.watchdog=ours"
        listings = iter([f"11 Maven {marker}\n12 Surefire {marker}\n99 Other -Diotdb.ut.watchdog=other",
                         f"11 Maven {marker}\n13 Surefire {marker}", f"14 Surefire {marker}"])
        attached = []

        def fake_capture(command, destination, *_):
            if command[1] == "-v":
                destination.write_text(next(listings) + "\nUTC=end diagnostic_exit=0\n")
            else:
                attached.append(command[-1])
                destination.write_text("Full thread dump")

        with patch.object(watchdog, "jdk_tool", side_effect=lambda name: name), patch.object(watchdog, "capture", side_effect=fake_capture):
            watchdog.collect(args, test, marker)
        self.assertEqual(attached, ["11", "12", "11", "13", "14"])
        self.assertNotIn("Other", (self.state / "diagnostics" / "discovery.txt").read_text())
        self.assertFalse((self.state / "jps.txt").exists())

    def test_attach_failure_keeps_output(self):
        self.state.mkdir()
        destination = self.root / "dump.txt"
        watchdog.capture([sys.executable, "-c", "print('attach failed'); raise SystemExit(9)"],
                         destination, 2, self.sleeper(), self.state)
        self.assertIn("attach failed", destination.read_text())
        self.assertIn("diagnostic_exit=9", destination.read_text())

    def test_attach_timeout_cleans_process(self):
        self.state.mkdir()
        destination = self.root / "dump.txt"
        pidfile = self.root / "attach.pid"
        started = time.monotonic()
        watchdog.capture([sys.executable, "-c", f"import os,time; open({str(pidfile)!r},'w').write(str(os.getpid())); time.sleep(60)"],
                         destination, 0.4, self.sleeper(), self.state)
        self.assertLess(time.monotonic() - started, 2)
        self.assertIn("attach timeout", destination.read_text())
        if os.name != "nt":
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)

    def test_test_exit_interrupts_attach(self):
        self.state.mkdir()
        test = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.2)"],
                                start_new_session=os.name != "nt")
        self.children.append(test)
        started = time.monotonic()
        destination = self.root / "dump.txt"
        watchdog.capture([sys.executable, "-c", "import time; time.sleep(60)"],
                         destination, 30, test, self.state)
        self.assertLess(time.monotonic() - started, 2)
        self.assertIn("test ended", destination.read_text())

    def test_cancellation_stops_command(self):
        self.env["JAVA_HOME"] = str(self.root / "missing-jdk")
        pidfile = self.root / "command.pid"
        self.assertEqual(self.start(f"import os,time; open({str(pidfile)!r},'w').write(str(os.getpid())); time.sleep(60)").returncode, 0)
        self.assertEqual(self.cli("stop").returncode, 0)
        self.assertNotEqual(self.cli("wait").returncode, 0)
        if os.name != "nt":
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)

    def test_diagnostic_exception_preserves_command_result(self):
        self.env.pop("JAVA_HOME", None)
        self.assertEqual(self.start("import time; time.sleep(0.5); raise SystemExit(19)").returncode, 0)
        self.assertTrue((self.state / "diagnostics" / "error.txt").exists())
        self.assertEqual(self.cli("wait").returncode, 19)

    def test_cancel_start_during_threshold_wait(self):
        pidfile = self.root / "command.pid"
        proc = subprocess.Popen([sys.executable, str(SCRIPT), "start", "--state", str(self.state),
                                 "--threshold", "60", "--", sys.executable, "-c",
                                 f"import os,time; open({str(pidfile)!r},'w').write(str(os.getpid())); time.sleep(60)"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 5
            while not pidfile.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertTrue(pidfile.exists())
            # POSIX signal delivery is real here; Windows termination is covered
            # separately by the cooperative stop path and command simulation.
            proc.terminate()
            proc.wait(timeout=5)
            self.assertEqual(self.cli("stop").returncode, 0)
            self.assertTrue((self.state / "exit").exists())
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

    def test_windows_tree_cleanup_command_simulated(self):
        process = unittest.mock.Mock(pid=123)
        with patch.object(watchdog.os, "name", "nt"), patch.object(watchdog.subprocess, "run") as run:
            watchdog.stop_tree(process)
        self.assertEqual(run.call_args.args[0], ["taskkill", "/PID", "123", "/T", "/F"])
        process.wait.assert_called_once_with(timeout=10)

    @unittest.skipUnless(os.name == "posix" and shutil.which("java"), "requires local Java modules and POSIX launchers")
    def test_real_java_stacks(self):
        # Some cloud images retain JDK modules but omit the tool entry points.
        # Use their actual jps/jstack implementations, without downloading a JDK.
        java = shutil.which("java")
        probe = subprocess.run([java, "--list-modules"], capture_output=True, text=True)
        if "jdk.jcmd@" not in probe.stdout or "jdk.compiler@" not in probe.stdout:
            self.skipTest("JDK tool modules unavailable")
        bindir = self.root / "jdk" / "bin"
        bindir.mkdir(parents=True)
        for name, main in [("jps", "sun.tools.jps.Jps"), ("jstack", "sun.tools.jstack.JStack")]:
            launcher = bindir / name
            launcher.write_text(f'#!/bin/sh\nexec "{java}" -m jdk.jcmd/{main} "$@"\n')
            launcher.chmod(0o755)
        self.env["JAVA_HOME"] = str(bindir.parent)
        source = self.root / "Sleeper.java"
        source.write_text('public class Sleeper { public static void main(String[] a) throws Exception { Thread.currentThread().setName("watchdog-real-stack"); Thread.sleep(60000); } }')
        subprocess.run([java, "-m", "jdk.compiler/com.sun.tools.javac.Main", str(source)], check=True, capture_output=True)
        command = f"""
import subprocess, time
from pathlib import Path
cmd = [{java!r}, '-cp', {str(self.root)!r}, 'Sleeper']
a = subprocess.Popen(cmd)
b = subprocess.Popen(cmd)
diagnostics = Path({str(self.state / "diagnostics")!r})
while len([p for p in diagnostics.glob('round-1-pid-*.txt') if 'diagnostic_exit=0' in p.read_text()]) < 2:
    time.sleep(0.05)
b.terminate()
b.wait()
c = subprocess.Popen(cmd)
a.wait()
c.wait()
"""
        try:
            self.assertEqual(self.start(command, threshold="1", interval="1.5", attach="4").returncode, 0)
            self.assertFalse((self.state / "exit").exists())
            dumps = list((self.state / "diagnostics").glob("round-*-pid-*.txt"))
            self.assertEqual(len(dumps), 6)
            first = {p.stem.split('-')[-1] for p in dumps if p.name.startswith('round-1-')}
            second = {p.stem.split('-')[-1] for p in dumps if p.name.startswith('round-2-')}
            self.assertEqual(len(first & second), 1)
            for dump in dumps:
                text = dump.read_text()
                self.assertIn("Full thread dump", text)
                self.assertIn('"watchdog-real-stack"', text)
                self.assertIn("Sleeper.main", text)
                self.assertIn("diagnostic_exit=0", text)
            evidence = os.environ.get("UT_WATCHDOG_EVIDENCE")
            if evidence:
                shutil.copytree(self.state / "diagnostics", Path(evidence) / "real-java", dirs_exist_ok=True)
        finally:
            self.cli("stop")
            self.cli("wait")


if __name__ == "__main__":
    unittest.main(verbosity=2)
