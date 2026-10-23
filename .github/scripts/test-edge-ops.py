#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Run Edge ops against disposable data and stubbed service/stop commands."""

import fnmatch
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import xml.etree.ElementTree as ET

REPOSITORY = Path(__file__).resolve().parents[2]
WINDOWS = os.name == "nt"
DIRECTORY_KEYS = (
    "cn_system_dir",
    "cn_consensus_dir",
    "dn_system_dir",
    "dn_data_dirs",
    "dn_consensus_dir",
    "dn_wal_dirs",
    "dn_tracing_dir",
    "dn_sync_dir",
    "pipe_receiver_file_dirs",
    "iot_consensus_v2_receiver_file_dirs",
    "sort_tmp_dir",
)


class EdgeOpsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="iotdb-edge-ops-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.installation = self.root / "edge installation"
        self.config = self.installation / "conf"
        self.config.mkdir(parents=True)
        self.events = self.root / "events.txt"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = os.environ.copy()
        for key in (
            "IOTDB_HOME",
            "IOTDB_CONF",
            "IOTDB_DATA_HOME",
            "IOTDB_LOG_DIR",
            "JAVA_HOME",
        ):
            self.env.pop(key, None)
        self.env["PATH"] = str(self.bin) + os.pathsep + self.env["PATH"]
        self.env["EDGE_TEST_EVENTS"] = str(self.events)
        self.env["EDGE_TEST_STOP_STATUS"] = "0"
        self.env["SYSTEMD_DIR"] = str(self.root / "systemd")
        Path(self.env["SYSTEMD_DIR"]).mkdir()
        self.marker = self.directory(self.installation / "data") / "marker"
        self.marker.touch()
        self.keep = self.directory(self.installation / "logs") / "keep.log"
        self.keep.touch()
        if WINDOWS:
            self.destroy = self.copy("tools/windows/ops/destroy-edge.bat")
            self.stop = self.installation / "sbin/windows/stop-edge.bat"
            self.write(
                self.stop,
                "@echo off\n"
                '>>"%EDGE_TEST_EVENTS%" echo stop %*\n'
                'if not exist "%IOTDB_HOME%\\data\\marker" exit /b 97\n'
                "exit /b %EDGE_TEST_STOP_STATUS%\n",
            )
        else:
            self.destroy = self.copy("tools/ops/destroy-edge.sh")
            self.daemon = self.copy("tools/ops/daemon-edge.sh")
            self.stop = self.installation / "sbin/stop-edge.sh"
            self.write(
                self.stop,
                "#!/bin/bash\n"
                'printf "stop %s\\n" "$*" >> "$EDGE_TEST_EVENTS"\n'
                "sleep 0.05\n"
                '[ -f "$IOTDB_HOME/data/marker" ] || exit 97\n'
                'exit "$EDGE_TEST_STOP_STATUS"\n',
            )
            self.write(
                self.installation / "sbin/start-edge.sh", "#!/bin/bash\nexit 0\n"
            )
            self.write(self.bin / "java", "#!/bin/bash\nexit 0\n")
            self.write(
                self.bin / "systemctl",
                "#!/bin/bash\n"
                'printf "systemctl %s\\n" "$*" >> "$EDGE_TEST_EVENTS"\n'
                'if [ "$1" = show ]; then printf "%s\\n" "$EDGE_TEST_SERVICE_PIDFILE"; fi\n'
                '[ "$1" != "$EDGE_TEST_SYSTEMCTL_FAIL" ]\n',
            )

    @staticmethod
    def write(path, content):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content.encode("utf-8"))
        path.chmod(0o755)

    def copy(self, relative):
        destination = self.installation / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPOSITORY / "scripts" / relative, destination)
        destination.chmod(0o755)
        return destination

    @staticmethod
    def directory(path):
        path.mkdir(parents=True, exist_ok=True)
        (path / "data.txt").touch()
        return path

    def properties(self, lines, name="iotdb-system.properties"):
        self.write(self.config / name, "\r\n".join(lines) + "\r\n")

    def run_script(self, script, arguments=(), answer=""):
        if WINDOWS:
            command = '"{}" /d /c call "{}" {}'.format(
                os.environ["COMSPEC"], script, " ".join(arguments)
            )
        else:
            command = ["bash", str(script), *arguments]
        return subprocess.run(
            command,
            input=answer,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            errors="replace",
            env=self.env,
            cwd=self.root,
            timeout=30,
        )

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout)

    def event_lines(self):
        return self.events.read_text().splitlines() if self.events.exists() else []

    def test_default_answer_and_no_do_not_stop_or_delete(self):
        for answer in ("", "\n", "n\n", "yes\n"):
            with self.subTest(answer=answer):
                self.env["CLEAN_SERVICE"] = "y"
                self.assert_success(self.run_script(self.destroy, answer=answer))
                self.assertTrue(self.marker.exists())
                self.assertEqual(self.event_lines(), [])

    def test_invalid_arguments_do_not_stop_or_delete(self):
        for arguments in (("--unknown",), ("-f", "extra")):
            with self.subTest(arguments=arguments):
                result = self.run_script(self.destroy, arguments)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertTrue(self.marker.exists())
                self.assertEqual(self.event_lines(), [])

    def test_force_cleans_defaults_from_an_unrelated_working_directory(self):
        tracing = self.directory(self.installation / "datanode/tracing")
        result = self.run_script(self.destroy, ("-f",))
        self.assert_success(result)
        self.assertFalse((self.installation / "data").exists())
        self.assertFalse(tracing.exists())
        self.assertTrue(self.keep.exists())
        self.assertTrue(self.config.exists())
        self.assertIn("stop -f", self.event_lines())
        self.assertIn("IoTDB Edge clean done", result.stdout)

    def test_yes_confirms_cleanup(self):
        self.assert_success(self.run_script(self.destroy, answer="Y\n"))
        self.assertFalse(self.marker.exists())

    def test_stop_failure_preserves_all_data(self):
        self.env["EDGE_TEST_STOP_STATUS"] = "42"
        result = self.run_script(self.destroy, ("-f",))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertTrue(self.marker.exists())
        self.assertNotIn("clean done", result.stdout)

    def test_all_configured_directories_and_multiple_tiers(self):
        targets = []
        lines = ["# dn_data_dirs=ignored", "! cn_system_dir=ignored"]
        for index, key in enumerate(DIRECTORY_KEYS):
            base = self.installation if index % 2 else self.root
            target = self.directory(base / (key + " with spaces=[1]"))
            value = target.relative_to(base) if base == self.installation else target
            lines.append("  {} = {}  ".format(key, value.as_posix()))
            targets.append(target)
        tiers = [
            self.directory(self.installation / "tier one"),
            self.directory(self.root / "absolute tier"),
            self.directory(self.installation / "tier=two"),
        ]
        lines.append(
            " dn_data_dirs = tier one ; {}, tier=two ".format(tiers[1].as_posix())
        )
        # The previous value of a repeated property must not be removed.
        superseded = targets.pop(DIRECTORY_KEYS.index("dn_data_dirs"))
        self.properties(lines)
        self.assert_success(self.run_script(self.destroy, ("-f",)))
        for target in targets + tiers:
            self.assertFalse(target.exists(), str(target))
        self.assertTrue(superseded.exists())
        self.assertTrue((self.config / "iotdb-system.properties").exists())
        self.assertTrue(self.keep.exists())

    def test_legacy_configuration_files(self):
        cn = self.directory(self.root / "legacy cn")
        dn = self.directory(self.root / "legacy dn")
        self.properties(
            ["cn_system_dir=" + cn.as_posix()], "iotdb-confignode.properties"
        )
        self.properties(["dn_data_dirs=" + dn.as_posix()], "iotdb-datanode.properties")
        self.assert_success(self.run_script(self.destroy, ("-f",)))
        self.assertFalse(cn.exists())
        self.assertFalse(dn.exists())

    def test_custom_home_and_configuration(self):
        self.env["IOTDB_HOME"] = str(self.installation)
        self.config = self.root / "custom configuration"
        self.config.mkdir()
        self.env["IOTDB_CONF"] = str(self.config)
        target = self.directory(self.root / "custom data")
        self.properties(["dn_data_dirs=" + target.as_posix()])
        self.assert_success(self.run_script(self.destroy, ("-f",)))
        self.assertFalse(target.exists())

    def test_unified_configuration_takes_precedence(self):
        untouched = self.directory(self.root / "legacy data")
        self.properties(
            ["dn_data_dirs=" + untouched.as_posix()], "iotdb-datanode.properties"
        )
        self.properties(["# Defaults"])
        self.assert_success(self.run_script(self.destroy, ("-f",)))
        self.assertTrue(untouched.exists())

    def test_home_or_parent_is_rejected_before_any_deletion(self):
        # Only disposable paths are used even if the guard regresses.
        for value in (".", "data/..", ".."):
            with self.subTest(value=value):
                self.properties(["dn_data_dirs=" + value])
                result = self.run_script(self.destroy, ("-f",))
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("Refusing to remove", result.stdout)
                self.assertTrue(self.marker.exists())
                self.assertTrue(self.keep.exists())

    def test_missing_stop_script_preserves_data(self):
        self.stop.unlink()
        result = self.run_script(self.destroy, ("-f",))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertTrue(self.marker.exists())

    def test_empty_property_uses_default_not_an_earlier_value(self):
        untouched = self.directory(self.root / "superseded data")
        self.properties(["dn_data_dirs=" + untouched.as_posix(), "dn_data_dirs=  "])
        self.assert_success(self.run_script(self.destroy, ("-f",)))
        self.assertFalse(self.marker.exists())
        self.assertTrue(untouched.exists())

    @unittest.skipIf(WINDOWS, "Unix symbolic links")
    def test_symlinked_parent_cannot_hide_a_home_directory(self):
        (self.root / "home alias").symlink_to(
            self.installation, target_is_directory=True
        )
        self.properties(
            ["dn_data_dirs=" + (self.root / "home alias/data/..").as_posix()]
        )
        result = self.run_script(self.destroy, ("-f",))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("Refusing to remove", result.stdout)
        self.assertTrue(self.marker.exists())

    @unittest.skipIf(WINDOWS, "Unix deletion errors")
    def test_deletion_failure_is_not_reported_as_success(self):
        self.write(self.bin / "rm", "#!/bin/bash\nexit 42\n")
        result = self.run_script(self.destroy, ("-f",))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn("clean done", result.stdout)
        self.assertTrue(self.marker.exists())

    @unittest.skipIf(
        WINDOWS, "The Unix launcher supports a separate DataNode data home"
    )
    def test_data_home_does_not_change_confignode_home(self):
        data_home = self.directory(self.root / "external data home")
        self.env["IOTDB_DATA_HOME"] = str(data_home)
        cn = self.directory(self.installation / "cn")
        dn = self.directory(data_home / "dn")
        keep_cn = self.directory(data_home / "cn")
        keep_dn = self.directory(self.installation / "dn")
        self.properties(["cn_system_dir=cn", "dn_data_dirs=dn"])
        self.assert_success(self.run_script(self.destroy, ("-f",)))
        self.assertFalse(cn.exists())
        self.assertFalse(dn.exists())
        self.assertTrue(keep_cn.exists())
        self.assertTrue(keep_dn.exists())

    @unittest.skipIf(WINDOWS, "Unix systemd integration")
    def test_cleanup_stops_only_the_matching_systemd_service(self):
        self.env["EDGE_TEST_SERVICE_PIDFILE"] = str(self.installation / "edge.pid")
        self.assert_success(self.run_script(self.destroy, ("-f",)))
        events = self.event_lines()
        self.assertLess(
            events.index("systemctl stop iotdb-edge"), events.index("stop -f")
        )

    @unittest.skipIf(WINDOWS, "Unix systemd integration")
    def test_other_systemd_installation_is_not_stopped(self):
        self.env["EDGE_TEST_SERVICE_PIDFILE"] = str(self.root / "another edge/edge.pid")
        self.assert_success(self.run_script(self.destroy, ("-f",)))
        self.assertNotIn("systemctl stop iotdb-edge", self.event_lines())

    @unittest.skipIf(WINDOWS, "Unix systemd integration")
    def test_systemd_stop_failure_preserves_data(self):
        self.env["EDGE_TEST_SERVICE_PIDFILE"] = str(self.installation / "edge.pid")
        self.env["EDGE_TEST_SYSTEMCTL_FAIL"] = "stop"
        result = self.run_script(self.destroy, ("-f",))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertTrue(self.marker.exists())
        self.assertNotIn("stop -f", self.event_lines())

    @unittest.skipIf(WINDOWS, "Unix systemd registration")
    def test_daemon_tracks_the_forked_jvm_and_supports_java_home(self):
        java_home = self.root / "custom java"
        self.write(java_home / "bin/java", "#!/bin/bash\nexit 0\n")
        self.env["JAVA_HOME"] = str(java_home)
        result = self.run_script(self.daemon, answer="\n\n")
        self.assert_success(result)
        unit_path = Path(self.env["SYSTEMD_DIR"]) / "iotdb-edge.service"
        unit = unit_path.read_text()
        for setting in (
            "Type=forking",
            "PIDFile={}/edge.pid".format(self.installation),
            "Restart=on-failure",
            "SuccessExitStatus=143",
            "RestartPreventExitStatus=SIGKILL",
            "LimitNOFILE=65536",
            "StartLimitIntervalSec=600s",
            "StartLimitBurst=3",
            'Environment="JAVA_HOME={}"'.format(java_home),
            'Environment="PATH={}/bin:'.format(java_home),
            'ExecStart="{}/sbin/start-edge.sh"'.format(self.installation),
            'ExecStop="{}/sbin/stop-edge.sh"'.format(self.installation),
        ):
            self.assertIn(setting, unit)
        self.assertEqual(
            self.event_lines(),
            [
                "systemctl daemon-reload",
                "systemctl stop iotdb-edge",
                "stop ",
                "systemctl start iotdb-edge",
                "systemctl enable iotdb-edge",
            ],
        )
        if shutil.which("systemd-analyze"):
            check = subprocess.run(
                ["systemd-analyze", "verify", str(unit_path)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=30,
            )
            self.assertEqual(check.returncode, 0, check.stdout)

    @unittest.skipIf(WINDOWS, "Unix systemd registration")
    def test_daemon_path_java_and_declining_start_and_enable(self):
        self.assert_success(self.run_script(self.daemon, answer="n\nn\n"))
        self.assertEqual(self.event_lines(), ["systemctl daemon-reload"])
        unit = (Path(self.env["SYSTEMD_DIR"]) / "iotdb-edge.service").read_text()
        self.assertIn('Environment="JAVA_HOME="', unit)
        self.assertIn('Environment="PATH={}"'.format(self.env["PATH"]), unit)

    @unittest.skipIf(WINDOWS, "Unix systemd registration")
    def test_daemon_rejects_invalid_java_home(self):
        self.env["JAVA_HOME"] = str(self.root / "missing java")
        result = self.run_script(self.daemon, answer="n\nn\n")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.event_lines(), [])

    @unittest.skipIf(WINDOWS, "Unix systemd registration")
    def test_daemon_propagates_service_start_errors(self):
        self.env["EDGE_TEST_SYSTEMCTL_FAIL"] = "start"
        result = self.run_script(self.daemon, answer="y\ny\n")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn("systemctl enable iotdb-edge", self.event_lines())

    @unittest.skipUnless(WINDOWS, "Native Windows stop script")
    def test_windows_force_stop_returns_without_pausing(self):
        self.copy("sbin/windows/stop-edge.bat")
        result = self.run_script(self.stop, ("-f",))
        self.assert_success(result)
        self.assertNotIn("Press any key", result.stdout)


@unittest.skipIf(WINDOWS, "Unix stop-edge.sh")
class EdgeStopTest(unittest.TestCase):
    """Run the real stop-edge.sh against stand-in Edge processes that only sleep.

    start-edge.sh needs a JVM, so each stand-in carries the -DIOTDB_HOME that
    start-edge.sh would pass to it: the value initEdgeHome derives for it.
    """

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="iotdb-edge-stop-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        # Identifies this test's stand-ins, so that cleanup never touches others.
        self.marker = "-Dedge.stop.test=" + self.root.as_posix()
        self.env = os.environ.copy()
        self.env.pop("IOTDB_HOME", None)
        self.env.pop("CDPATH", None)

    def install(self, relative):
        home = self.root / relative
        for script in (
            "sbin/start-edge.sh",
            "sbin/stop-edge.sh",
            "conf/iotdb-common.sh",
        ):
            (home / script).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPOSITORY / "scripts" / script, home / script)
        return home

    def link(self, relative, target):
        path = self.root / relative
        if path.is_symlink():
            path.unlink()
        path.symlink_to(target, target_is_directory=True)

    def symlink_layout(self):
        # x/link/../edge is y/edge physically, while x/edge is another installation.
        (self.root / "y/sub").mkdir(parents=True)
        self.install("y/edge")
        self.install("x/edge")
        self.link("x/link", "../y/sub")

    def bash(self, arguments, iotdb_home=None, cwd=None, **variables):
        env = dict(self.env, **variables)
        if iotdb_home is not None:
            env["IOTDB_HOME"] = iotdb_home
        return subprocess.run(
            ["bash", *arguments],
            env=env,
            cwd=cwd or self.root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            timeout=60,
        )

    def started_home(self, script, iotdb_home=None, **variables):
        """The -DIOTDB_HOME that start-edge.sh at this path passes to the JVM."""
        result = self.bash(
            (
                "-c",
                'source "$(dirname "$1")/../conf/iotdb-common.sh"'
                ' && initEdgeHome "$1" && printf "%s" "$IOTDB_HOME"',
                "bash",
                script,
            ),
            iotdb_home,
            **variables,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def stop(self, script, iotdb_home=None, cwd=None, **variables):
        result = self.bash((script,), iotdb_home, cwd, **variables)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def spawn(self, command, cwd=None):
        # Detached through a short-lived parent, like the nohup'ed JVM, so that
        # the process is reaped once it exits: a zombie child of this test would
        # still look alive to stop-edge.sh.
        launcher = subprocess.run(
            [
                sys.executable,
                "-c",
                "import subprocess, sys; print(subprocess.Popen(sys.argv[1:],"
                " stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,"
                " stderr=subprocess.DEVNULL).pid)",
                *command,
            ],
            cwd=cwd or self.root,
            stdout=subprocess.PIPE,
            text=True,
            check=True,
            timeout=30,
        )
        pid = int(launcher.stdout)
        self.addCleanup(self.kill, pid)
        return pid

    def start(self, home, cwd=None):
        """Start a stand-in for the JVM and write edge.pid where start-edge.sh does."""
        cwd = cwd or self.root
        pid = self.spawn(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(600)",
                self.marker,
                "-DIOTDB_HOME=" + home,
                "-DCONFIGNODE_HOME=" + home,
                "org.apache.iotdb.edge.EdgeNode",
                "-s",
            ],
            cwd,
        )
        (Path(cwd) / home / "edge.pid").write_text("{}\n".format(pid))
        return pid

    def command(self, pid):
        return subprocess.run(
            ["ps", "-ww", "-p", str(pid), "-o", "command="],
            stdout=subprocess.PIPE,
            text=True,
            errors="replace",
        ).stdout

    def running(self, pid):
        return self.marker in self.command(pid)

    def kill(self, pid):
        if self.running(pid):
            os.kill(pid, signal.SIGKILL)

    def test_symlinked_home_is_stopped_through_the_real_path(self):
        # apache/iotdb#18545
        self.install("edge")
        self.link("link", "edge")
        home = self.started_home("edge/sbin/start-edge.sh", str(self.root / "link"))
        pid = self.start(home)
        self.stop("edge/sbin/stop-edge.sh")
        self.assertFalse(self.running(pid))

    def test_stop_after_the_symlink_is_re_pointed(self):
        self.install("edge-v1")
        self.install("edge-v2")
        for iotdb_home in (None, str(self.root / "iotdb")):
            with self.subTest(IOTDB_HOME=iotdb_home):
                self.link("iotdb", "edge-v1")
                pid = self.start(
                    self.started_home("iotdb/sbin/start-edge.sh", iotdb_home)
                )
                self.link("iotdb", "edge-v2")
                self.stop("iotdb/sbin/stop-edge.sh", iotdb_home)
                self.assertFalse(self.running(pid))

    def test_relative_home_of_another_installation_is_left_alone(self):
        # Recorded as-is by the start-edge.sh of earlier releases.
        pid = self.start(".", cwd=self.install("edge-a"))
        self.install("edge-b")
        for iotdb_home in (None, "."):
            with self.subTest(IOTDB_HOME=iotdb_home):
                self.stop("sbin/stop-edge.sh", iotdb_home, cwd=self.root / "edge-b")
                self.assertTrue(self.running(pid))

    def test_symlink_followed_by_dot_dot_in_the_home(self):
        self.symlink_layout()
        given = str(self.root / "x/link/../edge")
        home = self.started_home("y/edge/sbin/start-edge.sh", given)
        self.assertEqual(home, given)
        pid = self.start(home)
        self.stop("x/edge/sbin/stop-edge.sh")
        self.assertTrue(self.running(pid))
        self.stop("y/edge/sbin/stop-edge.sh")
        self.assertFalse(self.running(pid))

    def test_symlink_followed_by_dot_dot_in_the_script_path(self):
        self.symlink_layout()
        pid = self.start(self.started_home("x/link/../edge/sbin/start-edge.sh"))
        self.stop("x/link/../edge/sbin/stop-edge.sh")
        self.assertFalse(self.running(pid))

    def test_process_without_iotdb_home_is_not_matched(self):
        home = self.install("edge")
        # Everything before the first " -D" names the installation, and edge.pid
        # points at the process.
        pid = self.spawn(
            [
                "perl",
                "-e",
                "$0 = shift; sleep 600",
                "{} -Dfoo=1 {} org.apache.iotdb.edge.EdgeNode -s".format(
                    home, self.marker
                ),
            ]
        )
        for _ in range(100):
            if self.command(pid).startswith(str(home)):
                break
            time.sleep(0.05)
        (home / "edge.pid").write_text("{}\n".format(pid))
        self.stop("edge/sbin/stop-edge.sh")
        self.assertTrue(self.running(pid))

    def test_cdpath_is_not_used(self):
        self.install("edge")
        (self.root / "elsewhere/edge/sbin").mkdir(parents=True)
        cdpath = {"CDPATH": str(self.root / "elsewhere")}
        home = self.started_home("edge/sbin/start-edge.sh", **cdpath)
        self.assertEqual(home, str(self.root / "edge"))
        pid = self.start(home)
        self.stop("edge/sbin/stop-edge.sh", **cdpath)
        self.assertFalse(self.running(pid))

    def test_home_of_another_installation_is_reported(self):
        self.install("edge-a")
        own = self.install("edge-b")
        result = self.stop("edge-b/sbin/stop-edge.sh", str(self.root / "edge-a"))
        self.assertIn("belongs to {}".format(own), result.stderr)

    def test_missing_home_is_an_error(self):
        self.install("edge")
        result = self.bash(("edge/sbin/stop-edge.sh",), str(self.root / "missing"))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("not an accessible directory", result.stderr)


class AssemblyTest(unittest.TestCase):
    @staticmethod
    def packaged(descriptor, path):
        root = ET.parse(REPOSITORY / descriptor).getroot()
        for file_set in root.findall("./fileSets/fileSet"):
            directory = file_set.findtext("directory", "")
            if not directory.endswith("/scripts/tools"):
                continue
            includes = [node.text for node in file_set.findall("./includes/include")]
            excludes = [node.text for node in file_set.findall("./excludes/exclude")]
            if (
                not includes or any(fnmatch.fnmatchcase(path, p) for p in includes)
            ) and not any(fnmatch.fnmatchcase(path, p) for p in excludes):
                return file_set.findtext("fileMode")
        return None

    def test_edge_ops_are_executable_and_only_in_edge_packages(self):
        for path in (
            "ops/daemon-edge.sh",
            "ops/destroy-edge.sh",
            "windows/ops/destroy-edge.bat",
        ):
            self.assertEqual(
                self.packaged("distribution/src/assembly/edge.xml", path), "0755"
            )
            for descriptor in (
                "distribution/src/assembly/all.xml",
                "iotdb-core/datanode/src/assembly/server.xml",
                "iotdb-core/confignode/src/assembly/confignode.xml",
            ):
                self.assertIsNone(
                    self.packaged(descriptor, path), descriptor + ": " + path
                )

    def test_edge_does_not_restore_standalone_ops(self):
        paths = ["ops/daemon-{}.sh".format(node) for node in ("confignode", "datanode")]
        for node in ("all", "confignode", "datanode"):
            paths.extend(
                (
                    "ops/destroy-{}.sh".format(node),
                    "windows/ops/destroy-{}.bat".format(node),
                )
            )
        for path in paths:
            self.assertIsNone(
                self.packaged("distribution/src/assembly/edge.xml", path), path
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
