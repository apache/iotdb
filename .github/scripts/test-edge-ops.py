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
import subprocess
import tempfile
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
                "distribution/src/assembly/datanode.xml",
                "distribution/src/assembly/confignode.xml",
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
