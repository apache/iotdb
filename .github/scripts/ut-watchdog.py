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

"""Run UTs across Actions steps so diagnostics can upload before Maven exits."""

import argparse
import datetime
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def publish(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(str(value), encoding="utf-8")
    temporary.replace(path)


def jdk_tool(name):
    suffix = ".exe" if os.name == "nt" else ""
    return str(Path(os.environ["JAVA_HOME"]) / "bin" / (name + suffix))


def stop_tree(process):
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait(timeout=10)


def cancelled(state):
    return (state / "stop").exists()


def capture(command, destination, timeout, test, state):
    # Keep full jstack output on disk; never hold an unbounded dump in memory.
    with destination.open("w", encoding="utf-8") as output:
        output.write(f"UTC={stamp()} tool={Path(command[0]).name} pid={command[-1]}\n")
        output.flush()
        try:
            proc = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                    start_new_session=os.name != "nt")
        except OSError as exc:
            output.write(f"Unable to start diagnostic tool: {exc}\n")
            return
        deadline = time.monotonic() + timeout
        try:
            while proc.poll() is None:
                if test.poll() is not None or cancelled(state) or time.monotonic() >= deadline:
                    output.write("Diagnostic interrupted (test ended, cancellation, or attach timeout).\n")
                    break
                time.sleep(0.1)
        finally:
            if proc.poll() is None:
                stop_tree(proc)
        output.write(f"UTC={stamp()} diagnostic_exit={proc.returncode}\n")


def collect(args, test, marker):
    state = args.state
    diagnostics = state / "diagnostics"
    diagnostics.mkdir(exist_ok=True)
    # jps has the worker's environment, not the marked test environment.
    for round_number in range(1, 4):
        if test.poll() is not None or cancelled(state):
            break
        started = time.monotonic()
        listing = state / "jps.txt"
        capture([jdk_tool("jps"), "-v"], listing, args.attach_timeout, test, state)
        lines = listing.read_text(encoding="utf-8", errors="replace").splitlines()
        pids = sorted({
            fields[0] for line in lines
            if (fields := line.split()) and fields[0].isdigit() and marker in fields
        })
        # Do not retain VM arguments: they may contain credentials.
        discovery_status = lines[-1:]
        listing.unlink()
        with (diagnostics / "discovery.txt").open("a", encoding="utf-8") as log:
            log.write(f"UTC={stamp()} round={round_number} pids={','.join(pids) or 'none'} status={discovery_status}\n")
        for pid in pids:
            if test.poll() is not None or cancelled(state):
                break
            capture([jdk_tool("jstack"), "-l", pid],
                    diagnostics / f"round-{round_number}-pid-{pid}.txt",
                    args.attach_timeout, test, state)
        while round_number < 3 and time.monotonic() < started + args.interval:
            if test.poll() is not None or cancelled(state):
                return
            time.sleep(0.1)


def worker(args):
    state = args.state
    marker = "-Diotdb.ut.watchdog=" + uuid.uuid4().hex
    env = os.environ.copy()
    env["JAVA_TOOL_OPTIONS"] = (env.get("JAVA_TOOL_OPTIONS", "") + " " + marker).strip()
    command = json.loads((state / "command.json").read_text(encoding="utf-8"))
    test = None
    result = 1
    try:
        test = subprocess.Popen(command, env=env, start_new_session=os.name != "nt")
        deadline = time.monotonic() + args.threshold
        while test.poll() is None and not cancelled(state) and time.monotonic() < deadline:
            time.sleep(0.1)
        if test.poll() is None and not cancelled(state):
            print(f"UT diagnostic threshold reached at {stamp()}", flush=True)
            try:
                collect(args, test, marker)
            except Exception as exc:
                # Diagnostics must never replace the test result.
                print(f"UT diagnostics failed at {stamp()}: {exc}", flush=True)
                try:
                    (state / "diagnostics").mkdir(exist_ok=True)
                    (state / "diagnostics" / "error.txt").write_text(
                        f"{stamp()} {exc}\n", encoding="utf-8")
                except OSError:
                    pass
        publish(state / "ready", "ready")
        while test.poll() is None and not cancelled(state):
            time.sleep(0.1)
        if cancelled(state) and test.poll() is None:
            stop_tree(test)
        result = test.wait()
    finally:
        if test is not None and test.poll() is None:
            stop_tree(test)
        publish(state / "exit", result)
        publish(state / "ready", "ready")


def wait(args, ready):
    state = args.state
    offset = int((state / "offset").read_text()) if (state / "offset").exists() else 0
    try:
        with (state / "test.log").open("rb") as log:
            log.seek(offset)
            while True:
                data = log.read(65536)
                if data:
                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()
                if (state / ("ready" if ready else "exit")).exists() and (ready or not data):
                    publish(state / "offset", log.tell())
                    break
                elif not data:
                    time.sleep(0.1)
    except BaseException:
        publish(state / "stop", "stop")
        raise
    if ready:
        return 0
    result = int((state / "exit").read_text())
    return result if result >= 0 else 128 - result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["start", "worker", "wait", "stop"])
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--threshold", type=float, default=10800)
    parser.add_argument("--interval", type=float, default=30)
    parser.add_argument("--attach-timeout", type=float, default=10)
    # Command is explicitly separated from watchdog options.
    argv = sys.argv[1:]
    split = argv.index("--") if "--" in argv else len(argv)
    args = parser.parse_args(argv[:split])
    args.command = argv[split + 1:]
    args.state = args.state.resolve()
    if args.threshold < 0 or args.interval < 0 or args.attach_timeout <= 0:
        parser.error("threshold/interval must be nonnegative and attach-timeout positive")
    if args.mode == "start" and not args.command:
        parser.error("start requires a command after --")
    if args.mode == "start":
        args.state.mkdir(parents=True, exist_ok=False)
        publish(args.state / "command.json", json.dumps(args.command))
        with (args.state / "test.log").open("wb") as log:
            subprocess.Popen([sys.executable, __file__, "worker", "--state", str(args.state),
                              "--threshold", str(args.threshold), "--interval", str(args.interval),
                              "--attach-timeout", str(args.attach_timeout)],
                             stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                             start_new_session=os.name != "nt")
        return wait(args, ready=True)
    if args.mode == "worker":
        worker(args)
        return 0
    if args.mode == "stop":
        if args.state.exists():
            publish(args.state / "stop", "stop")
            deadline = time.monotonic() + 15
            while not (args.state / "exit").exists():
                if time.monotonic() >= deadline:
                    print("UT worker did not acknowledge cleanup within 15 seconds", file=sys.stderr)
                    return 1
                time.sleep(0.1)
        return 0
    return wait(args, ready=False)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    sys.exit(main())
