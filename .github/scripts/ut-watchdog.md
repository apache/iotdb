<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Long-running unit-test diagnostics

`unit-test.yml` and `daily-ut.yml` share the local `ut-watchdog` composite action.
It requires Python 3.8+, bash and the `JAVA_HOME` JDK supplied by setup-java.
There are no Python packages or additional remote services to install.
The existing OS matrices and Maven arguments are unchanged, including daily macOS
and the different datanode fork settings.

## Lifecycle

1. `start` creates a worker and tails its test log. The timer starts when the
   worker launches the UT command. Checkout, setup/cache and the separate
   `mvn clean install -DskipTests` step are outside this timer.
2. If the command is still running after 10,800 seconds, the worker captures
   three rounds of `jstack -l`, with round starts at least 30 seconds apart.
   Every round runs `jps -v` again. A unique JVM property, inherited through
   `JAVA_TOOL_OPTIONS`, selects this invocation's Maven and Surefire JVMs,
   including new forks. It does not assume a fork count. Other child JVMs that
   inherit the property are also included.
3. Every discovery/attach operation has a 10-second timeout. Each dump includes
   UTC timestamps, PID, full stdout/stderr and tool exit status. Discovery records
   round numbers, discovered PIDs and tool status. Raw JVM argument listings are
   removed and excluded from the artifact. Diagnostic subprocesses do not inherit
   `JAVA_TOOL_OPTIONS`, `_JAVA_OPTIONS` or `JDK_JAVA_OPTIONS`, preventing tool JVMs
   from echoing those option values into the artifact. This does not change the
   test JVM's options. Discovery status is generated explicitly, never copied
   from a raw JVM argument line.
4. Once collection completes (or the test exits), `start` returns successfully.
   Maven can still be running. The next action step uploads `diagnostics/` using
   the repository's existing `actions/upload-artifact@v6` dependency. Upload is
   best effort and cannot replace the test result.
5. `wait` resumes log streaming and returns the original command's exit code
   (POSIX signal exits use the conventional 128 + signal). The worker and active
   diagnostic tools stop promptly if the command finishes. Cancellation requests
   stop the command tree; POSIX uses a dedicated process group and Windows uses
   `taskkill /T /F`. The final cleanup step requests worker shutdown and waits
   up to 15 seconds for acknowledgement.

The diagnostic threshold neither kills Maven nor fails the UT. In particular,
artifact upload does not depend on Maven eventually returning or on job-timeout
cleanup. A permanently hung test still remains subject to the existing Actions
job timeout. Upload cannot be guaranteed after runner loss, cancellation before
capture, a job timeout before the threshold, or an artifact-service failure.
The worker retains the runner's tracking environment so normal runner job cleanup
continues to apply. Temporary command/state/test-log files live in `runner.temp`;
only the diagnostics directory is uploaded.

JVM discovery relies on the JDK's local JVM instrumentation and inherited
`JAVA_TOOL_OPTIONS`. A JVM disabling perf data/attach, clearing that environment
variable, or exiting between discovery and attach may not yield a dump. Failures
are recorded without changing the test result. This mechanism does not modify
JUnit or the IT timeout framework.

## Local checks

```bash
python3 .github/scripts/test-ut-watchdog.py
python3 -m py_compile .github/scripts/ut-watchdog.py .github/scripts/test-ut-watchdog.py
git diff --check
```

All thresholds in the tests are seconds, not hours. The suite exercises early
success, original failure and signal exit codes, the upload boundary while the
command is still alive, discovery of multiple/changing PIDs, missing tools,
attach failure/timeout, interrupted collection and cancellation/cleanup. The
real Java test uses the installed JDK modules through temporary POSIX launchers,
starts two small JVMs, replaces one between rounds and checks six full dumps.
It skips explicitly if those modules are unavailable. To retain its dumps:

```bash
UT_WATCHDOG_EVIDENCE=/tmp/ut-watchdog-evidence python3 .github/scripts/test-ut-watchdog.py
```

Linux process behavior and Java 21 JDK-module stack capture have been tested in
the cloud environment. Windows `taskkill` arguments are simulated; Windows
process lifetime, `.exe` invocation, Git Bash, and native JDK 17 attach are not
locally exercised. macOS is retained and uses the POSIX path but has not been run
here. YAML parsing and unchanged workflow commands/matrices were checked locally.
No IoTDB full build, remote CI run, or real artifact upload was performed.

## Review boundaries

The worker redirects all standard handles to a local file or the null device;
POSIX workers start a new session. Local tests confirm that the `start` process
can return while the worker/test remain alive, followed by a separate `wait`
process. This is not an end-to-end validation of GitHub runner process handling.
The normal runner tracking environment is retained for job cleanup.

Exit codes have been verified with real Python and bash commands, including
nonzero exits and POSIX signals. Maven is not installed in this cloud image, so
an actual Maven invocation has not been tested. The workflow's original Maven
command is passed unchanged through `bash -e -o pipefail -c`.

The wrapper does not enumerate or print environment variables and does not
upload Maven logs, command files, or raw `jps` listings. GitHub log masking still
applies to streamed test stdout. A full JVM thread dump can itself contain
application-defined thread names or exception text; this implementation cannot
guarantee those application-controlled strings are free of secrets, and artifact
contents are not covered by GitHub's console log masking.
