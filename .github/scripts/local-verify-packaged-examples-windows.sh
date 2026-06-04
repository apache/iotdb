#!/usr/bin/env bash
# Mirror package-windows "Verify packaged examples" from unzip through example exes.
# Usage: local-verify-packaged-examples-windows.sh <path-to-iotdb-session-cpp.zip> [workspace]
set -euxo pipefail

PKG_TARBALL="${1:?zip path required}"
GITHUB_WORKSPACE="${2:-$(cd "$(dirname "$0")/../.." && pwd)}"
CMAKE_GENERATOR="${CMAKE_GENERATOR:-}"
SKIP_DIST_MVN="${SKIP_DIST_MVN:-0}"

export RUNNER_OS=Windows
export RUNNER_TEMP="${RUNNER_TEMP:-${TEMP:-/tmp}}"
TEMP_BASE="$(cygpath -u "${RUNNER_TEMP}")"
WORKSPACE_BASE="$(cygpath -u "${GITHUB_WORKSPACE}")"

echo "PKG_TARBALL=${PKG_TARBALL}"
echo "WORKSPACE_BASE=${WORKSPACE_BASE}"
echo "TEMP_BASE=${TEMP_BASE}"

PKG_UNPACK="${TEMP_BASE}/client-cpp-package"
rm -rf "${PKG_UNPACK}"
mkdir -p "${PKG_UNPACK}"
unzip -q -o "${PKG_TARBALL}" -d "${PKG_UNPACK}"
# GitHub artifact downloads may be a wrapper zip containing the real package zip + .sha512.
NESTED_ZIP=$(find "${PKG_UNPACK}" -maxdepth 1 -type f -name 'iotdb-session-cpp-*.zip' ! -name '*.sha512' -print -quit)
if [ -n "${NESTED_ZIP}" ]; then
  echo "Unpacking nested SDK zip: ${NESTED_ZIP}"
  unzip -q -o "${NESTED_ZIP}" -d "${PKG_UNPACK}"
fi
PKG_ROOT=$(find "${PKG_UNPACK}" -mindepth 1 -maxdepth 1 -type d -name 'iotdb-session-cpp-*' -print -quit)
test -n "${PKG_ROOT}"
echo "PKG_ROOT=${PKG_ROOT}"

EXAMPLE_BUILD="${TEMP_BASE}/client-cpp-example-smoke"
rm -rf "${EXAMPLE_BUILD}"
CMAKE_ARGS=(-S "${PKG_ROOT}/examples" -B "${EXAMPLE_BUILD}" -DIOTDB_SDK_ROOT="${PKG_ROOT}")
if [ -n "${CMAKE_GENERATOR}" ]; then
  CMAKE_ARGS+=(-G "${CMAKE_GENERATOR}")
fi
cmake "${CMAKE_ARGS[@]}"
cmake --build "${EXAMPLE_BUILD}" --config Release

if [ "${SKIP_DIST_MVN}" = "0" ]; then
  (cd "${WORKSPACE_BASE}" && ./mvnw -pl distribution -am -DskipTests -Dspotless.skip=true package)
else
  echo "SKIP_DIST_MVN=1: skipping ./mvnw -pl distribution package"
fi

SERVER_ROOT=$(find "${WORKSPACE_BASE}/distribution/target" -path '*/apache-iotdb-*-all-bin/sbin/windows/start-standalone.bat' -print -quit | sed 's#/sbin/windows/start-standalone.bat##')
if [ -z "${SERVER_ROOT}" ]; then
  echo "ERROR: no distribution under distribution/target (set SKIP_DIST_MVN=0 to build)"
  exit 1
fi
echo "SERVER_ROOT=${SERVER_ROOT}"

START_BAT="$(cygpath -w "${SERVER_ROOT}/sbin/windows/start-standalone.bat")"
STOP_BAT="$(cygpath -w "${SERVER_ROOT}/sbin/windows/stop-standalone.bat")"
cmd.exe //c "${START_BAT}"
trap "cmd.exe //c \"${STOP_BAT}\" || true" EXIT

echo "Waiting for IoTDB RPC on 127.0.0.1:6667..."
ready=0
for _ in $(seq 1 60); do
  if powershell.exe -NoProfile -Command \
    '(Test-NetConnection -ComputerName 127.0.0.1 -Port 6667 -WarningAction SilentlyContinue).TcpTestSucceeded' \
    | grep -qi True; then
    ready=1
    break
  fi
  sleep 2
done
if [ "${ready}" -ne 1 ]; then
  echo "ERROR: IoTDB did not listen on 6667 within 120s"
  ls -la "${SERVER_ROOT}/logs" 2>/dev/null || true
  exit 1
fi

for exe in SessionExample.exe AlignedTimeseriesSessionExample.exe TableModelSessionExample.exe tree_example.exe table_example.exe; do
  EXE=$(find "${EXAMPLE_BUILD}" -name "${exe}" -print -quit)
  test -n "${EXE}"
  echo "Running ${EXE}"
  "${EXE}"
done
echo "OK: local Windows verify packaged examples smoke passed"
