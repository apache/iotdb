#!/usr/bin/env bash
set -euxo pipefail

PKG_ZIP="${1:-/c/Users/76141/Downloads/iotdb-session-cpp-2.0.7-SNAPSHOT-windows-x86_64-msvc14.3.zip}"
REPO_EXAMPLES="${2:-/d/workspace/iotdb/iotdb-client/client-cpp/examples}"
GEN='Visual Studio 15 2017'

TMP=/tmp/vs2017-smoke
rm -rf "${TMP}" && mkdir -p "${TMP}/outer"
unzip -q -o "${PKG_ZIP}" -d "${TMP}/outer"
INNER=$(find "${TMP}/outer" -maxdepth 1 -name 'iotdb-session-cpp-*.zip' ! -name '*.sha512' -print -quit)
unzip -q -o "${INNER}" -d "${TMP}"
PKG_ROOT=$(find "${TMP}" -mindepth 1 -maxdepth 1 -type d -name 'iotdb-session-cpp-*' -print -quit)
echo "PKG_ROOT=${PKG_ROOT}"

cl_path_from_cache() {
  grep -m1 'CMAKE_CXX_COMPILER:FILEPATH=' "$1/CMakeCache.txt" | cut -d= -f2 || true
}

echo "=== without -A x64 (packaged examples, reproduces CI bug) ==="
B1=/tmp/vs2017-no-a
rm -rf "${B1}"
cmake -S "${PKG_ROOT}/examples" -B "${B1}" -DIOTDB_SDK_ROOT="${PKG_ROOT}" -G "${GEN}"
echo "cl: $(cl_path_from_cache "${B1}")"

echo "=== with -A x64 (workflow fix on packaged examples) ==="
B2=/tmp/vs2017-a-x64
rm -rf "${B2}"
cmake -S "${PKG_ROOT}/examples" -B "${B2}" -DIOTDB_SDK_ROOT="${PKG_ROOT}" -G "${GEN}" -A x64
echo "cl: $(cl_path_from_cache "${B2}")"
cmake --build "${B2}" --config Release --target SessionExample

echo "=== repo examples CMakeLists (CMAKE fix, no -A on cmd) ==="
B3=/tmp/vs2017-cmakefix
rm -rf "${B3}"
cmake -S "${REPO_EXAMPLES}" -B "${B3}" -DIOTDB_SDK_ROOT="${PKG_ROOT}" -G "${GEN}"
echo "cl: $(cl_path_from_cache "${B3}")"
cmake --build "${B3}" --config Release --target SessionExample

echo "OK: VS2017 x64 example build verified"
