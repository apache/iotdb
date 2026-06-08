#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with this
# work for additional information regarding copyright ownership.  The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the
# License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Build client-cpp in manylinux2014 and verify max required GLIBC symbol <= 2.17.
set -euxo pipefail

MACHINE=$(uname -m)
case "${MACHINE}" in
  x86_64)
    CMAKE_PKG_ARCH=linux-x86_64
    JDK_API_ARCH=linux/x64
    DEFAULT_CLASSIFIER=linux-x86_64-glibc2.17
    ;;
  aarch64)
    CMAKE_PKG_ARCH=linux-aarch64
    JDK_API_ARCH=linux/aarch64
    DEFAULT_CLASSIFIER=linux-aarch64-glibc2.17
    ;;
  *)
    echo "Unsupported architecture: ${MACHINE}" >&2
    exit 1
    ;;
esac

PACKAGE_CLASSIFIER="${PACKAGE_CLASSIFIER:-${DEFAULT_CLASSIFIER}}"

# manylinux2014 is glibc 2.17 based, but its system GCC may be too old to
# provide libstdc++'s dual ABI. Use the newest available devtoolset so
# _GLIBCXX_USE_CXX11_ABI=1 produces std::__cxx11 symbols.
for devtoolset in devtoolset-12 devtoolset-11 devtoolset-10 devtoolset-9 devtoolset-8 devtoolset-7; do
  if [[ -f "/opt/rh/${devtoolset}/enable" ]]; then
    # shellcheck disable=SC1090
    set +u
    source "/opt/rh/${devtoolset}/enable"
    set -u
    echo "Enabled ${devtoolset}"
    break
  fi
done

CMAKE_VERSION=3.28.4
CMAKE_DIR="/opt/cmake-${CMAKE_VERSION}"
if [[ ! -x "${CMAKE_DIR}/bin/cmake" ]]; then
  curl -fsSL -o /tmp/cmake.tar.gz "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-${CMAKE_PKG_ARCH}.tar.gz"
  rm -rf "${CMAKE_DIR}"
  mkdir -p /opt
  tar xf /tmp/cmake.tar.gz -C /opt
  mv "/opt/cmake-${CMAKE_VERSION}-${CMAKE_PKG_ARCH}" "${CMAKE_DIR}"
fi

JAVA_HOME=/opt/jdk-17
if [[ ! -x "${JAVA_HOME}/bin/java" ]]; then
  curl -fsSL -o /tmp/jdk17.tar.gz "https://api.adoptium.net/v3/binary/latest/17/ga/${JDK_API_ARCH}/jdk/hotspot/normal/eclipse?project=jdk"
  rm -rf /opt/jdk-17*
  mkdir -p /opt
  tar xf /tmp/jdk17.tar.gz -C /opt
  JAVA_HOME=$(find /opt -maxdepth 1 -type d -name 'jdk-17*' -print -quit)
  ln -sfn "${JAVA_HOME}" /opt/jdk-17
  JAVA_HOME=/opt/jdk-17
fi

export PATH="${CMAKE_DIR}/bin:${JAVA_HOME}/bin:${PATH}"
export JAVA_HOME

gcc --version
c++ --version
gcc_major=$(gcc -dumpversion | cut -d. -f1)
if (( gcc_major < 5 )); then
  echo "ERROR: GCC >= 5 is required for _GLIBCXX_USE_CXX11_ABI=1; got $(gcc -dumpversion)"
  exit 1
fi
cmake --version
java -version

cd "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is not set}"
./mvnw clean package -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
  -Dspotless.skip=true \
  -Dclient.cpp.package.classifier="${PACKAGE_CLASSIFIER}" \
  -Diotdb.cxx11.abi=1

SO="iotdb-client/client-cpp/target/install/lib/libiotdb_session.so"
test -f "${SO}"

echo "=== Build host glibc ==="
ldd --version 2>&1 | sed -n '1p'

echo "=== Highest GLIBC_* symbols in libiotdb_session.so ==="
objdump -T "${SO}" | grep GLIBC_ | sed "s/.*GLIBC_/GLIBC_/" | sort -Vu | tail -10

max_glibc=$(objdump -T "${SO}" | grep -oE "GLIBC_[0-9.]+" | sed "s/GLIBC_//" | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)
echo "max_glibc=${max_glibc}"

if [[ -z "${max_glibc}" ]]; then
  echo "ERROR: could not determine max GLIBC version from ${SO}"
  exit 1
fi

if awk -v max="${max_glibc}" "BEGIN { exit !(max > 2.17) }"; then
  echo "ERROR: libiotdb_session.so requires glibc > 2.17 (max=${max_glibc})"
  exit 1
fi

echo "glibc compatibility check passed (max=${max_glibc} <= 2.17)"

echo "=== CXX11 ABI symbols in libiotdb_session.so ==="
abi_symbol_sample=$(nm -D --demangle "${SO}" | awk '/std::__cxx11|\[abi:cxx11\]/ { print; if (++count >= 20) exit }' || true)
if [[ -n "${abi_symbol_sample}" ]]; then
  printf '%s\n' "${abi_symbol_sample}"
else
  echo "No std::__cxx11 symbols found in the dynamic symbol table; verifying by link test."
fi

ABI_SMOKE="/tmp/client-cpp-abi-smoke-glibc217"
rm -rf "${ABI_SMOKE}"
mkdir -p "${ABI_SMOKE}"
cat > "${ABI_SMOKE}/main.cpp" <<'EOF'
#include "Session.h"

#include <string>

int main() {
  Session session(std::string("127.0.0.1"), 6667, std::string("root"), std::string("root"));
  session.setDatabase(std::string("root.test"));
  return 0;
}
EOF

if ! c++ -std=c++11 -D_GLIBCXX_USE_CXX11_ABI=1 \
    -I"iotdb-client/client-cpp/target/install/include" \
    "${ABI_SMOKE}/main.cpp" \
    -L"iotdb-client/client-cpp/target/install/lib" \
    -Wl,-rpath,"${GITHUB_WORKSPACE}/iotdb-client/client-cpp/target/install/lib" \
    -liotdb_session -pthread \
    -o "${ABI_SMOKE}/abi-smoke"; then
  echo "ERROR: libiotdb_session.so is not link-compatible with _GLIBCXX_USE_CXX11_ABI=1"
  exit 1
fi
echo "CXX11 ABI link check passed"

echo "=== Example package build/link/run smoke test ==="
PKG_ZIP=$(find "${GITHUB_WORKSPACE}/iotdb-client/client-cpp/target" -maxdepth 1 -type f -name "iotdb-session-cpp-*-${PACKAGE_CLASSIFIER}.zip" -print -quit)
if [[ -z "${PKG_ZIP}" ]]; then
  echo "ERROR: could not find package zip for ${PACKAGE_CLASSIFIER}"
  exit 1
fi
PKG_UNPACK="/tmp/client-cpp-package-smoke-glibc217"
rm -rf "${PKG_UNPACK}"
mkdir -p "${PKG_UNPACK}"
unzip -q -o "${PKG_ZIP}" -d "${PKG_UNPACK}"
PKG_ROOT=$(find "${PKG_UNPACK}" -mindepth 1 -maxdepth 1 -type d -name "iotdb-session-cpp-*-${PACKAGE_CLASSIFIER}" -print -quit)
if [[ -z "${PKG_ROOT}" ]]; then
  echo "ERROR: could not find unpacked package directory for ${PACKAGE_CLASSIFIER}"
  exit 1
fi
EXAMPLE_BUILD="/tmp/client-cpp-example-smoke-glibc217"
rm -rf "${EXAMPLE_BUILD}"
mkdir -p "${EXAMPLE_BUILD}"
unset CC CXX CFLAGS CXXFLAGS
"${CMAKE_DIR}/bin/cmake" -S "${PKG_ROOT}/examples" -B "${EXAMPLE_BUILD}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DIOTDB_SDK_ROOT="${PKG_ROOT}"
"${CMAKE_DIR}/bin/cmake" --build "${EXAMPLE_BUILD}" -j"$(nproc)"

./mvnw -pl distribution -am -DskipTests -Dspotless.skip=true package
SERVER_ROOT=$(find "${GITHUB_WORKSPACE}/distribution/target" -path '*/apache-iotdb-*-all-bin/sbin/start-standalone.sh' -print -quit | sed 's#/sbin/start-standalone.sh##')
if [[ -z "${SERVER_ROOT}" ]]; then
  echo "ERROR: could not find IoTDB distribution under distribution/target"
  exit 1
fi
"${SERVER_ROOT}/sbin/start-standalone.sh"
trap '"${SERVER_ROOT}/sbin/stop-standalone.sh" || true' EXIT
sleep 30
for example in SessionExample AlignedTimeseriesSessionExample TableModelSessionExample tree_example table_example; do
  test -x "${EXAMPLE_BUILD}/${example}"
  "${EXAMPLE_BUILD}/${example}"
done
echo "Example package smoke test passed"
