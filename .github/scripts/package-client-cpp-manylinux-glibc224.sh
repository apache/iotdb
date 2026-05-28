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
# manylinux_2_24 baseline; explicit CXX11 libstdc++ ABI; max glibc 2.24.
set -euxo pipefail

MAX_GLIBC=2.24

MACHINE=$(uname -m)
case "${MACHINE}" in
  x86_64)
    CMAKE_PKG_ARCH=linux-x86_64
    JDK_API_ARCH=linux/x64
    DEFAULT_CLASSIFIER=linux-x86_64-glibc224
    ;;
  aarch64)
    CMAKE_PKG_ARCH=linux-aarch64
    JDK_API_ARCH=linux/aarch64
    DEFAULT_CLASSIFIER=linux-aarch64-glibc224
    ;;
  *)
    echo "Unsupported architecture: ${MACHINE}" >&2
    exit 1
    ;;
esac

PACKAGE_CLASSIFIER="${PACKAGE_CLASSIFIER:-${DEFAULT_CLASSIFIER}}"

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

echo "=== Toolchain ==="
command -v gcc g++ || true
gcc --version | head -1
g++ --version | head -1
g++ -dM -E -x c++ /dev/null | grep GLIBCXX_USE_CXX11_ABI || true
cmake --version
java -version

cd "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is not set}"
./mvnw clean package -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
  -Dspotless.skip=true \
  -Dclient.cpp.package.classifier="${PACKAGE_CLASSIFIER}" \
  -Diotdb.libstdcxx.cxx11.abi=ON

SO="iotdb-client/client-cpp/target/install/lib/libiotdb_session.so"
test -f "${SO}"

echo "=== libstdc++ ABI check (cxx11: mangled names must contain __cxx11) ==="
# Use mangled symbol names: "nm -C" demangles std::__cxx11::basic_string to std::string
# and hides __cxx11 even when the library was built with the new ABI.
_abi_has_cxx11() {
  nm -a "$1" 2>/dev/null | grep -q '__cxx11'
}
if _abi_has_cxx11 "${SO}"; then
  echo "ABI check passed (__cxx11 in ${SO})"
else
  SESSION_OBJ=$(find "${GITHUB_WORKSPACE}/iotdb-client/client-cpp/target" \
    -path '*/iotdb_session.dir/src/session/Session.cpp.o' -print -quit 2>/dev/null || true)
  if [[ -n "${SESSION_OBJ}" ]] && _abi_has_cxx11 "${SESSION_OBJ}"; then
    echo "ABI check passed (__cxx11 in ${SESSION_OBJ}; .so has minimal symbol table)"
  else
    echo "ERROR: cxx11 ABI build must contain __cxx11 symbols in ${SO} (or Session.cpp.o)"
    exit 1
  fi
fi

echo "=== Build host glibc ==="
ldd --version 2>&1 | sed -n '1p'

echo "=== Highest GLIBC_* symbols in libiotdb_session.so ==="
objdump -T "${SO}" | grep GLIBC_ | sed "s/.*GLIBC_/GLIBC_/" | sort -Vu | tail -10

max_glibc=$(objdump -T "${SO}" | grep -oE "GLIBC_[0-9.]+" | sed "s/GLIBC_//" | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)
echo "max_glibc=${max_glibc}"

if awk -v max="${max_glibc}" -v limit="${MAX_GLIBC}" 'BEGIN { exit !(max > limit) }'; then
  echo "ERROR: libiotdb_session.so requires glibc > ${MAX_GLIBC} (max=${max_glibc})"
  exit 1
fi

echo "glibc compatibility check passed (max=${max_glibc} <= ${MAX_GLIBC})"

echo "=== Example link test (default g++) ==="
INSTALL_ROOT="${GITHUB_WORKSPACE}/iotdb-client/client-cpp/target/install"
EXAMPLE_SRC="${GITHUB_WORKSPACE}/example/client-cpp-example/src"
LINKTEST_BUILD="/tmp/client-cpp-example-linktest-glibc224"
rm -rf "${LINKTEST_BUILD}"
mkdir -p "${LINKTEST_BUILD}"
unset CC CXX CFLAGS CXXFLAGS
"${CMAKE_DIR}/bin/cmake" -S "${EXAMPLE_SRC}" -B "${LINKTEST_BUILD}" \
  -DCMAKE_BUILD_TYPE=Release \
  -DIOTDB_SDK_ROOT="${INSTALL_ROOT}"
"${CMAKE_DIR}/bin/cmake" --build "${LINKTEST_BUILD}" --target SessionExample -j"$(nproc)"
test -x "${LINKTEST_BUILD}/SessionExample"
echo "Example link test passed"
