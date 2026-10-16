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
# Build client-cpp in manylinux_2_28 and verify max required GLIBC symbol <= 2.28.
set -euxo pipefail

MACHINE=$(uname -m)
case "${MACHINE}" in
  x86_64)
    CMAKE_PKG_ARCH=linux-x86_64
    JDK_API_ARCH=linux/x64
    DEFAULT_CLASSIFIER=linux-x86_64-glibc2.28
    ;;
  aarch64)
    CMAKE_PKG_ARCH=linux-aarch64
    JDK_API_ARCH=linux/aarch64
    DEFAULT_CLASSIFIER=linux-aarch64-glibc2.28
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

gcc --version
c++ --version
gcc_major=$(gcc -dumpversion | cut -d. -f1)
if (( gcc_major < 14 )); then
  echo "ERROR: GCC >= 14 is required; got $(gcc -dumpversion)"
  exit 1
fi
cmake --version
java -version

cd "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is not set}"
./mvnw clean package -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
  -Dspotless.skip=true \
  -Dclient.cpp.package.classifier="${PACKAGE_CLASSIFIER}"

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

if awk -v max="${max_glibc}" "BEGIN { exit !(max > 2.28) }"; then
  echo "ERROR: libiotdb_session.so requires glibc > 2.28 (max=${max_glibc})"
  exit 1
fi

echo "glibc compatibility check passed (max=${max_glibc} <= 2.28)"
