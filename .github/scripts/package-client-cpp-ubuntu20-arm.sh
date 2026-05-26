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
# Build client-cpp on Ubuntu 20.04 aarch64 (glibc 2.31 baseline).
set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y build-essential wget git ca-certificates \
  openjdk-17-jdk binutils

# Ubuntu 20.04 ships CMake 3.16; FetchBoost.cmake needs ARCHIVE_EXTRACT (3.18+).
CMAKE_VERSION=3.28.4
CMAKE_DIR=/opt/cmake-${CMAKE_VERSION}
if [[ ! -x "${CMAKE_DIR}/bin/cmake" ]]; then
  wget -q "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-aarch64.tar.gz" \
    -O /tmp/cmake.tar.gz
  rm -rf "${CMAKE_DIR}"
  mkdir -p /opt
  tar xf /tmp/cmake.tar.gz -C /opt
  mv "/opt/cmake-${CMAKE_VERSION}-linux-aarch64" "${CMAKE_DIR}"
fi

JAVA_BIN=$(readlink -f "$(command -v java)")
export JAVA_HOME=$(dirname "$(dirname "${JAVA_BIN}")")
export PATH="${CMAKE_DIR}/bin:${JAVA_HOME}/bin:${PATH}"

java -version
gcc --version
cmake --version
ldd --version 2>&1 | sed -n '1p'

cd "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is not set}"
./mvnw clean package -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
  -Dspotless.skip=true \
  -Dclient.cpp.package.classifier=linux-aarch64-glibc231

SO="iotdb-client/client-cpp/target/install/lib/libiotdb_session.so"
test -f "${SO}"

max_glibc=$(objdump -T "${SO}" | grep -oE "GLIBC_[0-9.]+" | sed "s/GLIBC_//" | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)
echo "max_glibc=${max_glibc}"

if awk -v max="${max_glibc}" "BEGIN { exit !(max > 2.31) }"; then
  echo "ERROR: libiotdb_session.so requires glibc > 2.31 (max=${max_glibc})"
  exit 1
fi

echo "glibc compatibility check passed (max=${max_glibc} <= 2.31)"
