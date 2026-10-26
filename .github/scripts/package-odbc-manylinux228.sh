#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/native-linux-env.sh"
if (( $(gcc -dumpversion | cut -d. -f1) < NATIVE_GCC_MIN_MAJOR )); then
  echo "GCC ${NATIVE_GCC_MIN_MAJOR}+ is required" >&2
  exit 1
fi
dnf install -y unixODBC-devel perl-IPC-Cmd perl-Data-Dumper perl-Time-Piece
odbc_build="${ODBC_BUILD_DIR:-iotdb-client/odbc/target/build}"
cmake -S iotdb-client/odbc -B "${odbc_build}" \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_SKIP_RPATH=ON \
  -DODBC_PACKAGE_VERSION="${ODBC_PACKAGE_VERSION:-$(sed -n 's:.*<version>\([^<]*\)</version>.*:\1:p' iotdb-client/pom.xml | head -1)}" \
  -DODBC_PACKAGE_CLASSIFIER="linux-x86_64-glibc${NATIVE_GLIBC_BASELINE}"
cmake --build "${odbc_build}" --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-4}"
ctest --test-dir "${odbc_build}" --output-on-failure
cmake -DDRIVER="${odbc_build}/libapache_iotdb_odbc.so" \
  -DGLIBC_BASELINE="${NATIVE_GLIBC_BASELINE}" -P iotdb-client/odbc/cmake/CheckLinuxPackage.cmake
(cd "${odbc_build}" && cpack)
