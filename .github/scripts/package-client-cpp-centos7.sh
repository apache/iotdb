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
# Build client-cpp for glibc 2.17 baseline (manylinux2014 x86_64/aarch64, or CentOS 7 fallback).
# Set PACKAGE_CLASSIFIER (e.g. linux-x86_64-glibc217 / linux-aarch64-glibc217).
set -euxo pipefail

MACHINE=$(uname -m)
case "${MACHINE}" in
  x86_64)
    CMAKE_PKG_ARCH=linux-x86_64
    JDK_API_ARCH=linux/x64
  ;;
  aarch64)
    CMAKE_PKG_ARCH=linux-aarch64
    JDK_API_ARCH=linux/aarch64
  ;;
  *)
    echo "Unsupported architecture: ${MACHINE}" >&2
    exit 1
    ;;
esac

if [[ -z "${PACKAGE_CLASSIFIER:-}" ]]; then
  if [[ "${MACHINE}" == "x86_64" ]]; then
    PACKAGE_CLASSIFIER=linux-x86_64-glibc217
  else
    PACKAGE_CLASSIFIER=linux-aarch64-glibc217
  fi
fi

run_maven_build() {
  gcc --version
  cmake --version
  java -version
  cd "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is not set}"
  ./mvnw clean package -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
    -Dspotless.skip=true \
    -Dclient.cpp.package.classifier="${PACKAGE_CLASSIFIER}"
}

# CentOS 7 EOL: redirect yum repos to vault.centos.org (see CentOS wiki / SIG SCLo).
fix_yum_vault_repos() {
  local repo
  for repo in /etc/yum.repos.d/*.repo; do
    [[ -f "${repo}" ]] || continue
    sed -i \
      -e 's/mirror\.centos\.org/vault.centos.org/g' \
      -e 's/^mirrorlist=/#mirrorlist=/g' \
      -e 's/^#baseurl=/baseurl=/g' \
      -e 's/^# baseurl=/baseurl=/g' \
      "${repo}"
  done
}

# SCLo packages live under vault .../7.9.2009/sclo/... ; $releasever is "7" in the image.
write_sclo_vault_repos() {
  cat > /etc/yum.repos.d/CentOS-SCLo-scl.repo <<'EOF'
[centos-sclo-sclo]
name=CentOS-7 - SCLo sclo
baseurl=http://vault.centos.org/centos/7.9.2009/sclo/$basearch/sclo/
gpgcheck=1
enabled=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-SCLo

[centos-sclo-scl]
name=CentOS-7 - SCLo scl
baseurl=http://vault.centos.org/centos/7.9.2009/sclo/$basearch/scl/
gpgcheck=1
enabled=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-SCLo
EOF

  cat > /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo <<'EOF'
[centos-sclo-rh]
name=CentOS-7 - SCLo rh
baseurl=http://vault.centos.org/centos/7.9.2009/sclo/$basearch/rh/
gpgcheck=1
enabled=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-SIG-SCLo
EOF
}

install_centos7_devtoolset8() {
  fix_yum_vault_repos
  yum install -y ca-certificates centos-release-scl epel-release
  write_sclo_vault_repos
  fix_yum_vault_repos
  yum clean all
  yum makecache -y
  yum install -y devtoolset-8-gcc devtoolset-8-gcc-c++ devtoolset-8-binutils \
    devtoolset-8-libstdc++-devel scl-utils \
    make wget tar which git patch unzip bzip2
}

if [[ -x /opt/rh/devtoolset-10/root/usr/bin/gcc ]]; then
  # manylinux2014 images ship devtoolset-10 on PATH for glibc 2.17-compatible builds.
  yum install -y wget tar which git patch unzip bzip2 || true
else
  install_centos7_devtoolset8
fi

CMAKE_VERSION=3.28.4
CMAKE_DIR=/opt/cmake-${CMAKE_VERSION}
if [[ ! -x "${CMAKE_DIR}/bin/cmake" ]]; then
  wget -q "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-${CMAKE_PKG_ARCH}.tar.gz" \
    -O /tmp/cmake.tar.gz
  rm -rf "${CMAKE_DIR}"
  mkdir -p /opt
  tar xf /tmp/cmake.tar.gz -C /opt
  mv "/opt/cmake-${CMAKE_VERSION}-${CMAKE_PKG_ARCH}" "${CMAKE_DIR}"
fi

JAVA_HOME=/opt/jdk-17
if [[ ! -x "${JAVA_HOME}/bin/java" ]]; then
  wget -qL -O /tmp/jdk17.tar.gz \
    "https://api.adoptium.net/v3/binary/latest/17/ga/${JDK_API_ARCH}/jdk/hotspot/normal/eclipse?project=jdk"
  rm -rf /opt/jdk-17*
  mkdir -p /opt
  tar xf /tmp/jdk17.tar.gz -C /opt
  JAVA_HOME=$(find /opt -maxdepth 1 -type d -name 'jdk-17*' -print -quit)
  ln -sfn "${JAVA_HOME}" /opt/jdk-17
  JAVA_HOME=/opt/jdk-17
fi

export PATH="${CMAKE_DIR}/bin:${JAVA_HOME}/bin:${PATH}"
export JAVA_HOME

if [[ -x /opt/rh/devtoolset-10/root/usr/bin/gcc ]]; then
  run_maven_build
else
  scl enable devtoolset-8 -- bash -c "
    set -euxo pipefail
    gcc --version
    cmake --version
    java -version
    cd \"\${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is not set}\"
    ./mvnw clean package -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
      -Dspotless.skip=true \
      -Dclient.cpp.package.classifier=${PACKAGE_CLASSIFIER}
  "
fi

SO="iotdb-client/client-cpp/target/install/lib/libiotdb_session.so"
test -f "${SO}"

echo "=== Build host glibc ==="
ldd --version 2>&1 | sed -n '1p'

echo "=== Highest GLIBC_* symbols in libiotdb_session.so ==="
objdump -T "${SO}" | grep GLIBC_ | sed "s/.*GLIBC_/GLIBC_/" | sort -Vu | tail -10

max_glibc=$(objdump -T "${SO}" | grep -oE "GLIBC_[0-9.]+" | sed "s/GLIBC_//" | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)
echo "max_glibc=${max_glibc}"

if awk -v max="${max_glibc}" "BEGIN { exit !(max > 2.17) }"; then
  echo "ERROR: libiotdb_session.so requires glibc > 2.17 (max=${max_glibc})"
  exit 1
fi

echo "glibc compatibility check passed (max=${max_glibc} <= 2.17)"
