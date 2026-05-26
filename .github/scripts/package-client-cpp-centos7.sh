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
# Build client-cpp on manylinux2014 (CentOS 7, glibc 2.17 baseline) with devtoolset-8.
set -euxo pipefail

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

fix_yum_vault_repos
yum install -y ca-certificates centos-release-scl epel-release
write_sclo_vault_repos
fix_yum_vault_repos
yum clean all
yum makecache -y

yum install -y devtoolset-8-gcc devtoolset-8-gcc-c++ devtoolset-8-binutils \
  devtoolset-8-libstdc++-devel scl-utils \
  make wget tar which git patch unzip bzip2

CMAKE_VERSION=3.28.4
CMAKE_DIR=/opt/cmake-${CMAKE_VERSION}
if [[ ! -x "${CMAKE_DIR}/bin/cmake" ]]; then
  wget -q "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz" \
    -O /tmp/cmake.tar.gz
  rm -rf "${CMAKE_DIR}"
  mkdir -p /opt
  tar xf /tmp/cmake.tar.gz -C /opt
  mv "/opt/cmake-${CMAKE_VERSION}-linux-x86_64" "${CMAKE_DIR}"
fi

JAVA_HOME=/opt/jdk-17
if [[ ! -x "${JAVA_HOME}/bin/java" ]]; then
  wget -qL -O /tmp/jdk17.tar.gz \
    "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse?project=jdk"
  rm -rf /opt/jdk-17*
  mkdir -p /opt
  tar xf /tmp/jdk17.tar.gz -C /opt
  JAVA_HOME=$(find /opt -maxdepth 1 -type d -name 'jdk-17*' | head -1)
  ln -sfn "${JAVA_HOME}" /opt/jdk-17
  JAVA_HOME=/opt/jdk-17
fi

export PATH="${CMAKE_DIR}/bin:${JAVA_HOME}/bin:${PATH}"
export JAVA_HOME

scl enable devtoolset-8 -- bash -c '
  set -euxo pipefail
  gcc --version
  cmake --version
  java -version
  cd "${GITHUB_WORKSPACE:?GITHUB_WORKSPACE is not set}"
  ./mvnw clean package -P with-cpp -pl iotdb-client/client-cpp -am -DskipTests \
    -Dspotless.skip=true \
    -Dclient.cpp.package.classifier=linux-x86_64-glibc217
'

SO="iotdb-client/client-cpp/target/install/lib/libiotdb_session.so"
test -f "${SO}"

echo "=== Build host glibc ==="
ldd --version | head -1

echo "=== Highest GLIBC_* symbols in libiotdb_session.so ==="
objdump -T "${SO}" | grep GLIBC_ | sed "s/.*GLIBC_/GLIBC_/" | sort -Vu | tail -10

max_glibc=$(objdump -T "${SO}" | grep -oE "GLIBC_[0-9.]+" | sed "s/GLIBC_//" | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)
echo "max_glibc=${max_glibc}"

if awk -v max="${max_glibc}" "BEGIN { exit !(max > 2.17) }"; then
  echo "ERROR: libiotdb_session.so requires glibc > 2.17 (max=${max_glibc})"
  exit 1
fi

echo "glibc compatibility check passed (max=${max_glibc} <= 2.17)"
