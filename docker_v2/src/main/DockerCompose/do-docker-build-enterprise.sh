#!/bin/bash
#
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
#

current_path=$(cd "$(dirname "$0")"; pwd)
iotdb_path=$(cd "${current_path}/../../../../"; pwd)
iotdb_zip_path="${current_path}/../target/"
options="standalone latest"
nocache="true"
do_build="false"
docker_build="docker build"
do_publish="false"
docker_publish=""
image_prefix="nexus.infra.timecho.com:8243/timecho/timechodb"
save_image="false"

function print_usage() {
  echo "Usage: $(basename "$0") [option] "
  echo "  -t image to build, required. Options: ${options} all"
  echo "  -v TimechoDB version, default 1.2.0-SNAPSHOT"
  echo "  -u specified the docker image maintainer, default git current user"
  echo "  -c commit id, default git current short commit id"
  echo "  -s save image to .tar.gz file"
  echo "  -b do maven build of TimechoDB from source codes, version is read from pom.xml"
  echo "  -p publish to docker registry using buildx"
  exit 1
}

while getopts 'v:u:t:c:bsph' OPT; do
  case "${OPT}" in
    s) save_image="true" ;;
    t) build_what="${OPTARG}" ;;
    v) version="${OPTARG}" ;;
    u) maintainer="${OPTARG}" ;;
    c) commit_id="${OPTARG}" ;;
    b)
      do_build="true"
      version=$(grep packaging -B 2 "${iotdb_path}/pom.xml" | grep version | cut -d '<' -f2 | cut -d '>' -f2)
      ;;
    p)
      do_publish="true"
      docker_publish="--push"
      ;;
    h) print_usage ;;
    ?) print_usage ;;
  esac
done

if [[ -z "${build_what}" ]]; then
  echo "-t is required."
  print_usage
fi

version=${version:-"1.2.0-SNAPSHOT"}
maintainer=${maintainer:-"$(git config user.name)"}
build_date="$(date +'%Y-%m-%dT%H:%M:%S+08:00')"
commit_id=${commit_id:-"$(git rev-parse --short HEAD)"}

echo "#################################"
echo "build_what=${build_what}"
echo "version=${version}"
echo "do_build=${do_build}"
echo "maintainer=${maintainer}"
echo "commit_id=${commit_id}"
echo "#################################"

set -ex

function build_single() {
  echo "##### build docker image #####"
  local dockerfile="Dockerfile-1.0.0-standalone"
  local image

  if [[ "$1" == "latest" ]]; then
    image="${image_prefix}:latest"
  else
    image="${image_prefix}:${version}-$1"
  fi

  cd "${current_path}/../"
  ${docker_build} -f "${dockerfile}" \
    --build-arg version="${version}" \
    --build-arg target="timechodb-${version}-bin" \
    --label timechodb.docker_v2="true" \
    --label build_date="${build_date}" \
    --label maintainer="${maintainer}" \
    --label commit_id="${commit_id}" \
    --no-cache="${nocache}" -t "${image}" . ${docker_publish}

  if [[ "${save_image}" == "true" && "$1" != "latest" ]]; then
    docker save "${image}" | gzip > "timechodb-${version}-standalone-docker.tar.gz"
  fi
  echo "##### done #####"
}

function prepare_buildx() {
  if [[ "${do_publish}" == "true" ]]; then
    if ! docker buildx version; then
      echo "WARN: docker buildx does not support!"
      docker_build="docker build"
      docker_publish=""
      return
    fi
    docker_build="docker buildx build --platform linux/amd64,linux/arm64/v8"
    if ! docker buildx inspect mybuilder; then
      docker buildx create --name mybuilder --driver docker-container --bootstrap --use
      docker run --rm --privileged tonistiigi/binfmt:latest --install all
    fi
  else
    docker_build="docker build"
    docker_publish=""
  fi
}

function build_iotdb() {
  if [[ "${do_build}" == "false" ]]; then
    return
  fi
  echo "##### build TimechoDB #####"
  cd "${iotdb_path}"
  mvn clean package -pl distribution -am -DskipTests
  if [[ ! -d "${iotdb_zip_path}" ]]; then
    mkdir "${iotdb_zip_path}"
  fi
  cd "${iotdb_path}/distribution/target"
  cp "timechodb-${version}-bin.zip" "${iotdb_zip_path}/"
  do_build="false"
  echo "##### done #####"
}

function check_build() {
  if [[ "${do_build}" == "true" ]]; then
    return
  fi
  local zip_file="${iotdb_zip_path}/timechodb-${version}-bin.zip"
  if [[ ! -f "${zip_file}" ]]; then
    echo "File is not found: ${zip_file}"
    exit 3
  fi
}

function process_single() {
  check_build
  build_iotdb
  build_single "$1"
}

function main() {
  prepare_buildx
  case "${build_what}" in
    standalone)
      process_single standalone
      ;;
    latest)
      process_single latest
      ;;
    all)
      check_build
      build_iotdb
      for b in ${options}; do
        build_single "${b}"
      done
      ;;
    *)
      echo "bad value of -t."
      print_usage
      ;;
  esac
  echo "clean up docker images"
  docker image prune -f --filter "label=timechodb.docker_v2=true" > /dev/null 2>&1 || true
}

main
exit $?
