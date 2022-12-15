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

#docker network create --driver=bridge --subnet=172.20.0.0/16 --gateway=172.20.0.1 iotdb

current_path=$(cd $(dirname $0); pwd)
iotdb_path=$(cd ${current_path}/../../../../; pwd)
iotdb_zip_path=${current_path}/../tmp
options="confignode datanode 1c1d"
nocache="true"
do_build="false"

function print_usage(){
    echo "Usage: $(basename $0) [option] "
    echo "	-t image to build, required. Options:$options all"
    echo "	-v iotdb version, default 1.0.1"
    echo "	-u specified the docker image maintainer, default git current user"
    echo "	-c commit id, default git current short commit id"
    exit -1
}
while getopts 'v:u:t:c:bh' OPT; do
    case $OPT in
       t) build_what="$OPTARG";;
       v) version="$OPTARG";;
       u) maintainer="$OPTARG";;
       c) commit_id="$OPTARG";;
       b) do_build=true;;
       h) print_usage;;
       ?) print_usage;;
    esac
done

if [[ -z "$build_what" ]]; then echo "-t is required."; print_usage; fi

version=${version:-"1.0.1-SNAPSHOT"}
maintainer=${maintainer:-"$(git config user.name)"}
build_date="$(date +'%Y-%m-%dT%H:%M:%S+08:00')"
commit_id=${commit_id:-"$(git rev-parse --short HEAD)"}
image_prefix="apache/iotdb"

echo "#################################"
echo "build_what=$build_what"
echo "version=$version"
echo "do_build=$do_build"
echo "maintainer=$maintainer"
echo "commit_id=$commit_id"
echo "#################################"

set -ex

function build_single(){
    echo "##### build docker image #####"
    local dockerfile="Dockerfile-1.0.0-$1"
    local image="${image_prefix}:${version}-$1"
    cd ${current_path}/../
    docker build -f ${dockerfile} \
	--build-arg version=${version} \
        --label build_date="${build_date}" \
        --label maintainer="${maintainer}" \
        --label commit_id="${commit_id}" \
        --no-cache=${nocache} -t ${image} .
    echo "##### done #####"
}

function build_iotdb(){
    if [[ "$do_build" == "false" ]]; then
        return;
    fi
    echo "##### build iotdb #####"
    cd $iotdb_path
    mvn clean package -pl distribution -am -DskipTests
    if [[ ! -d ${iotdb_zip_path} ]]; then mkdir ${iotdb_zip_path}; fi
    cd ${iotdb_path}/distribution/target
    cp apache-iotdb-${version}-all-bin.zip apache-iotdb-${version}-confignode-bin.zip apache-iotdb-${version}-datanode-bin.zip ${iotdb_zip_path}/
    do_build=false
    echo "##### done #####"
}

function check_build(){
    local zip_file=${iotdb_zip_path}/apache-iotdb-${version}-$1-bin.zip
    if [[ ! -f ${zip_file} ]]; then
        do_build=true
    fi
}

function process_single(){
    check_build "${1:-$build_what}"
    build_iotdb
    build_single ${build_what}
}

function main() {
    case "$build_what" in
        confignode)
            process_single
            ;;
        datanode)
            process_single
            ;;
        1c1d)
            process_single all
            ;;
        all)
            check_build all
            build_iotdb
	    for b in $options ; do
	        build_single ${b}
	    done
            ;;
	?)
	    print_usage ;;
   esac
   # clean up docker images
   docker rmi `docker images|grep '<none>'|awk '{ print $3 }'` > /dev/null 2>&1 || true
}
main
exit $?
