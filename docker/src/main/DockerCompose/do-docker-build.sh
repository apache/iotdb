#!/bin/bash

#docker network create --driver=bridge --subnet=172.20.0.0/16 --gateway=172.20.0.1 iotdb

current_path=$(cd $(dirname $0); pwd)
iotdb_path=$(cd ${current_path}/../../../../; pwd)
iotdb_zip_path=${current_path}/../tmp
options="confignode datanode 1c1d"
nocache="true"

function print_usage(){
    echo "Usage: $(base_name $0) [option] "
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
do_build=${do_build:-false}
image_prefix="apache/iotdb"

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
    local zip_file=${iotdb_zip_path}/$1
    if [[ ! -f ${zip_file} ]]; then
        do_build=true
    fi
}

function process_single(){
    check_build "$1"
    build_iotdb
    build_single ${build_what}
}

function main() {
    case "$build_what" in
        confignode)
            process_single apache-iotdb-${version}-confignode-bin
            ;;
        datanode)
            process_single apache-iotdb-${version}-datanode-bin
            ;;
        1c1d)
            process_single apache-iotdb-${version}-all-bin
            ;;
        all)
            check_build apache-iotdb-${version}-all-bin
            build_iotdb
	    for b in $options ; do
	        build_single ${b}
	    done
            ;;
	?)
	    print_usage ;;
   esac
   # clean up docker images
   docker rmi `docker images|grep '<none>'|awk '{ print $3 }'` || true
}
main
exit $?
