#!/bin/bash

################################################################################
##
##  Licensed to the Apache Software Foundation (ASF) under one or more
##  contributor license agreements.  See the NOTICE file distributed with
##  this work for additional information regarding copyright ownership.
##  The ASF licenses this file to You under the Apache License, Version 2.0
##  (the "License"); you may not use this file except in compliance with
##  the License.  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
##  Unless required by applicable law or agreed to in writing, software
##  distributed under the License is distributed on an "AS IS" BASIS,
##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##  See the License for the specific language governing permissions and
##  limitations under the License.
##
################################################################################

BUILDTOOLS_DIR=`dirname $0`

IOTDB_ROOT_DIR=.
# BUNDLE_DIR is results of maven release:perform's creation of release candidate
BUNDLE_DIR=${IOTDB_ROOT_DIR}/target/checkout/target

IOTDB_ASF_GIT_URL=https://git-wip-us.apache.org/repos/asf/iotdb.git
IOTDB_ASF_DIST_URL=https://www.apache.org/dist/iotdb
IOTDB_ASF_DIST_DYN_URL=https://www.apache.org/dyn/closer.cgi/iotdb
IOTDB_ASF_SVN_RELEASE_URL=https://dist.apache.org/repos/dist/release/iotdb
IOTDB_ASF_SVN_RC_URL=https://dist.apache.org/repos/dist/dev/iotdb

USAGE=

RELEASE_PROP_FILE=${IOTDB_ROOT_DIR}/iotdb.release.properties

function die() {  # [$* msgs]
  [ $# -gt 0 ] && echo "Error: $*"
  exit 1
}

function setUsage() {  # $1: usage string
  USAGE=$1
}

function usage() {  #  [$*: msgs]
  [ $# -gt 0 ] && echo "Error: $*"
  echo "Usage: ${USAGE}"
  exit 1
}

function handleHelp() { # usage: handleHelp "$@"
  if [ "$1" == "-?" -o "$1" == "--help" ]; then
    usage
  fi
}

function requireArg() {  # usage: requireArgs "$@"
  if [ $# -lt 1 ] || [[ $1 =~ ^- ]]; then
    usage "missing argument"
  fi
}

function noExtraArgs() { # usage: noExtraArgs "$@"
  [ $# = 0 ] || usage "extra arguments"
}

function getAbsPath() { # $1: rel-or-abs-path
    echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

function confirm () {  # [$1: question]
  while true; do
    # call with a prompt string or use a default
    /bin/echo -n "${1:-Are you sure?}"
    read -r -p " [y/n] " response
    case $response in
      [yY]) return `true` ;;
      [nN]) return `false` ;;
      *) echo "illegal response '$response'" ;;
    esac
  done
}

function dieSuperceeded { # no args
  die "This tool is superceeded with the new maven build tooling.  See src/site/asciidoc/releasing.adoc."
}

function checkIOTDBSourceRootGitDie { # no args; dies if !ok
  [ -d "${IOTDB_ROOT_DIR}/.git" ] || die "Not an IOTDB source root git directory \"${IOTDB_ROOT_DIR}\""
}

function checkUsingMgmtCloneWarn() { # no args; warns if iotdb root isn't a mgmt clone
  CLONE_DIR=`cd ${IOTDB_ROOT_DIR}; pwd`
  CLONE_DIRNAME=`basename $CLONE_DIR`
  if [ ! `echo $CLONE_DIRNAME | grep -o -E '^mgmt-iotdb'` ]; then
    echo "Warning: the IOTDB root dir \"${IOTDB_ROOT_DIR}\" is not a release mgmt clone!"
    return 1
  else
    return 0
  fi
}

function checkBundleDir() { # no args  returns true/false (0/1)
  if [ -d ${BUNDLE_DIR} ]; then
    return 0
  else
    return 1
  fi
}

function checkVerNum() {  #  $1: X.Y.Z  returns true/false (0/1)
  if [ `echo $1 | grep -o -E '^\d+\.\d+\.\d+$'` ]; then
    return 1
  else
    return 0
  fi
}

function checkVerNumDie() { #  $1: X.Y.Z  dies if not ok
  checkVerNum $1 || die "Not a X.Y.Z version number \"$1\""
}

function checkRcNum() {  # $1: rc-num   returns true/false (0/1)
  if [ `echo $1 | grep -o -E '^\d+$'` ] && [ $1 != 0 ]; then
    return 1
  else
    return 0
  fi
}

function checkRcNumDie() {  # $1: rc-num dies if not ok
  checkRcNum $1 || die "Not a release candidate number \"$1\""
}

function createReleaseProperties { # X.Y.Z
  VER="$1"
  checkVerNumDie ${VER}
  echo "releaseNum=${VER}" > ${RELEASE_PROP_FILE}
}

function getReleaseProperty {  # <property-name>
  PN=$1
  PNVAL=`grep ${PN} ${RELEASE_PROP_FILE}`
  VAL=`echo ${PNVAL} | sed -e "s/^${PN}=//"`
  echo ${VAL}
}

function getIOTDBVer() {  # [$1 == "bundle"]
  MSG="getIOTDBVer(): unknown mode \"$1\""
  VER=""
  if [ "$1" == "" ]; then
    VER=`getReleaseProperty releaseNum`
    MSG="Unable to identify the release version id from ${RELEASE_PROP_FILE}"
  elif [ $1 == "gradle" ]; then
    die "'getIOTDBVer() gradle' is no longer supported"
    # Get the X.Y.Z version from gradle build info
    PROPS=${IOTDB_ROOT_DIR}/gradle.properties
    VER=`grep build_version ${PROPS} | grep -o -E '\d+\.\d+\.\d+'`
    MSG="Unable to identify the version id from ${PROPS}"
  elif [ $1 == "bundle" ]; then
    # Get the X.Y.Z version from a build generated bundle's name
    BUNDLE=`echo ${BUNDLE_DIR}/apache-iotdb-*-source-release.tar.gz`
    VER=`echo ${BUNDLE} | grep -o -E '\d+\.\d+\.\d+'`
    MSG="Unable to identify the version id from bundle ${BUNDLE}"
  fi
  [ "${VER}" ] || die "${MSG}"
  echo $VER
}

function getMajMinVerNum() {  #  $1: X.Y.Z  returns X.Y
  VER=$1; shift
  checkVerNumDie ${VER}
  MAJ_MIN_VER=`echo ${VER} | sed -e 's/\.[0-9][0-9]*$//'`
  echo ${MAJ_MIN_VER}
}

function getReleaseBranch() { # $1: X.Y.Z version
  MAJ_MIN_NUM=`getMajMinVerNum $1`
  echo "release/${MAJ_MIN_NUM}"
}

function getReleaseTag() {  # $1: X.Y.Z  [$2: rc-num]
  VER=$1; shift
  checkVerNumDie ${VER}
  RC_SFX=""
  if [ $# -gt 0 ] && [ "$1" != "" ]; then
    RC_SFX="-RC$1"
  fi
  echo "${VER}${RC_SFX}"
}

function getReleaseTagComment() {  # $1: X.Y.Z  [$2: rc-num]
  VER=$1; shift
  checkVerNumDie ${VER}
  RC_SFX=""
  if [ $# -gt 0 ] && [ "$1" != "" ]; then
    RC_SFX=" RC$1"
  fi
  echo "Apache IOTDB ${VER}${RC_SFX}"
}