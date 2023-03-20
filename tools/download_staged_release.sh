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

set -e

# NOTICE This is mostly copied from the PLC4X Project and was adopted to 
# the IoTDB Project

# Download the collection of files associated with an Apache IoTDB
# Release or Release Candidate from the Apache Distribution area:
# https://dist.apache.org/repos/dist/release/iotdb
# or https://dist.apache.org/repos/dist/dev/iotdb
# respectively.
#
# Prompts before taking actions unless "--nquery"
# Prompts to perform signature validation (using buildTools/check_sigs.sh)
# unless --nvalidate or --validate is specified.


. `dirname $0`/common.sh



setUsage "`basename $0` [--nquery] [--validate|--nvalidate] <version> [<rc-num>]"
handleHelp "$@"

BUILDTOOLS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

NQUERY=
if [ "$1" == "--nquery" ]; then
  NQUERY="--nquery"; shift
fi

VALIDATE=-1  # query
if [ "$1" == "--validate" ]; then
  VALIDATE=1; shift
elif [ "$1" == "--nvalidate" ]; then
  VALIDATE=0; shift
fi

requireArg "$@"
VER=$1; shift
checkVerNum $VER || usage "Not a X.Y.Z version number \"$VER\""

RC_NUM=
if [ $# -gt 0 ]; then
  RC_NUM=$1; shift
  checkRcNum ${RC_NUM} || usage "Not a release candidate number \"${RC_NUM}\""
fi

noExtraArgs "$@"

# Release or Release Candidate mode
IS_RC=
if [ ${RC_NUM} ]; then
  IS_RC=1
fi

BASE_URL=${IOTDB_ASF_SVN_RELEASE_URL}
if [ ${IS_RC} ]; then
  BASE_URL=${IOTDB_ASF_SVN_RC_URL}
fi

RC_SFX=
if [ ${IS_RC} ]; then
    RC_SFX=rc${RC_NUM}
fi

DST_BASE_DIR=downloaded-iotdb-${VER}${RC_SFX}
[ -d ${DST_BASE_DIR} ] && die "${DST_BASE_DIR} already exists"

[ ${NQUERY} ] || confirm "Proceed to download to ${DST_BASE_DIR} from ${BASE_URL}?" || exit

echo Downloading to ${DST_BASE_DIR} ...

function mywget() {
  # OSX lacks wget by default
  (set -x; curl -f -O $1)
}

function getSignedBundle() {
  mywget ${1}
  mywget ${1}.asc
  mywget ${1}.sha512
}

mkdir -p ${DST_BASE_DIR}
cd ${DST_BASE_DIR}
ABS_BASE_DIR=`pwd`
URL=${BASE_URL}
mywget ${URL}/KEYS

DST_VER_DIR=${VER}
URL=${BASE_URL}/${VER}
if [ ${IS_RC} ]; then
  DST_VER_DIR=${DST_VER_DIR}/${RC_SFX}
  URL=${URL}/${RC_SFX}
fi

mkdir -p ${DST_VER_DIR}
cd ${DST_VER_DIR}
mywget ${URL}/README.md
mywget ${URL}/RELEASE_NOTES.md
getSignedBundle ${URL}/apache-iotdb-${VER}-source-release.zip
getSignedBundle ${URL}/apache-iotdb-${VER}-bin.zip

echo
echo Done Downloading to ${DST_BASE_DIR}

[ ${VALIDATE} == 0 ] && exit
[ ${VALIDATE} == 1 ] || [ ${NQUERY} ] || confirm "Do you want to check the bundle signatures and compare source bundles?" || exit

cd ${ABS_BASE_DIR}

echo
echo "If the following bundle gpg signature checks fail, you may need to"
echo "import the project's list of signing keys to your keyring"
echo "    $ gpg ${BUILDTOOLS_DIR}/${DST_BASE_DIR}/KEYS            # show the included keys"
echo "    $ gpg --import ${BUILDTOOLS_DIR}/${DST_BASE_DIR}/KEYS"
gpg ${BUILDTOOLS_DIR}/${DST_BASE_DIR}/KEYS
gpg --import ${BUILDTOOLS_DIR}/${DST_BASE_DIR}/KEYS

echo
echo "Verifying the source bundle signatures..."
(set -x; bash $BUILDTOOLS_DIR/check_sigs.sh ${BUILDTOOLS_DIR}/${DST_BASE_DIR}/${DST_VER_DIR})