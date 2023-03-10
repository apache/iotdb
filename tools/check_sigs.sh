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

# Checks the signatures of all bundles in the build/release-edgent directory
# Or checks the bundles in the specified directory

. `dirname $0`/common.sh

setUsage "`basename $0` [bundle-directory]"
handleHelp "$@"

if [ $# -ge 1 ]
then
    BUNDLE_DIR=$1; shift
fi

noExtraArgs "$@"

[ -d ${BUNDLE_DIR} ] || die "Bundle directory \"${BUNDLE_DIR}\" does not exist"

function checkFile() {
    FILE="$1"
    echo
    echo "Checking $FILE..."

    HASH=`shasum -a 512 "${FILE}" | awk '{print$1}'`
    CHECK=`cat "${FILE}.sha512"`

    if [ "$HASH" != "$CHECK" ]
    then
        echo "${FILE} SHA incorrect"
        exit 1;
    else
       echo "${FILE} SHA OK";
    fi

    gpg --verify "${FILE}.asc" "${FILE}"

}

for bundle in ${BUNDLE_DIR}/*.zip
do
    checkFile ${bundle}
done

echo
echo "SUCCESS: all checksum and signature files OK"