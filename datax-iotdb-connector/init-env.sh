#/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

####################################################################
# This script is used to initial environment of DataX
####################################################################

set -eo pipefail

DATAX_EXTENSION_HOME=`dirname "$0"`
DATAX_EXTENSION_HOME=`cd "$DATAX_EXTENSION_HOME"; pwd`

export DATAX_EXTENSION_HOME
DATAX_DOWNLOAD_URL=http://datax-opensource.oss-cn-hangzhou.aliyuncs.com/datax.tar.gz
DATAX_HOME=$DATAX_EXTENSION_HOME/datax



USE_MACOS=0
# check OS type
if [[ ! -z "$OSTYPE" ]]; then
    if [[ ${OSTYPE:0:6} == "darwin" ]]; then
        USE_MACOS=1
    fi
fi

if [ ! -d DATAX_HOME ]; then
    echo "Download DataX tar from $DATAX_DOWNLOAD_URL"
    wget  $DATAX_DOWNLOAD_URL -P   $DATAX_EXTENSION_HOME
    tar zxvf $DATAX_EXTENSION_HOME/datax.tar.gz
else
    echo "DataX Home  exists in $DATAX_EXTENSION_HOME"
fi







echo "Finish DataX environment initialization"
