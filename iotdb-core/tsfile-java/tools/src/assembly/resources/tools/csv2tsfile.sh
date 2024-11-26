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

echo ------------------------------------------
echo Starting Csv to TsFile Script
echo ------------------------------------------

if [ -z "${TSFILE_HOME}" ]; then
    export TSFILE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables.  > /dev/stderr
    exit 1;
fi


CLASSPATH=${TSFILE_HOME}/lib/*

MAIN_CLASS=org.apache.tsfile.tools.TsFileTool

TSFILE_CONF=${TSFILE_HOME}/conf
tsfile_params="-Dlogback.configurationFile=${TSFILE_CONF}/logback-cvs2tsfile.xml"

exec "$JAVA" -DTSFILE_HOME=${TSFILE_HOME} $tsfile_params -cp "$CLASSPATH" "$MAIN_CLASS" "$@"