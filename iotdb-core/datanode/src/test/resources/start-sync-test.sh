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

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="$(cd "`dirname "$0"`"/..;cd ".."; cd ".."; cd "iotdb" ;pwd)"
  echo $IOTDB_HOME
fi

IOTDB_CONF=${IOTDB_HOME}/conf
# IOTDB_LOGS=${IOTDB_HOME}/logs

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

CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

MAIN_CLASS=org.apache.iotdb.db.postback.utils.CreateDataSender1

"$JAVA" -DIOTDB_HOME=${IOTDB_HOME} -DTSFILE_HOME=${IOTDB_HOME} -DIOTDB_CONF=${IOTDB_CONF} -Dlogback.configurationFile=${IOTDB_CONF}/logback.xml $IOTDB_DERBY_OPTS $IOTDB_JMX_OPTS -Dname=postBackTest -cp "$CLASSPATH" "$MAIN_CLASS"

exit $?
