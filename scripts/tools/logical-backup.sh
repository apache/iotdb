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

if [ -n "${IOTDB_INCLUDE}" ] && [ -r "${IOTDB_INCLUDE}" ]; then
  . "${IOTDB_INCLUDE}"
fi

if [ -z "${IOTDB_HOME}" ]; then
  IOTDB_HOME="$(cd "$(dirname "$0")/.."; pwd)"
  export IOTDB_HOME
fi

if [ -n "${JAVA_HOME}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
  JAVA="${JAVA_HOME}/bin/java"
else
  JAVA=java
fi

CLASSPATH=""
for jar in "${IOTDB_HOME}"/lib/*.jar; do
  CLASSPATH="${CLASSPATH}:${jar}"
done

exec "${JAVA}" -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8 \
  -DIOTDB_HOME="${IOTDB_HOME}" -cp "${CLASSPATH}" \
  org.apache.iotdb.tool.pipe.PipeLogicalBackupTool "$@"
