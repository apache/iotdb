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


echo ---------------------
echo Starting IoTDB
echo ---------------------

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="`dirname "$0"`/.."
fi

IOTDB_CONF=${IOTDB_HOME}/conf
# IOTDB_LOGS=${IOTDB_HOME}/logs

is_conf_path=false
for arg do
  shift
  if [ "$arg" == "-c" ]; then
    is_conf_path=true
    continue
  fi
  if [ $is_conf_path == true ]; then
    IOTDB_CONF=$arg
    is_conf_path=false
    continue
  fi
  set -- "$@" "$arg"
done

CONF_PARAMS=$*

if [ -f "$IOTDB_CONF/iotdb-env.sh" ]; then
    if [ "$#" -ge "1" -a "$1" == "printgc" ]; then
      . "$IOTDB_CONF/iotdb-env.sh" "printgc"
    else
        . "$IOTDB_CONF/iotdb-env.sh"
    fi
else
    echo "can't find $IOTDB_CONF/iotdb-env.sh"
fi

CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done
classname=org.apache.iotdb.db.service.IoTDB

launch_service()
{
	class="$1"
	iotdb_parms="-Dlogback.configurationFile=${IOTDB_CONF}/logback.xml"
	iotdb_parms="$iotdb_parms -DIOTDB_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DTSFILE_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DIOTDB_CONF=${IOTDB_CONF}"
	iotdb_parms="$iotdb_parms -DTSFILE_CONF=${IOTDB_CONF}"
	iotdb_parms="$iotdb_parms -Dname=iotdb\.IoTDB"
	exec "$JAVA" $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" "$class" $CONF_PARAMS
	return $?
}

# Start up the service
launch_service "$classname"

exit $?
