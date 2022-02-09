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
echo "Starting IoTDB (Cluster Mode)"
echo ---------------------

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="`dirname "$0"`/.."
fi

enable_printgc=false
if [ "$#" -ge "1" -a "$1" == "printgc" ]; then
  enable_printgc=true;
  shift
fi

IOTDB_CONF=$1
if [ -z "${IOTDB_CONF}" ]; then
  export IOTDB_CONF=${IOTDB_HOME}/conf
fi

if [ -f "$IOTDB_CONF/iotdb-env.sh" ]; then
    if [ $enable_printgc == "true" ]; then
      . "$IOTDB_CONF/iotdb-env.sh" "printgc"
    else
       . "$IOTDB_CONF/iotdb-env.sh"
    fi
elif [ -f "${IOTDB_HOME}/conf/iotdb-env.sh" ]; then
    if [ $enable_printgc == "true" ]; then
      . "${IOTDB_HOME}/conf/iotdb-env.sh" "printgc"
    else
      . "${IOTDB_HOME}/conf/iotdb-env.sh"
    fi
else
    echo "can't find $IOTDB_CONF/iotdb-env.sh"
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

CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done
classname=org.apache.iotdb.cluster.ClusterIoTDB

launch_service()
{
	class="$1"
	iotdb_parms="-Dlogback.configurationFile=${IOTDB_CONF}/logback.xml"
	iotdb_parms="$iotdb_parms -DIOTDB_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DTSFILE_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DIOTDB_CONF=${IOTDB_CONF}"
	iotdb_parms="$iotdb_parms -Dname=iotdb\.IoTDB"
	exec "$JAVA" $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" "$class" -a
	return $?
}

# Start up the service
launch_service "$classname"

exit $?
