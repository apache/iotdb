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

echo ----------------------------
echo Starting to remove IoTDB ConfigNode
echo ----------------------------

if [ -z "${CONFIGNODE_HOME}" ]; then
  export CONFIGNODE_HOME="$(dirname "$0")/.."
fi

CONFIGNODE_CONF=${CONFIGNODE_HOME}/conf
CONFIGNODE_LOGS=${CONFIGNODE_HOME}/logs

is_conf_path=false
for arg; do
  shift
  if [ "$arg" == "-c" ]; then
    is_conf_path=true
    continue
  fi
  if [ $is_conf_path == true ]; then
    CONFIGNODE_CONF=$arg
    is_conf_path=false
    continue
  fi
  set -- "$@" "$arg"
done

CONF_PARAMS="-r "$*

if [ -f "$CONFIGNODE_CONF/confignode-env.sh" ]; then
  if [ "$#" -ge "1" -a "$1" == "printgc" ]; then
    . "$CONFIGNODE_CONF/confignode-env.sh" "printgc"
  else
    . "$CONFIGNODE_CONF/confignode-env.sh"
  fi
else
  echo "can't find $CONFIGNODE_CONF/confignode-env.sh"
fi

if [ -d ${CONFIGNODE_HOME}/lib ]; then
  LIB_PATH=${CONFIGNODE_HOME}/lib
else
  LIB_PATH=${CONFIGNODE_HOME}/../lib
fi

CLASSPATH=""
for f in ${LIB_PATH}/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done
classname=org.apache.iotdb.confignode.service.ConfigNode

launch_service() {
  class="$1"
  confignode_parms="-Dlogback.configurationFile=${CONFIGNODE_CONF}/logback-confignode.xml"
  confignode_parms="$confignode_parms -DCONFIGNODE_HOME=${CONFIGNODE_HOME}"
  confignode_parms="$confignode_parms -DCONFIGNODE_CONF=${CONFIGNODE_CONF}"
  exec "$JAVA" $illegal_access_params $confignode_parms $CONFIGNODE_JMX_OPTS -cp "$CLASSPATH" "$class" $CONF_PARAMS
  return $?
}

# Start up the service
launch_service "$classname"

exit $?
