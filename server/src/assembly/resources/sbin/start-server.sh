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

if [ -f "$IOTDB_CONF/datanode-env.sh" ]; then
    if [ "$#" -ge "1" -a "$1" == "printgc" ]; then
      . "$IOTDB_CONF/datanode-env.sh" "printgc"
    else
        . "$IOTDB_CONF/datanode-env.sh"
    fi
else
    echo "can't find $IOTDB_CONF/datanode-env.sh"
fi

if [ -d ${IOTDB_HOME}/lib ]; then
LIB_PATH=${IOTDB_HOME}/lib
else
LIB_PATH=${IOTDB_HOME}/../lib
fi

CLASSPATH=""
for f in ${LIB_PATH}/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done
classname=org.apache.iotdb.db.service.IoTDB

launch_service()
{
	class="$1"
	iotdb_parms="-Dlogback.configurationFile=${IOTDB_CONF}/logback-datanode.xml"
	iotdb_parms="$iotdb_parms -DIOTDB_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DTSFILE_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DIOTDB_CONF=${IOTDB_CONF}"
	iotdb_parms="$iotdb_parms -DTSFILE_CONF=${IOTDB_CONF}"
	iotdb_parms="$iotdb_parms -Dname=iotdb\.IoTDB"
	exec "$JAVA" $illegal_access_params $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" "$class" $CONF_PARAMS
	return $?
}


# check whether tool 'lsof' exists
check_tool_env() {
  if  ! type lsof > /dev/null 2>&1 ; then
    echo ""
    echo " Warning: No tool 'lsof', Please install it."
    echo " Note: Some checking function need 'lsof'."
    echo ""
    return 1
  else
    return 0
  fi
}

# convert path to real full-path.
# If path has been deleted, return ""
get_real_path() {
  local path=$1
  local real_path=""
  cd $path > /dev/null 2>&1
  if [ $? -eq 0 ] ; then
    real_path=$(pwd -P)
    cd -  > /dev/null 2>&1
  fi
  echo "${real_path}"
}

# check whether same directory's IotDB server process has been running
check_running_process() {
  check_tool_env

  PIDS=$(ps ax | grep "$classname" | grep java | grep DIOTDB_HOME | grep -v grep | awk '{print $1}')
  for pid in ${PIDS}
  do
    run_conf_path=""
    run_cwd=$(lsof -p $pid 2>/dev/null | awk '$4~/cwd/ {print $NF}')
    run_home_path=$(ps -fp $pid | sed "s/ /\n/g" | sed -n "s/-DIOTDB_HOME=//p")
    run_home_path=$(get_real_path "${run_cwd}/${run_home_path}")

    #if dir ${run_home_path} has been deleted
    if [ "${run_home_path}" == "" ]; then
      continue
    fi

    current_home_path=$(get_real_path ${IOTDB_HOME})
    if [ "${run_home_path}" == "${current_home_path}" ]; then
      echo ""
      echo " Found running IoTDB server (PID=$pid)."  >&2
      echo " Can not run duplicated IoTDB server!"  >&2
      echo " Exit..."  >&2
      echo ""
      exit 1
    fi
  done
}


check_tool_env
# If needed tool is ready, check whether same directory's IotDB server is running
if [ $? -eq 0 ]; then
  check_running_process
fi

# Start up the service
launch_service "$classname"

exit $?
