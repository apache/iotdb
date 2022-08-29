#!/bin/sh
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

IOTDB_CONF="`dirname "$0"`/../conf"
rpc_port=`sed '/^rpc_port=/!d;s/.*=//' ${IOTDB_CONF}/iotdb-datanode.properties`
PID=""

function getPid {
  if  type lsof > /dev/null 2>&1 ; then
    PID=$(lsof -t -i:${rpc_port} -sTCP:LISTEN)
  elif type netstat > /dev/null 2>&1 ; then
    PID=$(netstat -anp 2>/dev/null | grep ":${rpc_port} " | grep ' LISTEN ' | awk '{print $NF}' | sed "s|/.*||g" )
  else
    echo ""
    echo " Error: No necessary tool."
    echo " Please install 'lsof' or 'netstat'."
    exit 1
  fi
}

getPid
if [ -z "$PID" ]; then
  echo "No IoTDB server to stop."
  exit 1
fi

PIDS=$(ps ax | grep -i 'IoTDB' | grep java | grep -v grep | awk '{print $1}')
sig=0
for every_pid in ${PIDS}
do
  if [ "$every_pid" = "$PID" ]; then
    sig=1
    break
  fi
done

if [ $sig -eq 0 ]; then
  echo "No IoTDB server to stop"
  exit 1
fi

echo -n "Begin to stop IoTDB ..."
kill -s TERM $PID
for ((i=1; i<=1000;i++))   #check status in 100 sec
do
  if [ ! -d "/proc/$PID" ]; then
    echo " closed gracefully."
    exit 0
  fi
  if [ $((i%10)) -eq 0 ]; then
    echo -n "."
  fi
  usleep 100000
done

kill -s KILL $PID
echo " forced to kill."
