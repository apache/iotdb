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


PIDS=$(ps ax | grep -i 'ClusterMain' | grep java | grep -v grep | awk '{print $1}')
sig=0
for evry_pid in ${PIDS}
do
  cwd_path=$(ls -l /proc/$evry_pid | grep "cwd ->" | grep -v grep | awk '{print $NF}')
  pwd_path=$(/bin/pwd)
  if [[ $pwd_path =~ $cwd_path ]]; then
    kill -s TERM $evry_pid
    echo "close IoTDB"
    sig=1
  fi
done

if [ $sig -eq 0 ]; then
  echo "No IoTDB server to stop"
  exit 1
fi

