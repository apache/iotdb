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


## Only for test
nodes=$1
if [ $nodes == "1" ]
then
  iplist=('192.168.130.14')
elif [ $nodes == "3" ]
then
  iplist=('192.168.130.12' '192.168.130.15' '192.168.130.14')
elif [ $nodes == "5" ]
then
  iplist=('192.168.130.12' '192.168.130.13' '192.168.130.14' '192.168.130.16' '192.168.130.18')
elif [ $nodes == "7" ]
then
  iplist=('192.168.130.8' '192.168.130.12' '192.168.130.13' '192.168.130.14' '192.168.130.15' '192.168.130.16' '192.168.130.18')
elif [ $nodes == "9" ]
then
  iplist=('192.168.130.6' '192.168.130.7' '192.168.130.8' '192.168.130.12' '192.168.130.13' '192.168.130.14' '192.168.130.15' '192.168.130.16' '192.168.130.18')
elif [ $nodes == "10" ]
then
  iplist=('192.168.130.5' '192.168.130.6' '192.168.130.7' '192.168.130.8' '192.168.130.12' '192.168.130.13' '192.168.130.14' '192.168.130.15' '192.168.130.16' '192.168.130.18')
else
  echo "node number error"
  exit 1;
fi

replication=$2


for ip in ${iplist[@]}
do
  idx="$(cut -d'.' -f4 <<<"$ip")"
  cat $3/src/test/resources/conf/$nodes-$replication-$idx.properties
  scp $3/src/test/resources/conf/$nodes-$replication-$idx.properties fit@$ip:/home/fit/xuyi/incubator-iotdb/iotdb/iotdb/conf/iotdb-cluster.properties
done
