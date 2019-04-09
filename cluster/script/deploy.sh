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


## ========== package file locally ==========
## cd ../../
#cd iotdb/iotdb
#rm -rf data/
#rm -rf lib/
#rm -rf lib_cluster/
#rm -rf logs/
#cd ../..
#git pull
#mvn clean package -pl cluster -am -Dmaven.test.skip=true
#cd iotdb
#rm cluster.zip
#zip -r cluster.zip iotdb/
#cd ..

## ========== package file locally end ==========


## ========== transfer to node ==========

#alliplist=('192.168.130.12' '192.168.130.14' '192.168.130.16' '192.168.130.18' '192.168.130.19')
#for ip in ${alliplist[@]}
#do
#  ssh fit@$ip "rm /home/fit/xuyi/cluster.zip"
#  ssh fit@$ip "rm -rf /home/fit/xuyi/iotdb"
#  scp iotdb/cluster.zip fit@$ip:/home/fit/xuyi
#  ssh fit@$ip "unzip -o -d /home/fit/xuyi /home/fit/xuyi/cluster.zip "
#done
## ========== transfer to node end ==========


nodes=$1
if [ $nodes == "3" ]
then
  iplist=('192.168.130.14' '192.168.130.16' '192.168.130.18')
else
  iplist=('192.168.130.12' '192.168.130.14' '192.168.130.16' '192.168.130.18' '192.168.130.19')
fi

replication=$2


for ip in ${iplist[@]}
do
  idx="$(cut -d'.' -f4 <<<"$ip")"
  cat $3/src/test/resources/conf/$nodes-$replication-$idx.properties
#  scp $3/src/test/resources/conf/$nodes-$replication-$idx.properties fit@$ip:/home/fit/xuyi/iotdb/conf/iotdb-cluster.properties
done

for ip in ${iplist[@]}
do
  ssh fit@$ip "chmod a+x /home/fit/xuyi/iotdb/bin/stop-cluster.sh"
  ssh fit@$ip "sh /home/fit/xuyi/iotdb/bin/stop-cluster.sh"
  ssh fit@$ip "rm -rf /home/fit/xuyi/iotdb/data"
  ssh fit@$ip "rm -rf /home/fit/xuyi/iotdb/logs"
  ssh fit@$ip "chmod a+x /home/fit/xuyi/iotdb/bin/start-cluster.sh"
  ssh fit@$ip "nohup /home/fit/xuyi/iotdb/bin/start-cluster.sh > output.log 2>&1 &"
done

