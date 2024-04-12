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


src_lib_path=/d/CodeRepo/iotdb2/distribution/target/apache-iotdb-1.1.0-SNAPSHOT-all-bin/apache-iotdb-1.1.0-SNAPSHOT-all-bin/lib/iotdb*

ips=(ecs1 ecs2 ecs3 ecs4)
#ips=(dc11 dc12 dc13 dc14 dc11 dc12)
target_lib_path=/root/jt/iotdb_expr/apache-iotdb-1.1.0-SNAPSHOT-all-bin/lib/

for ip in ${ips[*]}
  do
    ssh root@$ip "mkdir $target_lib_path"
    scp -r $src_lib_path root@$ip:$target_lib_path
  done