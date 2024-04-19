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


src_lib_path=/d/CodeRepo/iotdb2/distribution/target/apache-iotdb-1.3.1-SNAPSHOT-all-bin/apache-iotdb-1.3.1-SNAPSHOT-all-bin/lib/iotdb*

locs=(dn1 dn2 dn3)
#ips=(dc11 dc12 dc13 dc14 dc11 dc12)
target_lib_path=/c/Users/a/Desktop/phdDemo/

for loc in ${locs[*]}
  do
    cp -r $src_lib_path $target_lib_path$loc/lib
  done