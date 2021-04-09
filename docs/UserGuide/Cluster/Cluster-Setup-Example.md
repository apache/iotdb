<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

__NOTICE: CURRENT IoTDB CLUSTER IS FOR TESTING NOW! 
PLEASE BE DELIBERATE IF YOU RUN IT IN PRODUCT ENVIRONMENT.__

## Example of pseudo-distributed scaffolding for 3 nodes and 3 replicas
```
mvn clean package -DskipTests
chmod -R 777 ./cluster/target/
nohup ./cluster/target/iotdb-cluster-0.12.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node1conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.12.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node2conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.12.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node3conf/ >/dev/null 2>&1 &
```

## Example of distributed scaffolding for 3 nodes and 3 replicas

Suppose we need to deploy the distributed IoTDB on three physical nodes, A, B, and C, whose public network IP is a_public_IP, b_public_IP, and c_public_IP, and private network IP is a_private_IP, b_private_IP, and c_private_IP.
Note: If there is no public network IP or private network IP, both can be set to the same, just need to ensure that the client can access the server.

The operation steps are as follows:

1. Make sure ports 6667, 9003, 9004, 40010, 40011 and 31999 are open on all three nodes.
2. Use 'mvn clean package -pl cluster -am -DskipTests' to compile the distributed module.
3. Send the generated package (iotdb-cluster-0.12.0-SNAPSHOT) to all servers.
4. Configure all nodes' seed_nodes in conf/iotdb-cluster.properties as "A_private_ip:9003,B_private_ip:9003,C_private_ip:9003"
5. Configure the internal_ip in conf/iotdb-cluster.properties to be the private_ip of each node.
6. Configure rpc_address in conf/iotdb-engine.properties to be the public_ip of each node.
7. Run sh sbin/start-node.sh on each of the three nodes (or run in the background).