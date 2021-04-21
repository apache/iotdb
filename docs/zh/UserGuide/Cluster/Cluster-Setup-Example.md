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

__集群模式目前是测试版！请谨慎在生产环境中使用。__

## 3节点3副本伪分布式搭建示例
```
mvn clean package -DskipTests
chmod -R 777 ./cluster/target/
nohup ./cluster/target/iotdb-cluster-0.12.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node1conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.12.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node2conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.12.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node3conf/ >/dev/null 2>&1 &
```

## 3节点3副本分布式搭建示例

假设我们需要在三个物理节点上部署分布式 IoTDB，这三个节点分别为 A, B 和 C，其公网 ip 分别为 A_public_ip，B_public_ip 和 C_public_ip，私网 ip 分别为 A_private_ip，B_private_ip 和 C_private_ip。
注：如果没有公网 ip 或者私网 ip 则两者设置成一致即可, 只需要保证客户端能够访问到服务端即可。

以下为操作步骤：

1. 保证三个节点的 6667, 9003, 9004, 40010, 40011 和 31999 端口是开放的。
2. 使用 `mvn clean package -pl cluster -am -DskipTests` 编译分布式模块。
3. 将打出来的包(iotdb-cluster-0.12.0-SNAPSHOT)传到所有的服务器上。
4. 配置所有节点 conf/iotdb-cluster.properties 配置文件中的 seed_nodes 为 "A_private_ip:9003,B_private_ip:9003,C_private_ip:9003"
5. 配置所有节点 conf/iotdb-cluster.properties 配置文件中的 internal_ip 为各自节点的 private_ip。
6. 配置所有节点 conf/iotdb-engine.properties 配置文件中的 rpc_address 为各自节点的 public_ip。
7. 在 3 个节点上分别运行 sh sbin/start-node.sh 即可(后台运行也可)。