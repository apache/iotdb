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

## 前提条件
如果您在使用 Windows 系统，请安装 MinGW，WSL 或者 git bash。

## 3 节点 3 副本分布式搭建示例

假设我们需要在三个物理节点上部署分布式 IoTDB，这三个节点分别为 A, B 和 C，其公网 ip 分别为 A_public_ip，B_public_ip 和 C_public_ip，私网 ip 分别为 A_private_ip，B_private_ip 和 C_private_ip。
注：如果没有公网 ip 或者私网 ip 则两者设置成一致即可，只需要保证客户端能够访问到服务端即可。

以下为操作步骤：
1. 使用 `mvn clean package -pl cluster -am -DskipTests` 编译分布式模块或直接到 [官网](https://iotdb.apache.org/Download/) 下载最新版本。
2. 保证三个节点的 6567, 6667, 9003, 9004, 40010, 40011 和 31999 端口是开放的。
3. 将包上传到所有的服务器上。
4. 配置所有节点 conf/iotdb-cluster.properties 配置文件中的 seed_nodes 为 "A_private_ip:9003,B_private_ip:9003,C_private_ip:9003"
5. 配置所有节点 conf/iotdb-cluster.properties 配置文件中的 internal_ip 为各自节点的 private_ip。
6. 配置所有节点 conf/iotdb-cluster.properties 配置文件中的 default_replica_num 为 3。
7. 配置所有节点 conf/iotdb-engine.properties 配置文件中的 rpc_address 为各自节点的 public_ip。
8. 在 3 个节点上分别运行 sh sbin/start-node.sh 即可（后台运行也可）。

## 1 节点 1 副本分布式搭建示例
### 源码编译：
```
mvn clean package -DskipTests
chmod -R 777 ./cluster/target/
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh >/dev/null 2>&1 &
```
### 使用官网发布版本：
```
curl -O https://mirrors.bfsu.edu.cn/apache/iotdb/0.12.0/apache-iotdb-0.12.0-cluster-bin.zip
unzip apache-iotdb-0.12.0-cluster-bin.zip
cd apache-iotdb-0.12.0-cluster-bin
sed -i -e 's/^seed_nodes=127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007$/seed_nodes=127.0.0.1:9003/g' conf/iotdb-cluster.properties
sed -i -e 's/^default_replica_num=3$/default_replica_num=1/g' conf/iotdb-cluster.properties
nohup ./sbin/start-node.sh >/dev/null 2>&1 &
```

## 单机部署 3 节点 1 副本示例
### 源码编译：
```
mvn clean package -DskipTests
chmod -R 777 ./cluster/target/
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node1conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node2conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node3conf/ >/dev/null 2>&1 &
```
### 使用官网发布版本：
可以参考以上示例，不过在单机启动多个实例时需要处理好端口和文件目录的冲突。