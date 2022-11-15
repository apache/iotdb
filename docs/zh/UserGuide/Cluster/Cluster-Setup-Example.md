<!--

```
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
```

-->
## 集群搭建示例
### 前提条件

如果您在使用 Windows 系统，请安装 MinGW，WSL 或者 git bash。

### 1 节点 1 副本分布式搭建示例

#### 源码编译：

```
mvn clean package -DskipTests
chmod -R 777 ./cluster/target/
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh >/dev/null 2>&1 &
```

#### 使用官网发布版本：

```
curl -O https://downloads.apache.org/iotdb/0.12.1/apache-iotdb-0.12.1-cluster-bin.zip
unzip apache-iotdb-0.12.1-cluster-bin.zip
cd apache-iotdb-0.12.1-cluster-bin
sed -i -e 's/^seed_nodes=127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007$/seed_nodes=127.0.0.1:9003/g' conf/iotdb-cluster.properties
sed -i -e 's/^default_replica_num=3$/default_replica_num=1/g' conf/iotdb-cluster.properties
nohup ./sbin/start-node.sh >/dev/null 2>&1 &
```

### 单机部署 3 节点 1 副本示例

#### 配置

通过自己修改配置来处理端口和文件目录冲突，可以在一台机器上启动多个实例。

**节点1**:**(默认)**

***iotdb-cluster.properties***

```
seed_nodes = 127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007
default_replica_num = 1
internal_meta_port = 9003
internal_data_port = 40010
```

***iotdb-engine.properties***

```
rpc_port=6667
```

**节点2**:

***iotdb-cluster.properties***

```
seed_nodes = 127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007
default_replica_num = 1
internal_meta_port = 9005
internal_data_port = 40012
```

***iotdb-engine.properties***

```
rpc_port=6669
```

**节点3**:

***iotdb-cluster.properties***

```
seed_nodes = 127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007
default_replica_num = 1
internal_meta_port = 9007
internal_data_port = 40014
```

***iotdb-engine.properties***

```
rpc_port=6671
```

#### 源码编译：

```
mvn clean package -DskipTests
chmod -R 777 ./cluster/target/
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node1conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node2conf/ >/dev/null 2>&1 &
nohup ./cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/sbin/start-node.sh ./cluster/target/test-classes/node3conf/ >/dev/null 2>&1 &
```

#### 使用官网发布版本:

下载发布版本:

```
curl -O https://downloads.apache.org/iotdb/0.12.1/apache-iotdb-0.12.1-cluster-bin.zip
```

解压压缩包:

```
unzip apache-iotdb-0.12.1-cluster-bin.zip
```

进入IoTDB集群根目录:

```
cd apache-iotdb-0.12.1-cluster-bin
```

设置 default\_replica\_num = 1:

```
sed -i -e 's/^default_replica_num=3$/default_replica_num=1/g' conf/iotdb-cluster.properties
```

为节点2，节点3创建conf\_dir:

```
cp -r conf node2_confcp -r conf node3_conf
```

解决端口和文件目录冲突:

```
sed -i -e 's/^internal_meta_port=9003$/internal_meta_port=9005/g' -e 's/^internal_data_port=40010$/internal_data_port=40012/g' node2_conf/iotdb-cluster.properties
sed -i -e 's/^internal_meta_port=9003$/internal_meta_port=9007/g' -e 's/^internal_data_port=40010$/internal_data_port=40014/g' node3_conf/iotdb-cluster.properties
sed -i -e 's/^rpc_port=6667$/rpc_port=6669/g' -e node2_conf/iotdb-engine.properties
sed -i -e 's/^rpc_port=6667$/rpc_port=6671/g' -e node3_conf/iotdb-engine.properties
sed -i -e 's/^# data_dirs=data\/data$/data_dirs=node2\/data/g' -e 's/^# wal_dir=data\/wal$/wal_dir=node2\/wal/g' -e 's/^# tracing_dir=data\/tracing$/tracing_dir=node2\/tracing/g' -e 's/^# system_dir=data\/system$/system_dir=node2\/system/g' -e 's/^# udf_root_dir=ext\/udf$/udf_root_dir=node2\/ext\/udf/g' -e 's/^# index_root_dir=data\/index$/index_root_dir=node2\/index/g' node2_conf/iotdb-engine.properties
sed -i -e 's/^# data_dirs=data\/data$/data_dirs=node3\/data/g' -e 's/^# wal_dir=data\/wal$/wal_dir=node3\/wal/g' -e 's/^# tracing_dir=data\/tracing$/tracing_dir=node3\/tracing/g' -e 's/^# system_dir=data\/system$/system_dir=node3\/system/g' -e 's/^# udf_root_dir=ext\/udf$/udf_root_dir=node3\/ext\/udf/g' -e 's/^# index_root_dir=data\/index$/index_root_dir=node3\/index/g' node3_conf/iotdb-engine.properties
```

**你可以自己修改配置项而不使用“sed”命令**

根据配置文件路径启动三个节点:


```
nohup ./sbin/start-node.sh >/dev/null 2>&1 &nohup ./sbin/start-node.sh ./node2_conf/ >/dev/null 2>&1 &nohup ./sbin/start-node.sh ./node3_conf/ >/dev/null 2>&1 &
```



### 3 节点 3 副本分布式搭建示例

假设我们需要在三个物理节点上部署分布式 IoTDB，这三个节点分别为 A, B 和 C，其公网 ip 分别为 A\_public\_IP*, *B\_public\_IP*, and *C\_public\_IP*，私网 ip 分别为 *A\_private\_IP*, *B\_private\_IP*, and *C\_private\_IP*.

注：如果没有公网 ip 或者私网 ip 则两者**设置成一致**即可，只需要保证客户端能够访问到服务端即可。 私网ip对应iotdb-cluster.properties中的`internal_ip`配置项，公网ip对应iotdb-engine.properties中的`rpc_address`配置项。
#### 配置

**节点A**:

***iotdb-cluster.properties***

```
seed_nodes = A_private_Ip:9003,B_private_Ip:9003,C_private_Ip:9003
default_replica_num = 3
internal_meta_port = 9003
internal_data_port = 40010
internal_ip = A_private_Ip
```

***iotdb-engine.properties***

```
rpc_port = 6667
rpc_address = A_public_ip 
```

**节点B**:

***iotdb-cluster.properties***

```
seed_nodes = A_private_Ip:9003,B_private_Ip:9003,C_private_Ip:9003
default_replica_num = 3
internal_meta_port = 9003
internal_data_port = 40010
internal_ip = B_private_Ip
```

***iotdb-engine.properties***

```
rpc_port = 6667
rpc_address = B_public_ip 
```

**节点C**:

***iotdb-cluster.properties***

```
seed_nodes = A_private_Ip:9003,B_private_Ip:9003,C_private_Ip:9003
default_replica_num = 3
internal_meta_port = 9003
internal_data_port = 40010
internal_ip = C_private_Ip
```

***iotdb-engine.properties***

```
rpc_port = 6667
rpc_address = C_public_ip 
```


#### 启动IoTDB集群

以下为操作步骤：

* 使用 `mvn clean package -pl cluster -am -DskipTests` 编译分布式模块或直接到 [官网](https://iotdb.apache.org/Download/) 下载最新版本。
* 保证三个节点的 6567, 6667, 9003, 9004, 40010, 40011 和 31999 端口是开放的。

* 将包上传到所有的服务器上。

* 修改配置项。
* 在 3 个节点上分别运行 sh sbin/start-node.sh 即可（后台运行也可）。



#### 源码编译：
**在三个节点上分别执行操作**
```
mvn clean package -DskipTests
chmod -R 777 ./cluster/target/
cd cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/
```

#### 使用官网发布版本:
**在三个节点上分别执行操作**

下载发布版本:

```
curl -O https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-cluster-bin.zip
```

解压压缩包:

```
unzip apache-iotdb-0.12.4-cluster-bin.zip
```

进入IoTDB集群根目录:

```
cd apache-iotdb-0.12.4-cluster-bin
```

设置 default\_replica\_num = 3: 配置文件中默认为3，无需修改

设置 internal\_ip = 节点的私有ip (以192.168.1.1为例)
```
sed -i -e 's/^internal_ip=127.0.0.1$/internal_ip=192.168.1.1/g' conf/iotdb-cluster.properties
```
设置 seed\_node = A_private_Ip:9003,B_private_Ip:9003,C_private_Ip:9003 (三个节点ip分别为192.168.1.1,192.168.1.2,192.168.1.3为例)
```
sed -i -e 's/^seed_nodes=127.0.0.1:9003,127.0.0.1:9005,127.0.0.1:9007$/seed_nodes=192.168.1.1:9003,192.168.1.2:9003,192.168.1.3:9003/g' conf/iotdb-cluster.properties
```
设置 rpc\_address = 节点的公有ip (以192.168.1.1为例)
```
sed -i -e 's/^rpc_address=127.0.0.1$/rpc_address=192.168.1.1/g' conf/iotdb-engine.properties
```
**你可以自己修改配置项而不使用“sed”命令**

根据配置文件路径启动三个节点:

```
nohup ./sbin/start-node.sh >/dev/null 2>&1 &
```
