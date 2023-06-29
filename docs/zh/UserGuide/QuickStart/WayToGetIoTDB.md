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

## 下载与安装

IoTDB 为您提供了两种安装方式，您可以参考下面的建议，任选其中一种：

第一种，从官网下载安装包。这是我们推荐使用的安装方式，通过该方式，您将得到一个可以立即使用的、打包好的二进制可执行文件。

第二种，使用源码编译。若您需要自行修改代码，可以使用该安装方式。

### 安装环境要求

安装前请保证您的电脑上配有 JDK>=1.8 的运行环境，并配置好 JAVA_HOME 环境变量。

如果您需要从源码进行编译，还需要安装：

1. Maven >= 3.6 的运行环境，具体安装方法可以参考以下链接：[https://maven.apache.org/install.html](https://maven.apache.org/install.html)。

> 注： 也可以选择不安装，使用我们提供的'mvnw' 或 'mvnw.cmd' 工具。使用时请用'mvnw' 或 'mvnw.cmd'命令代替下文的'mvn'命令。

### 从官网下载二进制可执行文件

您可以从 [http://iotdb.apache.org/Download/](http://iotdb.apache.org/Download/) 上下载已经编译好的可执行程序 iotdb-xxx.zip，该压缩包包含了 IoTDB 系统运行所需的所有必要组件。

下载后，您可使用以下操作对 IoTDB 的压缩包进行解压：

```
Shell > unzip iotdb-<version>.zip
```

### 使用源码编译

您可以获取已发布的源码 [https://iotdb.apache.org/Download/](https://iotdb.apache.org/Download/) ，或者从 [https://github.com/apache/iotdb/tree/master](https://github.com/apache/iotdb/tree/master) git 仓库获取

源码克隆后，进入到源码文件夹目录下。如果您想编译已经发布过的版本，可以先用`git checkout -b my_{project.version} v{project.version}`命令新建并切换分支。比如您要编译0.12.4这个版本，您可以用如下命令去切换分支：

```shell
> git checkout -b my_0.12.4 v0.12.4
```

切换分支之后就可以使用以下命令进行编译：

```
> mvn clean package -pl iotdb-core/datanode -am -Dmaven.test.skip=true
```

编译后，IoTDB 服务器会在 "server/target/iotdb-server-{project.version}" 文件夹下，包含以下内容：

```
+- sbin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
|
+- tools/      <-- system tools
```

如果您想要编译项目中的某个模块，您可以在源码文件夹中使用`mvn clean package -pl {module.name} -am -DskipTests`命令进行编译。如果您需要的是带依赖的 jar 包，您可以在编译命令后面加上`-P get-jar-with-dependencies`参数。比如您想编译带依赖的 jdbc jar 包，您就可以使用以下命令进行编译：  

```shell
> mvn clean package -pl iotdb-client/jdbc -am -DskipTests -P get-jar-with-dependencies
```

编译完成后就可以在`{module.name}/target`目录中找到需要的包了。


### 通过 Docker 安装

Apache IoTDB 的 Docker 镜像已经上传至 [https://hub.docker.com/r/apache/iotdb](https://hub.docker.com/r/apache/iotdb)。
Apache IoTDB 的配置项以环境变量形式添加到容器内。

#### 简单尝试
```shell
# 获取镜像
docker pull apache/iotdb:1.1.0-standalone
# 创建 docker bridge 网络
docker network create --driver=bridge --subnet=172.18.0.0/16 --gateway=172.18.0.1 iotdb
# 创建 docker 容器
# 注意：必须固定IP部署。IP改变会导致 confignode 启动失败。
docker run -d --name iotdb-service \
              --hostname iotdb-service \
              --network iotdb \
              --ip 172.18.0.6 \
              -p 6667:6667 \
              -e cn_internal_address=iotdb-service \
              -e cn_target_config_node_list=iotdb-service:10710 \
              -e cn_internal_port=10710 \
              -e cn_consensus_port=10720 \
              -e dn_rpc_address=iotdb-service \
              -e dn_internal_address=iotdb-service \
              -e dn_target_config_node_list=iotdb-service:10710 \
              -e dn_mpp_data_exchange_port=10740 \
              -e dn_schema_region_consensus_port=10750 \
              -e dn_data_region_consensus_port=10760 \
              -e dn_rpc_port=6667 \
              apache/iotdb:1.1.0-standalone              
# 尝试使用命令行执行SQL
docker exec -ti iotdb-service /iotdb/sbin/start-cli.sh -h iotdb-service
```
外部连接：
```shell
# <主机IP/hostname> 是物理机的真实IP或域名。如果在同一台物理机，可以是127.0.0.1。
$IOTDB_HOME/sbin/start-cli.sh -h <主机IP/hostname> -p 6667
```
```yaml
# docker-compose-1c1d.yml
version: "3"
services:
  iotdb-service:
    image: apache/iotdb:1.1.0-standalone
    hostname: iotdb-service
    container_name: iotdb-service
    ports:
      - "6667:6667"
    environment:
      - cn_internal_address=iotdb-service
      - cn_internal_port=10710
      - cn_consensus_port=10720
      - cn_target_config_node_list=iotdb-service:10710
      - dn_rpc_address=iotdb-service
      - dn_internal_address=iotdb-service
      - dn_rpc_port=6667
      - dn_mpp_data_exchange_port=10740
      - dn_schema_region_consensus_port=10750
      - dn_data_region_consensus_port=10760
      - dn_target_config_node_list=iotdb-service:10710
    volumes:
        - ./data:/iotdb/data
        - ./logs:/iotdb/logs
    networks:
      iotdb:
        ipv4_address: 172.18.0.6

networks:
  iotdb:
    external: true
```
#### 集群部署
目前只支持 host 网络和 overlay 网络，不支持 bridge 网络。overlay 网络参照[1C2D](https://github.com/apache/iotdb/tree/master/docker/src/main/DockerCompose/docker-compose-cluster-1c2d.yml)的写法，host 网络如下。

假如有三台物理机，它们的hostname分别是iotdb-1、iotdb-2、iotdb-3。依次启动。
以 iotdb-2 节点的docker-compose文件为例：
```yaml
version: "3"
services:
  iotdb-confignode:
    image: apache/iotdb:1.1.0-confignode
    container_name: iotdb-confignode
    environment:
      - cn_internal_address=iotdb-2
      - cn_target_config_node_list=iotdb-1:10710
      - schema_replication_factor=3
      - cn_internal_port=10710
      - cn_consensus_port=10720
      - schema_region_consensus_protocol_class=org.apache.iotdb.consensus.ratis.RatisConsensus
      - config_node_consensus_protocol_class=org.apache.iotdb.consensus.ratis.RatisConsensus
      - data_replication_factor=3
      - data_region_consensus_protocol_class=org.apache.iotdb.consensus.iot.IoTConsensus
    volumes:
      - /etc/hosts:/etc/hosts:ro
      - ./data/confignode:/iotdb/data
      - ./logs/confignode:/iotdb/logs
    network_mode: "host"

  iotdb-datanode:
    image: apache/iotdb:1.1.0-datanode
    container_name: iotdb-datanode
    environment:
      - dn_rpc_address=iotdb-2
      - dn_internal_address=iotdb-2
      - dn_target_config_node_list=iotdb-1:10710
      - data_replication_factor=3
      - dn_rpc_port=6667
      - dn_mpp_data_exchange_port=10740
      - dn_schema_region_consensus_port=10750
      - dn_data_region_consensus_port=10760
      - data_region_consensus_protocol_class=org.apache.iotdb.consensus.iot.IoTConsensus
       - schema_replication_factor=3
      - schema_region_consensus_protocol_class=org.apache.iotdb.consensus.ratis.RatisConsensus
      - config_node_consensus_protocol_class=org.apache.iotdb.consensus.ratis.RatisConsensus
    volumes:
      - /etc/hosts:/etc/hosts:ro
      - ./data/datanode:/iotdb/data/
      - ./logs/datanode:/iotdb/logs/
    network_mode: "host"
```
注意：
1. `dn_target_config_node_list`所有节点配置一样，需要配置第一个启动的节点，这里为`iotdb-1`。
2. 上面docker-compose文件中，`iotdb-2`需要替换为每个节点的 hostname、域名或者IP地址。
3. 需要映射`/etc/hosts`，文件内配置了 iotdb-1、iotdb-2、iotdb-3 与IP的映射。或者可以在 docker-compose 文件中增加 `extra_hosts` 配置。
4. 首次启动时，必须首先启动 `iotdb-1`。
5. 如果部署失败要重新部署集群，必须将所有节点上的IoTDB服务停止并删除，然后清除`data`和`logs`文件夹后，再启动。
