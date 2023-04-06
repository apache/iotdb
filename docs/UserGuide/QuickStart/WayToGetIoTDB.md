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

## Way to get IoTDB binary files

IoTDB provides you three installation methods, you can refer to the following suggestions, choose one of them:

* Installation from source code. If you need to modify the code yourself, you can use this method.
* Installation from binary files. Download the binary files from the official website. This is the recommended method, in which you will get a binary released package which is out-of-the-box.
* Using Docker：The path to the dockerfile is https://github.com/apache/iotdb/blob/master/docker/Dockerfile

### Prerequisites

To use IoTDB, you need to have:

1. Java >= 1.8 (Please make sure the environment path has been set)
2. Maven >= 3.6 (Optional)
3. Set the max open files num as 65535 to avoid "too many open files" problem.

>Note: If you don't have maven installed, you should replace 'mvn' in the following commands with 'mvnw' or 'mvnw.cmd'.
>
>### Installation from binary files

You can download the binary file from:
[Download page](https://iotdb.apache.org/Download/)

### Installation from source code

You can get the released source code from https://iotdb.apache.org/Download/, or from the git repository https://github.com/apache/iotdb/tree/master
You can download the source code from:

```
git clone https://github.com/apache/iotdb.git
```

After that, go to the root path of IoTDB. If you want to build the version that we have released, you need to create and check out a new branch by command `git checkout -b my_{project.version} v{project.version}`. E.g., you want to build the version `0.12.4`, you can execute this command to make it:

```shell
> git checkout -b my_0.12.4 v0.12.4
```

Then you can execute this command to build the version that you want:

```
> mvn clean package -DskipTests
```

Then the binary version (including both server and client) can be found at **distribution/target/apache-iotdb-{project.version}-bin.zip**

> NOTE: Directories "thrift/target/generated-sources/thrift" and "antlr/target/generated-sources/antlr4" need to be added to sources roots to avoid compilation errors in IDE.

If you would like to build the IoTDB server, you can run the following command under the root path of iotdb:

```
> mvn clean package -pl server -am -DskipTests
```

After build, the IoTDB server will be at the folder "server/target/iotdb-server-{project.version}". 

If you would like to build a module, you can execute command `mvn clean package -pl {module.name} -am -DskipTests` under the root path of IoTDB.
If you need the jar with dependencies, you can add parameter `-P get-jar-with-dependencies` after the command. E.g., If you need the jar of jdbc with dependencies, you can execute this command:

```shell
> mvn clean package -pl jdbc -am -DskipTests -P get-jar-with-dependencies
```

Then you can find it under the path `{module.name}/target`.

### Installation by Docker 
Apache IoTDB' Docker image is released on [https://hub.docker.com/r/apache/iotdb](https://hub.docker.com/r/apache/iotdb)
Add environments of docker to update the configurations of Apache IoTDB.
#### Have a try
```shell
# get IoTDB official image
docker pull apache/iotdb:1.1.0-standalone
# create docker bridge network
docker network create --driver=bridge --subnet=172.18.0.0/16 --gateway=172.18.0.1 iotdb
# create docker container
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
# execute SQL
docker exec -ti iotdb-service /iotdb/sbin/start-cli.sh -h iotdb-service
```
External access：
```shell
# <IP Address/hostname> is the real IP or domain address rather than the one in docker network, could be 127.0.0.1 within the computer.
$IOTDB_HOME/sbin/start-cli.sh -h <IP Address/hostname> -p 6667
```
Notice：The confignode service would fail when restarting this container if the IP Adress of the container has been changed.
```yaml
# docker-compose-standalone.yml
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
#### deploy cluster
Until now, we support host and overlay networks but haven't supported bridge networks on multiple computers.
Overlay networks see [1C2D](https://github.com/apache/iotdb/tree/master/docker/src/main/DockerCompose/docker-compose-cluster-1c2d.yml) and here are the configurations and operation steps to start an IoTDB cluster with docker using host networks。

Suppose that there are three computers of iotdb-1, iotdb-2 and iotdb-3. We called them nodes.
Here is the docker-compose file of iotdb-2, as the sample:
```yaml
version: "3"
services:
  iotdb-confignode:
    image: apache/iotdb:1.1.0-confignode
    container_name: iotdb-confignode
    environment:
      - cn_internal_address=iotdb-2
      - cn_target_config_node_list=iotdb-1:10710
      - cn_internal_port=10710
      - cn_consensus_port=10720
      - schema_replication_factor=3
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
Notice：
1. The `dn_target_config_node_list` of three nodes must the same and it is the first starting node of `iotdb-1` with the cn_internal_port of 10710。
2. In this docker-compose file，`iotdb-2` should be replace with the real IP or hostname of each node to generate docker compose files in the other nodes.
3. The services would talk with each other, so they need map the /etc/hosts file or add the `extra_hosts` to the docker compose file.
4. We must start the IoTDB services of `iotdb-1` first at the first time of starting.
5. Stop and remove all the IoTDB services and clean up the `data` and `logs` directories of the 3 nodes，then start the cluster again.
