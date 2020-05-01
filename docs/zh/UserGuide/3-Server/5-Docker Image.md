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

# Docker映像

现在，已在分支enable_docker_image上的ROOT / docker / Dockerfile处写入了Dockerfile。

1. 您可以通过以下方式构建docker映像：
```
$ docker build -t iotdb:base git://github.com/apache/incubator-iotdb#master:docker
```
或者：
```
$ git clone https://github.com/apache/incubator-iotdb
$ cd incubator-iotdb
$ cd docker
$ docker build -t iotdb:base .
```
一旦在本地构建了docker映像（在此示例中，标签为iotdb：base），您就快完成了！

2. 为数据文件和日志创建docker卷：
```
$ docker volume create mydata
$ docker volume create mylogs
```
3. 运行一个Docker容器：
```shell
$ docker run -p 6667:6667 -v mydata:/iotdb/data -v mylogs:/iotdb/logs -d iotdb:base /iotdb/bin/start-server.sh
```
如果成功，您可以运行`docker ps`，并获得如下内容：
```
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                               NAMES
2a68b6944cb5        iotdb:base          "/iotdb/bin/start-se…"   4 minutes ago       Up 5 minutes        0.0.0.0:6667->6667/tcp              laughing_meitner
```
您可以使用上面的命令来获取容器ID：
```
$ docker container ls
```
假设ID为<C_ID>。

并通过以下方式获取docker IP：
```
$ docker inspect --format='{{.NetworkSettings.IPAddress}}' <C_ID>
```
假设IP为<C_IP>。

4. 如果您只想尝试使用iotdb-cli，则可以：
```
$ docker exec -it /bin/bash  <C_ID>
$ (now you have enter the container): /cli/sbin/start-client.sh -h localhost -p 6667 -u root -pw root
```

或者，运行一个新的Docker容器作为客户端：
```
$ docker run -it iotdb:base /cli/sbin/start-client.sh -h <C_IP> -p 6667 -u root -pw root
```
或者，如果您在本地拥有一个iotdb-cli（例如，您已通过`mvn package`编译了源代码），并且假设您的work_dir为cli / bin，则可以运行：
```
$ start-client.sh -h localhost -p 6667 -u root -pw root
```
5. 如果要编写代码以插入数据和查询数据，请添加以下依赖项：
```xml
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-jdbc</artifactId>
            <version>0.9.3</version>
        </dependency>
```
可以在以下位置找到有关如何将IoTDB与IoTDB-JDBC一起使用的一些示例： https://github.com/apache/incubator-iotdb/tree/master/example/jdbc/src/main/java/org/apache/iotdb

6. 现在享受吧！
