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

# Docker Image

Dockerfile 存放在的 docker 工程下的 src/main/Dockerfile 中.

1. 您可以使用下面的命令构建 docker image: 
```
$ docker build -t iotdb:base git://github.com/apache/iotdb#master:docker
```
或者:
```shell
$ git clone https://github.com/apache/iotdb
$ cd iotdb
$ cd docker
$ docker build -t iotdb:base .
```
当 docker image 在本地构建完成当时候 (示例中的 tag为 iotdb:base)，已经距完成只有一步之遥了!

2. 创建数据文件和日志的 docker 挂载目录(docker volume):
```
$ docker volume create mydata
$ docker volume create mylogs
```
3. 运行 docker container:
```shell
$ docker run -p 6667:6667 -v mydata:/iotdb/data -v mylogs:/iotdb/logs -d iotdb:base /iotdb/bin/start-server.sh
```
您可以使用`docker ps`来检查是否运行成功，当成功时控制台会输出下面的日志:
```
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                               NAMES
2a68b6944cb5        iotdb:base          "/iotdb/bin/start-se…"   4 minutes ago       Up 5 minutes        0.0.0.0:6667->6667/tcp              laughing_meitner
```
您可以使用下面的命令来获取 container 的 ID: 
```
$ docker container ls
```
假设这个 ID 为 <C_ID>.

然后使用下面的命令获取这个 ID 对应的 IP 地址:
```
$ docker inspect --format='{{.NetworkSettings.IPAddress}}' <C_ID>
```
假设获取的 IP 为 <C_IP>.

4. 如果您想尝试使用 iotdb-cli 命令行, 您可以使用如下命令:
```
$ docker exec -it /bin/bash  <C_ID>
$ (now you have enter the container): /cli/sbin/start-cli.sh -h localhost -p 6667 -u root -pw root
```

或者运行一个新的 client docker container，命令如下:
```
$ docker run -it iotdb:base /cli/sbin/start-cli.sh -h <C_IP> -p 6667 -u root -pw root
```
还可以使用本地的 iotdb-cli (比如：您已经使用`mvn package`编译过源码), 假设您的 work_dir 是 cli/bin, 那么您可以直接运行:
```
$ start-cli.sh -h localhost -p 6667 -u root -pw root
```
5. 如果您想写一些代码来插入或者查询数据，您可以在 pom.xml 文件中加入下面的依赖:
```xml
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-jdbc</artifactId>
            <version>0.10.0</version>
        </dependency>
```
这里是一些使用 IoTDB-JDBC 连接 IoTDB 的示例: https://github.com/apache/iotdb/tree/master/example/jdbc/src/main/java/org/apache/iotdb

6. 现在已经大功告成了
