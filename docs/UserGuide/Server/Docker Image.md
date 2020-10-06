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

Now a Dockerfile has been written at docker/src/main/Dockerfile.

1. You can build a docker image by: 
```
$ docker build -t iotdb:base git://github.com/apache/iotdb#master:docker
```
Or:
```
$ git clone https://github.com/apache/iotdb
$ cd iotdb
$ cd docker
$ docker build -t iotdb:base .
```
Once the docker image has been built locally (the tag is iotdb:base in this example), you are almost done!

2. create docker volume for data files and logs:
```
$ docker volume create mydata
$ docker volume create mylogs
```
3. run a docker container:
```shell
$ docker run -p 6667:6667 -v mydata:/iotdb/data -v mylogs:/iotdb/logs -d iotdb:base /iotdb/bin/start-server.sh
```
If success, you can run `docker ps`, and get something like the following:
```
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                               NAMES
2a68b6944cb5        iotdb:base          "/iotdb/bin/start-se…"   4 minutes ago       Up 5 minutes        0.0.0.0:6667->6667/tcp              laughing_meitner
```
You can use the above command to get the container ID: 
```
$ docker container ls
```
suppose the ID is <C_ID>.

And get the docker IP by:
```
$ docker inspect --format='{{.NetworkSettings.IPAddress}}' <C_ID>
```
suppose the IP is <C_IP>.

4. If you just want to have a try by using iotdb-cli, you can:
```
$ docker exec -it /bin/bash  <C_ID>
$ (now you have enter the container): /cli/sbin/start-cli.sh -h localhost -p 6667 -u root -pw root
```

Or,  run a new docker container as the client:
```
$ docker run -it iotdb:base /cli/sbin/start-cli.sh -h <C_IP> -p 6667 -u root -pw root
```
Or,  if you have a iotdb-cli locally (e.g., you have compiled the source code by `mvn package`), and suppose your work_dir is cli/bin, then you can just run:
```
$ start-cli.sh -h localhost -p 6667 -u root -pw root
```
5. If you want to write codes to insert data and query data, please add the following dependence:
```xml
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-jdbc</artifactId>
            <version>0.10.0</version>
        </dependency>
```
Some examples about how to use IoTDB with IoTDB-JDBC can be found at: https://github.com/apache/iotdb/tree/master/example/jdbc/src/main/java/org/apache/iotdb

6. Now enjoy it!
