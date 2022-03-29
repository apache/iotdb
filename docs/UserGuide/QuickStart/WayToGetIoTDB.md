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

>Note: If you don't have maven installed, you should replace 'mvn' in the following commands with 'mvnw.sh' or 'mvnw.cmd'.
>
>### Installation from  binary files

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

### Installation by Docker (Dockerfile)

Apache IoTDB' Docker image is released on [https://hub.docker.com/r/apache/iotdb](https://hub.docker.com/r/apache/iotdb),


1. **Get IoTDB docker image**
   - **Recommended:** Using `docker pull apache/iotdb:latest` can get the latest docker image.
   - Users can also build a docker image themselves. Now a Dockerfile has been written at docker/src/main/Dockerfile.
     - Way 1: `$ docker build -t iotdb:base git://github.com/apache/iotdb#master:docker`
     - Way 2: 
     ```shell
      $ git clone https://github.com/apache/iotdb
      $ cd iotdb
      $ cd docker
      $ docker build -t iotdb:base .
		```

Once the docker image has been built locally (the tag is iotdb:base in this example), you are almost done!

2. **Create docker volume for data files and logs:**
```shell
$ docker volume create mydata
$ docker volume create mylogs
```
3. **Run a docker container:**
```shell
$ docker run -p 6667:6667 -v mydata:/iotdb/data -v mylogs:/iotdb/logs -d iotdb:base /iotdb/bin/start-server.sh
```
If success, you can run `docker ps`, and get something like the following:
```
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                               NAMES
2a68b6944cb5        iotdb:base          "/iotdb/bin/start-se…"   4 minutes ago       Up 5 minutes        0.0.0.0:6667->6667/tcp              laughing_meitner
```
You can use the above command to get the container ID:
```shell
$ docker container ls
```
suppose the ID is <C_ID>.

And get the docker IP by the following, suppose the IP is <C_IP>.:
```shell
$ docker inspect --format='{{.NetworkSettings.IPAddress}}' <C_ID>
```
Now IoTDB server has started succesfully.

4. If you just want to have a try by using iotdb-cli, you can:
```shell
$ docker exec -it <C_ID> /bin/bash
$ (now you have enter the container): /iotdb/sbin/start-cli.sh -h localhost -p 6667 -u root -pw root
```

Or,  if you have a iotdb-cli locally, execute the following command: 
```shell
$ /%IOTDB_HOME%/sbin/start-cli.sh -h localhost -p 6667 -u root -pw root
```
5. If you want to write codes to insert data and query data, please add the following dependence:
```xml
        <dependency>
            <groupId>org.apache.iotdb</groupId>
            <artifactId>iotdb-jdbc</artifactId>
            <version>0.13.0-SNAPSHOT</version>
        </dependency>
```
Some examples about how to use IoTDB with IoTDB-JDBC can be found at: https://github.com/apache/iotdb/tree/master/example/jdbc/src/main/java/org/apache/iotdb
