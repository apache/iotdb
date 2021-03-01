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

# Download

IoTDB provides you three installation methods, you can refer to the following suggestions, choose one of them:

* Installation from source code. If you need to modify the code yourself, you can use this method.
* Installation from binary files. Download the binary files from the official website. This is the recommended method, in which you will get a binary released package which is out-of-the-box.
* Using Docker：The path to the dockerfile is https://github.com/apache/iotdb/blob/master/docker/Dockerfile

## Prerequisites

To use IoTDB, you need to have:

1. Java >= 1.8 (Please make sure the environment path has been set)
2. Maven >= 3.6 (Optional)
3. Set the max open files num as 65535 to avoid "too many open files" problem.

>Note: If you don't have maven installed, you should replace 'mvn' in the following commands with 'mvnw.sh' or 'mvnw.cmd'.
>
>### Installation from  binary files

You can download the binary file from:
[Here](/Download/)

## Installation from source code

You can get the released source code from https://iotdb.apache.org/Download/, or from the git repository https://github.com/apache/iotdb/tree/master
You can download the source code from:

```
git clone https://github.com/apache/iotdb.git
```

Under the root path of iotdb:

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

## Installation by Docker (Dockerfile)

You can build and run a IoTDB docker image by following the guide of [Deployment by Docker](../Server/Docker%20Image.md)
