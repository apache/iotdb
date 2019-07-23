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

# Chapter 4: Deployment and Management

## Deployment

IoTDB provides you two installation methods, you can refer to the following suggestions, choose one of them:

* Installation from binary files. Download the binary files from the official website. This is the recommended method, in which you will get a binary released package which is out-of-the-box.
* Installation from source code. If you need to modify the code yourself, you can use this method.

### Prerequisites

To install and use IoTDB, you need to have:

1. Java >= 1.8 (Please make sure the environment path has been set)
2. Maven >= 3.0 (If you want to compile and install IoTDB from source code)
3. TsFile >= 0.8.0 (TsFile Github page: [https://github.com/thulab/tsfile](https://github.com/thulab/tsfile))
4. IoTDB-JDBC >= 0.8.0 (IoTDB-JDBC Github page: [https://github.com/thulab/iotdb-jdbc](https://github.com/thulab/iotdb-jdbc))

TODO: TsFile and IoTDB-JDBC dependencies will be removed after the project reconstruct.

### Installation from  binary files

IoTDB provides you binary files which contains all the necessary components for the IoTDB system to run. You can get them on our website [http://tsfile.org/download](http://tsfile.org/download). 

```
NOTE:
iotdb-<version>.tar.gz # For Linux or MacOS
iotdb-<version>.zip # For Windows
```

After downloading, you can extract the IoTDB tarball using the following operations:

```
Shell > uzip iotdb-<version>.zip # For Windows
Shell > tar -zxf iotdb-<version>.tar.gz # For Linux or MacOS
```

The IoTDB project will be at the subfolder named iotdb. The folder will include the following contents:

```
iotdb/     <-- root path
|
+- bin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
|
+- LICENSE    <-- LICENSE
```

### Installation from source code

Use git to get IoTDB source code:

```
Shell > git clone https://github.com/apache/incubator-iotdb.git
```

Or:

```
Shell > git clone git@github.com:apache/incubator-iotdb.git
```

Now suppose your directory is like this:

```
> pwd
/workspace/incubator-iotdb

> ls -l
incubator-iotdb/     <-- root path
|
+- iotdb/
|
+- jdbc/
|
+- iotdb-cli/
|
...
|
+- pom.xml
```

Let $IOTDB_HOME = /workspace/incubator-iotdb/iotdb/iotdb/
Let $IOTDB_CLI_HOME = /workspace/incubator-iotdb/iotdb-cli/cli/

Note:
* if `IOTDB_HOME` is not explicitly assigned, 
then by default `IOTDB_HOME` is the direct parent directory of `bin/start-server.sh` on Unix/OS X 
(or that of `bin\start-server.bat` on Windows).

* if `IOTDB_CLI_HOME` is not explicitly assigned, 
then by default `IOTDB_CLI_HOME` is the direct parent directory of `bin/start-client.sh` on 
Unix/OS X (or that of `bin\start-client.bat` on Windows).

If you are not the first time that building IoTDB, remember deleting the following files:

```
> rm -rf $IOTDB_HOME/data/
> rm -rf $IOTDB_HOME/lib/
```

Then under the root path of incubator-iotdb, you can build IoTDB using Maven:

```
> pwd
/workspace/incubator-iotdb

> mvn clean package -pl iotdb -am -Dmaven.test.skip=true
```

If successful, you will see the the following text in the terminal:

```
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO]
[INFO] IoTDB Root ......................................... SUCCESS [  7.020 s]
[INFO] TsFile ............................................. SUCCESS [ 10.486 s]
[INFO] Service-rpc ........................................ SUCCESS [  3.717 s]
[INFO] IoTDB Jdbc ......................................... SUCCESS [  3.076 s]
[INFO] IoTDB .............................................. SUCCESS [  8.258 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

Otherwise, you may need to check the error statements and fix the problems.

After building, the IoTDB project will be at the subfolder named iotdb. The folder will include the following contents:

```
$IOTDB_HOME/
|
+- bin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
```

<!-- > NOTE: We also provide already built JARs and project at [http://tsfile.org/download](http://tsfile.org/download) instead of build the jar package yourself. -->

### Installation by Docker (Dockerfile)

You can build and run a IoTDB docker image by following the guide of [Deployment by Docker](/#/Documents/0.8.0/chap4/sec7)
