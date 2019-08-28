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

<!-- TOC -->

## Outline

- Quick Start
 - Prerequisites
    - Installation
        - Installation from source code
    - Configure
    - Start
        - Start Server
        - Start Cli
        - Have a try
        - Stop Server

<!-- /TOC -->

# Quick Start

This short guide will walk you through the basic process of using IoTDB. For a more-complete guide, please visit our website's [User Guide](https://iotdb.apache.org/#/Documents/0.8.0/chap1/sec1).

## Prerequisites

To use IoTDB, you need to have:

1. Java >= 1.8 (Please make sure the environment path has been set)
2. Maven >= 3.1 (If you want to compile and install IoTDB from source code)

## Installation

IoTDB provides you two installation methods, you can refer to the following suggestions, choose one of them:

* Installation from source code. If you need to modify the code yourself, you can use this method.
* Installation from binary files. Download the binary files from the official website. This is the recommended method, in which you will get a binary released package which is out-of-the-box.(Comming Soon...)

Here in the Quick Start, we give a brief introduction of using source code to install IoTDB. For further information, please refer to Chapter 4 of the User Guide.

## Build from source

```
> mvn clean package -Papache-release -DskipTests
```

Then the binary version (including both server and client) can be found at **distribution/target/apache-iotdb-{project.version}-incubating-bin.zip**

> NOTE: Directories "service-rpc/target/generated-sources/thrift" and "server/target/generated-sources/antlr3" need to be added to sources roots to avoid compilation errors in IDE.

### Configurations

configuration files are under "conf" folder

  * environment config module (`iotdb-env.bat`, `iotdb-env.sh`), 
  * system config module (`tsfile-format.properties`, `iotdb-engine.properties`)
  * log config module (`logback.xml`). 

For more, see [Chapter4: Deployment and Management](https://iotdb.apache.org/#/Documents/0.8.0/chap4/sec1) in detail.

### Start server

```
# Unix/OS X
> sbin/start-server.sh

# Windows
> sbin\start-server.bat
```

### Stop Server

The server can be stopped with ctrl-C or the following script:

```
# Unix/OS X
> sbin/stop-server.sh

# Windows
> sbin\stop-server.bat
```

### Using client 

```
# Unix/OS X
> sbin/start-cli.sh -h <IP> -p <PORT> -u <USER_NAME>

# Windows
> sbin\start-cli.bat -h <IP> -p <PORT> -u <USER_NAME>
```

> The default user is 'root'. The default password for 'root' is 'root'.

> The default parameters are "-h 127.0.0.1 -p 6667 -u root -pw root".

``` 
IoTDB> set storage group to root.vehicle

IoTDB> create timeseries root.vehicle.d0.s0 with datatype=INT32, encoding=RLE

IoTDB> show timeseries root
+-----------------------------+---------------------+--------+--------+
|                   Timeseries|        Storage Group|DataType|Encoding|
+-----------------------------+---------------------+--------+--------+
|           root.vehicle.d0.s0|         root.vehicle|   INT32|     RLE|
+-----------------------------+---------------------+--------+--------+

IoTDB> insert into root.vehicle.d0(timestamp,s0) values(1,101);

IoTDB> SELECT d0.s0 FROM root.vehicle
+-----------------------------+------------------+
|                         Time|root.vehicle.d0.s0|
+-----------------------------+------------------+
|1970-01-01T08:00:00.001+08:00|               101|
+-----------------------------+------------------+
Total line number = 1

IoTDB> quit/exit
```

For more on what commands are supported by IoTDB SQL, see [Chapter 5: IoTDB SQL Documentation](https://iotdb.apache.org/#/Documents/0.8.0/chap5/sec1).

### Usage of import-csv.sh

### Create metadata
```
SET STORAGE GROUP TO root.fit.d1;
SET STORAGE GROUP TO root.fit.d2;
SET STORAGE GROUP TO root.fit.p;
CREATE TIMESERIES root.fit.d1.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d1.s2 WITH DATATYPE=TEXT,ENCODING=PLAIN;
CREATE TIMESERIES root.fit.d2.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d2.s3 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.p.s1 WITH DATATYPE=INT32,ENCODING=RLE;
```

### Run import shell
```
# Unix/OS X
> tools/import-csv.sh -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv>

# Windows
> tools\import-csv.bat -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv>
```

### Error data file

`csvInsertError.error`

# Usage of export-csv.sh

### Run export shell
```
# Unix/OS X
> tools/export-csv.sh -h <ip> -p <port> -u <username> -pw <password> -td <xxx.csv> [-tf <time-format>]

# Windows
> tools\export-csv.bat -h <ip> -p <port> -u <username> -pw <password> -td <xxx.csv> [-tf <time-format>]
```

## Only build server

Under the root path of incubator-iotdb:

```
> mvn clean package -pl server -am -DskipTests=true
```

After build, the IoTDB server will be at the folder "server/target/iotdb-server-{project.version}". 
 

## Only build client 

```
> mvn clean package -pl client -am -DskipTests=true
```

After build, the IoTDB client will be at the folder "client/target/iotdb-client-{project.version}".