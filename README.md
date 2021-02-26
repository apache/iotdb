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
[English](./README.md) | [中文](./README_ZH.md)

# IoTDB
[![Build Status](https://www.travis-ci.org/apache/iotdb.svg?branch=master)](https://www.travis-ci.org/apache/iotdb)
[![coveralls](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)](https://coveralls.io/repos/github/apache/iotdb/badge.svg?branch=master)
[![GitHub release](https://img.shields.io/github/release/apache/iotdb.svg)](https://github.com/apache/iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb.svg)
![](https://img.shields.io/github/downloads/apache/iotdb/total.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macox%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/apache/iotdb.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/apache/iotdb/context:java)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.iotdb/iotdb-parent/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.iotdb")
[![Gitpod Ready-to-Code](https://img.shields.io/badge/Gitpod-Ready--to--Code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/apache/iotdb) 

# Overview

IoTDB (Internet of Things Database) is a data management system for time series data, which can provide users specific services, such as, data collection, storage and analysis. Due to its light weight structure, high performance and usable features together with its seamless integration with the Hadoop and Spark ecology, IoTDB meets the requirements of massive dataset storage, high throughput data input, and complex data analysis in the industrial IoT field.

# Main Features

Main features of IoTDB are as follows:

1. Flexible deployment strategy. IoTDB provides users a one-click installation tool on either the cloud platform or the terminal devices, and a data synchronization tool bridging the data on cloud platform and terminals.
2. Low cost on hardware. IoTDB can reach a high compression ratio of disk storage.
3. Efficient directory structure. IoTDB supports efficient organization for complex time series data structure from intelligent networking devices, organization for time series data from devices of the same type, fuzzy searching strategy for massive and complex directory of time series data.
4. High-throughput read and write. IoTDB supports millions of low-power devices' strong connection data access, high-speed data read and write for intelligent networking devices and mixed devices mentioned above.
5. Rich query semantics. IoTDB supports time alignment for time series data across devices and measurements, computation in time series field (frequency domain transformation) and rich aggregation function support in time dimension.
6. Easy to get started. IoTDB supports SQL-Like language, JDBC standard API and import/export tools which is easy to use.
7. Seamless integration with state-of-the-practice Open Source Ecosystem. IoTDB supports analysis ecosystems such as, Hadoop, Spark, and visualization tool, such as, Grafana.

For the latest information about IoTDB, please visit [IoTDB official website](https://iotdb.apache.org/). If you encounter any problems or identify any bugs while using IoTDB, please report an issue in [jira](https://issues.apache.org/jira/projects/IOTDB/issues).

<!-- TOC -->

## Outline

- [IoTDB](#iotdb)
- [Overview](#overview)
- [Main Features](#main-features)
  - [Outline](#outline)
- [Quick Start](#quick-start)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
    - [Build from source](#build-from-source)
    - [Configurations](#configurations)
  - [Start](#start)
    - [Start IoTDB](#start-iotdb)
    - [Use IoTDB](#use-iotdb)
      - [Use Cli](#use-cli)
      - [Basic commands for IoTDB](#basic-commands-for-iotdb)
    - [Stop IoTDB](#stop-iotdb)
  - [Only build server](#only-build-server)
  - [Only build cli](#only-build-cli)
  - [Usage of CSV Import and Export Tool](#usage-of-csv-import-and-export-tool)

<!-- /TOC -->

# Quick Start

This short guide will walk you through the basic process of using IoTDB. For a more detailed introduction, please visit our website's [User Guide](https://iotdb.apache.org/UserGuide/Master/Get%20Started/QuickStart.html).

## Prerequisites

To use IoTDB, you need to have:

1. Java >= 1.8 (1.8, 11, and 13 are verified. Please make sure the environment path has been set accordingly).
2. Maven >= 3.6 (If you want to compile and install IoTDB from source code).
3. Set the max open files num as 65535 to avoid "too many open files" error.

## Installation

IoTDB provides three installation methods, you can refer to the following suggestions, choose the one fits you best:

* Installation from source code. If you need to modify the code yourself, you can use this method.
* Installation from binary files. Download the binary files from the official website. This is the recommended method, in which you will get a binary released package which is out-of-the-box.
* Using Docker：The path to the dockerfile is https://github.com/apache/iotdb/tree/master/docker/src/main


Here in the Quick Start, we give a brief introduction of using source code to install IoTDB. For further information, please refer to Chapter 3 of the User Guide.

## Build from source

Skip this paragraph if you are using Windows. As we use Thrift for our RPC module (communication and
protocol definition), we involve Thrift during the compilation, so Thrift compiler 0.13.0 (or
higher) is required to generate Thrift Java code. Thrift officially provides binary compiler for
Windows, but unfortunately, they do not provide that for Unix OSs. However, we compiled a Unix
compiler ourselves and put it onto GitHub, and with the help of a maven plugin, it will be
downloaded automatically during compilation. This compiler works fine with gcc8 or later, Ubuntu
MacOS, and CentOS, but previous versions and other OSs are not guaranteed. Should you find your gcc
version or OS does not support the precompiled compiler, please upgrade your gcc version or follow the
Thrift official instructions to compile the compiler yourself and rename it into `{project_root}\thrift\target\tools\thrift_0.12.0_0.13.0_linux.exe`.
If you have already installed a compatible Thrift compiler, you may add the following parameter
when running Maven: `-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<YOUR LOCAL THRIFT BINARY FILE>`.
If you want to download the Thrift compiler from another position, you may add the following
parameter: `-Dthrift.download-url=<THE REMOTE URL FOR DOWNLOADING> -Dthrift.exec.absolute.path=<THE DOWNLOADED BINARY FILE NAME>`. Or you may directly modify our root pom if you are skilled enough.
Here is the Thrift official site: https://thrift.apache.org/

You can download the source code from:

```
git clone https://github.com/apache/iotdb.git
```

The default master branch is the dev branch, If you want to use a released version x.x.x:

```
git checkout release/x.x.x
```


Under the root path of iotdb:

```
> mvn clean package -DskipTests
```

Using `-P compile-cpp` for compiling cpp client (For more details, read client-cpp's Readme file.)

Then the binary version (including both server and cli) can be found at **distribution/target/apache-iotdb-{project.version}-bin.zip**

> NOTE: Directories "thrift/target/generated-sources/thrift" and "antlr/target/generated-sources/antlr4" need to be added to sources roots to avoid compilation errors in the IDE.


### Configurations

configuration files are under "conf" folder

  * environment config module (`iotdb-env.bat`, `iotdb-env.sh`),
  * system config module (`iotdb-engine.properties`)
  * log config module (`logback.xml`).

For more information, please see [Chapter3: Server](http://iotdb.apache.org/UserGuide/Master/Server/Config%20Manual.html).

## Start

You can go through the following steps to test the installation, if there is no error returned after execution, the installation is completed.

### Start IoTDB

Users can start IoTDB by the start-server script under the sbin folder.

```
# Unix/OS X
> nohup sbin/start-server.sh >/dev/null 2>&1 &
or
> nohup sbin/start-server.sh -c <conf_path> -rpc_port <rpc_port> >/dev/null 2>&1 &

# Windows
> sbin\start-server.bat -c <conf_path> -rpc_port <rpc_port>
```

- "-c" and "-rpc_port" are optional.
- option "-c" specifies the system configuration file directory.
- option "-rpc_port" specifies the rpc port.
- if both option specified, the *rpc_port* will overrides the rpc_port in *conf_path*.


### Use IoTDB

#### Use Cli

IoTDB offers different ways to interact with server, here we introduce the basic steps of using Cli tool to insert and query data.

After installing IoTDB, there is a default user 'root', its default password is also 'root'. Users can use this
default user to login Cli to use IoTDB. The startup script of Cli is the start-cli script in the folder sbin. When executing the script, user should assign
IP, PORT, USER_NAME and PASSWORD. The default parameters are "-h 127.0.0.1 -p 6667 -u root -pw -root".

Here is the command for starting the Cli:

```
# Unix/OS X
> sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root

# Windows
> sbin\start-cli.bat -h 127.0.0.1 -p 6667 -u root -pw root
```

The command line cli is interactive, so you should see the welcome logo and statements if everything is ready:

```
 _____       _________  ______   ______
|_   _|     |  _   _  ||_   _ `.|_   _ \
  | |   .--.|_/ | | \_|  | | `. \ | |_) |
  | | / .'`\ \  | |      | |  | | |  __'.
 _| |_| \__. | _| |_    _| |_.' /_| |__) |
|_____|'.__.' |_____|  |______.'|_______/  version x.x.x


IoTDB> login successfully
IoTDB>
```

#### Basic commands for IoTDB

Now, let us introduce the way of creating timeseries, inserting data and querying data.

The data in IoTDB is organized as timeseries. Each timeseries includes multiple data-time pairs, and is owned by a storage group. Before defining a timeseries, we should define a storage group using SET STORAGE GROUP first, and here is an example:

```
IoTDB> SET STORAGE GROUP TO root.ln
```

We can also use SHOW STORAGE GROUP to check the storage group being created:

```
IoTDB> SHOW STORAGE GROUP
+-----------------------------------+
|                      Storage Group|
+-----------------------------------+
|                            root.ln|
+-----------------------------------+
storage group number = 1
```

After the storage group is set, we can use CREATE TIMESERIES to create a new timeseries. When creating a timeseries, we should define its data type and the encoding scheme. Here We create two timeseries:

```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

In order to query the specific timeseries, we can use SHOW TIMESERIES <Path>. <Path> represent the location of the timeseries. The default value is "null", which queries all the timeseries in the system(the same as using "SHOW TIMESERIES root"). Here are some examples:

1. Querying all timeseries in the system:

```
IoTDB> SHOW TIMESERIES
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
|                   timeseries  | alias|storage group|dataType|encoding|compression|tags|attributes|
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
|       root.ln.wf01.wt01.status|  null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
|  root.ln.wf01.wt01.temperature|  null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
Total timeseries number = 2
```

2. Querying a specific timeseries(root.ln.wf01.wt01.status):

```
IoTDB> SHOW TIMESERIES root.ln.wf01.wt01.status
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
|                   timeseries  | alias|storage group|dataType|encoding|compression|tags|attributes|
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
|       root.ln.wf01.wt01.status|  null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-------------------------------+------+-------------+--------+--------+-----------+----+----------+
Total timeseries number = 1
```

Insert timeseries data is a basic operation of IoTDB, you can use ‘INSERT’ command to finish this. Before insertion, you should assign the timestamp and the suffix path name:

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

The data that you have just inserted will display as follows:

```
IoTDB> SELECT status FROM root.ln.wf01.wt01
+-----------------------+------------------------+
|                   Time|root.ln.wf01.wt01.status|
+-----------------------+------------------------+
|1970-01-01T08:00:00.100|                    true|
|1970-01-01T08:00:00.200|                   false|
+-----------------------+------------------------+
Total line number = 2
```

You can also query several timeseries data using one SQL statement:

```
IoTDB> SELECT * FROM root.ln.wf01.wt01
+-----------------------+--------------------------+-----------------------------+
|                   Time|  root.ln.wf01.wt01.status|root.ln.wf01.wt01.temperature|
+-----------------------+--------------------------+-----------------------------+
|1970-01-01T08:00:00.100|                      true|                         null|
|1970-01-01T08:00:00.200|                     false|                        20.71|
+-----------------------+--------------------------+-----------------------------+
Total line number = 2
```

The commands to exit the Cli are:

```
IoTDB> quit
or
IoTDB> exit
```

For more information about the commands supported by IoTDB SQL, please see [SQL Reference](http://iotdb.apache.org/UserGuide/Master/Operation%20Manual/SQL%20Reference.html).

### Stop IoTDB

The server can be stopped with "ctrl-C" or the following script:

```
# Unix/OS X
> sbin/stop-server.sh

# Windows
> sbin\stop-server.bat
```

## Only build server

Under the root path of iotdb:

```
> mvn clean package -pl server -am -DskipTests
```

After being built, the IoTDB server is located at the folder: "server/target/iotdb-server-{project.version}".


## Only build cli

Under the root path of iotdb:

```
> mvn clean package -pl cli -am -DskipTests
```

After being built, the IoTDB cli is located at the folder "cli/target/iotdb-cli-{project.version}".

# Usage of CSV Import and Export Tool

see [Usage of CSV Import and Export Tool](https://iotdb.apache.org/UserGuide/Master/System%20Tools/CSV%20Tool.html)

# Frequent Questions for Compiling
see [Frequent Questions when Compiling the Source Code](https://iotdb.apache.org/Development/ContributeGuide.html#_Frequent-Questions-when-Compiling-the-Source-Code)

# Contact Us
### QQ Group

* Apache IoTDB User Group: 659990460

### Wechat Group

* Add friend: tietouqiao, and then we'll invite you to the group.

see [Join the community](https://github.com/apache/iotdb/issues/1995) for more!
