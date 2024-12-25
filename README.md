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
[![Unit-Test](https://github.com/apache/iotdb/actions/workflows/unit-test.yml/badge.svg)](https://github.com/apache/iotdb/actions/workflows/unit-test.yml)
[![codecov](https://codecov.io/github/apache/iotdb/graph/badge.svg?token=ejF3UGk0Nv)](https://codecov.io/github/apache/iotdb)
[![GitHub release](https://img.shields.io/github/release/apache/iotdb.svg)](https://github.com/apache/iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb.svg)
![](https://img.shields.io/github/downloads/apache/iotdb/total.svg)
![](https://img.shields.io/badge/platform-win%20%7C%20macos%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8%20%7C%2011%20%7C%2017-blue.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.iotdb/iotdb-parent/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.iotdb")
[![Gitpod Ready-to-Code](https://img.shields.io/badge/Gitpod-Ready--to--Code-blue?logo=gitpod)](https://gitpod.io/#https://github.com/apache/iotdb)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/apacheiotdb/shared_invite/zt-qvso1nj8-7715TpySZtZqmyG5qXQwpg)

# Overview

IoTDB (Internet of Things Database) is a data management system for time series data, which provides users with specific services, including data collection, storage and analysis. Due to its lightweight structure, high performance and usable features, together with its seamless integration with the Hadoop and Spark ecosystem, IoTDB meets the requirements of massive dataset storage, high throughput data input, and complex data analysis in the industrial IoT field.

[Click for More Information](https://www.timecho.com/archives/shi-xu-shu-ju-ku-iotdb-gong-neng-xiang-jie-yu-xing-ye-ying-yong)

IoTDB depends on [TsFile](https://github.com/apache/tsfile) which is a columnar storage file format designed for time series data. The branch `iotdb` of TsFile project is used to deploy SNAPSHOT version for IoTDB project.

# Main Features

The main features of IoTDB are as follows:

1. Flexible deployment strategy. IoTDB provides users with a one-click installation tool on either the cloud platform or the terminal devices, and a data synchronization tool bridging the data on cloud platform and terminals.
2. Low cost on hardware. IoTDB can reach a high compression ratio of disk storage.
3. Efficient directory structure. IoTDB supports efficient organization for complex time series data structures from intelligent networking devices, organization for time series data from devices of the same type, and fuzzy searching strategy for massive and complex directory of time series data.
4. High-throughput read and write. IoTDB supports millions of low-power devices' strong connection data access, high-speed data read and write for intelligent networking devices and mixed devices mentioned above.
5. Rich query semantics. IoTDB supports time alignment for time series data across devices and measurements, computation in time series field (frequency domain transformation) and rich aggregation function support in time dimension.
6. Easy to get started. IoTDB supports SQL-like language, JDBC standard API and import/export tools which are easy to use.
7. Seamless integration with state-of-the-practice Open Source Ecosystem. IoTDB supports analysis ecosystems, such as Hadoop and Spark, as well as visualization tools, such as Grafana.

For the latest information about IoTDB, please visit [IoTDB official website](https://iotdb.apache.org/). If you encounter any problems or identify any bugs while using IoTDB, please report an issue in [Jira](https://issues.apache.org/jira/projects/IOTDB/issues).

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

This short guide will walk you through the basic process of using IoTDB. For a more detailed introduction, please visit our website's [User Guide](https://iotdb.apache.org/UserGuide/Master/QuickStart/QuickStart.html).

## Prerequisites

To use IoTDB, you need to have:

1. Java >= 1.8 (1.8, 11 to 17 are verified. Please make sure the environment path has been set accordingly).
2. Maven >= 3.6 (If you want to compile and install IoTDB from source code).
3. Set the max open files num as 65535 to avoid the "too many open files" error.
4. (Optional) Set the somaxconn as 65535 to avoid "connection reset" error when the system is under high load.
    ```
    # Linux
    > sudo sysctl -w net.core.somaxconn=65535
   
    # FreeBSD or Darwin
    > sudo sysctl -w kern.ipc.somaxconn=65535
    ```
### Linux

(This guide is based on an installation of Ubuntu 22.04.)

#### Git

Make sure `Git` is installed, if it's missing, simply install it via:

    sudo apt install git

#### Java

Make sure `Java` is installed, if it's missing, simply install it via:

    sudo apt install default-jdk

#### Flex

    sudo apt install flex

#### Bison

    sudo apt install bison

#### Boost

    sudo apt install libboost-all-dev

#### OpenSSL header files

Usually OpenSSL is already installed, however it's missing the header files we need to compile.
So ensure these are installed:

    sudo apt install libssl-dev

### Mac OS

#### Git

First ensure `git` works.

Usually on a new Mac, as soon as you simply type `git` in a `Terminal` window, a popup will come up and ask if you want to finish installing the Mac developer tools. 
Just say yes.
As soon as this is finished, you are free to use `git`.

#### Homebrew

Then install `Homebrew` - If this hasn't been installed yet, as we are going to be installing everything using `Homebrew`.

    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

#### Java

As soon as that's done install `Java`, if this hasn't been installed yet:

    brew install java

Depending on your version of Homebrew, it will tell you to do one of the following (depending on the type of processor in your device).

Mainly on the Intel-based models:

    sudo ln -sfn /usr/local/opt/openjdk/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk

Mainly on the ARM-based models:

    sudo ln -sfn /opt/homebrew/opt/openjdk/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk

#### CPP Prerequisites 

Building `Thrift` requires us to add two more dependencies to the picture.

This however is only needed when enabling the `with-cpp` profile:

    brew install boost
    brew install bison
    brew install openssl

### Windows

#### Chocolatey

Then install `Chocolatey` - If this hasn't been installed yet, as we are going to be installing everything using `Chocolatey`.

https://chocolatey.org/install

#### Git

    choco install git.install

#### Java

    choco install openjdk

#### Visual Studio 19 2022

    choco install visualstudio2022community
    choco install visualstudio2022buildtools
    choco install visualstudio2022-workload-nativedesktop

#### Flex / Bison

    choco install winflexbison

#### Boost

    choco install boost-msvc-14.2

#### OpenSSL

    choco install openssl

## Installation

IoTDB provides three installation methods, you can refer to the following suggestions, choose the one fits you best:

* Installation from source code. If you need to modify the code yourself, you can use this method.
* Installation from binary files. Download the binary files from the official website. This is the recommended method, in which you will get a binary released package which is out-of-the-box.
* Using Docker：The path to the dockerfile is [here](https://github.com/apache/iotdb/tree/master/docker/src/main).


Here in the Quick Start, we give a brief introduction of using source code to install IoTDB. For further information, please refer to [User Guide](https://iotdb.apache.org/UserGuide/Master/QuickStart/QuickStart.html).

## Build from source

### Prepare Thrift compiler

Skip this chapter if you are using Windows. 

As we use Thrift for our RPC module (communication and
protocol definition), we involve Thrift during the compilation, so Thrift compiler 0.13.0 (or
higher) is required to generate Thrift Java code. Thrift officially provides binary compiler for
Windows, but unfortunately, they do not provide that for Unix OSs. 

If you have permission to install new software, use `apt install` or `yum install` or `brew install`
to install the Thrift compiler. (If you already have installed the thrift compiler, skip this step.)
Then, you may add the following parameter
when running Maven: `-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<YOUR LOCAL THRIFT BINARY FILE>`.

If not, then you have to compile the thrift compiler, and it requires you install a boost library first.
Therefore, we compiled a Unix compiler ourselves and put it onto GitHub, and with the help of a
maven plugin, it will be downloaded automatically during compilation. 
This compiler works fine with gcc8 or later, Ubuntu MacOS, and CentOS, but previous versions 
and other OSs are not guaranteed.

If you can not download the thrift compiler automatically because of a network problem, you can download 
it by yourself, and then either:
rename your thrift file to `{project_root}\thrift\target\tools\thrift_0.12.0_0.13.0_linux.exe`;
or, add Maven commands:
`-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<YOUR LOCAL THRIFT BINARY FILE>`.

### Compile IoTDB

You can download the source code from:

```
git clone https://github.com/apache/iotdb.git
```

The default dev branch is the master branch, if you want to use a released version x.x.x:

```
git checkout vx.x.x
```

Or checkout to the branch of a big version, e.g., the branch of 1.0 is rel/1.0.

```
git checkout rel/x.x
```

### Build IoTDB from source

Under the root path of iotdb:

```
> mvn clean package -pl distribution -am -DskipTests
```

After being built, the IoTDB distribution is located at the folder: "distribution/target".


### Only build cli

Under the iotdb/iotdb-client path:

```
> mvn clean package -pl cli -am -DskipTests
```

After being built, the IoTDB cli is located at the folder "cli/target".

### Build Others

Use `-P with-cpp` for compiling the cpp client. (For more details, read client-cpp's Readme file.)

**NOTE: Directories "`thrift/target/generated-sources/thrift`", "`thrift-sync/target/generated-sources/thrift`",
"`thrift-cluster/target/generated-sources/thrift`", "`thrift-influxdb/target/generated-sources/thrift`" 
and "`antlr/target/generated-sources/antlr4`" need to be added to sources roots to avoid compilation errors in the IDE.**

**In IDEA, you just need to right click on the root project name and choose "`Maven->Reload Project`" after 
you run `mvn package` successfully.**

### Configurations

Configuration files are under the "conf" folder.

  * environment config module (`datanode-env.bat`, `datanode-env.sh`),
  * system config module (`iotdb-datanode.properties`)
  * log config module (`logback.xml`).

For more information, please see [Config Manual](https://iotdb.apache.org/UserGuide/Master/Reference/DataNode-Config-Manual.html).

## Start

You can go through the following steps to test the installation. If there is no error returned after execution, the installation is completed.

### Start IoTDB

Users can start 1C1D IoTDB by the start-standalone script under the sbin folder.

```
# Unix/OS X
> sbin/start-standalone.sh

# Windows
> sbin\start-standalone.bat
```

### Use IoTDB

#### Use Cli

IoTDB offers different ways to interact with server, here we introduce the basic steps of using Cli tool to insert and query data.

After installing IoTDB, there is a default user 'root', its default password is also 'root'. Users can use this
default user to login Cli to use IoTDB. The start-up script of Cli is the start-cli script in the folder sbin. When executing the script, user should assign
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

The data in IoTDB is organized as timeseries. Each timeseries includes multiple data–time pairs, and is owned by a database. Before defining a timeseries, we should define a database using CREATE DATABASE first, and here is an example:

```
IoTDB> CREATE DATABASE root.ln
```

We can also use SHOW DATABASES to check the database being created:

```
IoTDB> SHOW DATABASES
+-------------+
|     Database|
+-------------+
|      root.ln|
+-------------+
Total line number = 1
```

After the database is set, we can use CREATE TIMESERIES to create a new timeseries. When creating a timeseries, we should define its data type and the encoding scheme. Here we create two timeseries:

```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

In order to query the specific timeseries, we can use SHOW TIMESERIES <Path>. <Path> represent the location of the timeseries. The default value is "null", which queries all the timeseries in the system (the same as using "SHOW TIMESERIES root"). Here are some examples:

1. Querying all timeseries in the system:

```
IoTDB> SHOW TIMESERIES
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                   Timeseries|Alias|Database|DataType|Encoding|Compression|Tags|Attributes|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.temperature| null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|
|     root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 2
```

2. Querying a specific timeseries (root.ln.wf01.wt01.status):

```
IoTDB> SHOW TIMESERIES root.ln.wf01.wt01.status
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|              timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 1
```

Inserting timeseries data is a basic operation of IoTDB, you can use the ‘INSERT’ command to finish this. Before insertion, you should assign the timestamp and the suffix path name:

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

The data that you have just inserted will be displayed as follows:

```
IoTDB> SELECT status FROM root.ln.wf01.wt01
+------------------------+------------------------+
|                    Time|root.ln.wf01.wt01.status|
+------------------------+------------------------+
|1970-01-01T00:00:00.100Z|                    true|
|1970-01-01T00:00:00.200Z|                   false|
+------------------------+------------------------+
Total line number = 2
```

You can also query several timeseries data using one SQL statement:

```
IoTDB> SELECT * FROM root.ln.wf01.wt01
+------------------------+-----------------------------+------------------------+
|                    Time|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+------------------------+-----------------------------+------------------------+
|1970-01-01T00:00:00.100Z|                         null|                    true|
|1970-01-01T00:00:00.200Z|                        20.71|                   false|
+------------------------+-----------------------------+------------------------+
Total line number = 2
```

To change the time zone in Cli, you can use the following SQL:

```
IoTDB> SET time_zone=+08:00
Time zone has set to +08:00
IoTDB> SHOW time_zone
Current time zone: Asia/Shanghai
```

Add then the query result will show using the new time zone.

```
IoTDB> SELECT * FROM root.ln.wf01.wt01
+-----------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+-----------------------------+-----------------------------+------------------------+
|1970-01-01T08:00:00.100+08:00|                         null|                    true|
|1970-01-01T08:00:00.200+08:00|                        20.71|                   false|
+-----------------------------+-----------------------------+------------------------+
Total line number = 2
```

The commands to exit the Cli are:

```
IoTDB> quit
or
IoTDB> exit
```

For more information about the commands supported by IoTDB SQL, please see [User Guide](https://iotdb.apache.org/UserGuide/Master/QuickStart/QuickStart.html).

### Stop IoTDB

The server can be stopped with "ctrl-C" or the following script:

```
# Unix/OS X
> sbin/stop-standalone.sh

# Windows
> sbin\stop-standalone.bat
```

# The use of CSV Import and Export Tool

see [The use of CSV Import and Export Tool](https://iotdb.apache.org/UserGuide/latest/Tools-System/Import-Export-Tool.html)

# Frequent Questions for Compiling
see [Frequent Questions when Compiling the Source Code](https://iotdb.apache.org/Development/ContributeGuide.html#_Frequent-Questions-when-Compiling-the-Source-Code)

# Contact Us

### QQ Group

* Apache IoTDB User Group: 659990460

### Wechat Group

* Add friend: `tietouqiao` or `liutaohua001`, and then we'll invite you to the group.

### Slack

* [Slack channel](https://join.slack.com/t/apacheiotdb/shared_invite/zt-qvso1nj8-7715TpySZtZqmyG5qXQwpg)

see [Join the community](https://github.com/apache/iotdb/issues/1995) for more!
