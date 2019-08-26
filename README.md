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

# IoTDB
[![Build Status](https://www.travis-ci.org/apache/incubator-iotdb.svg?branch=master)](https://www.travis-ci.org/apache/incubator-iotdb)
[![codecov](https://codecov.io/gh/thulab/incubator-iotdb/branch/master/graph/badge.svg)](https://codecov.io/gh/thulab/incubator-iotdb)
[![GitHub release](https://img.shields.io/github/release/apache/incubator-iotdb.svg)](https://github.com/apache/incubator-iotdb/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/incubator-iotdb.svg)
![](https://img.shields.io/github/downloads/apache/incubator-iotdb/total.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macox%20%7C%20linux-yellow.svg)
![](https://img.shields.io/badge/java--language-1.8-blue.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)

# Overview

IoTDB (Internet of Things Database) is an integrated data management engine designed for time series data, which can provide users specific services for data collection, storage and analysis. Due to its light weight structure, high performance and usable features together with its intense integration with Hadoop and Spark ecology, IoTDB meets the requirements of massive dataset storage, high-speed data input and complex data analysis in the IoT industrial field.

# Main Features

IoTDB's features are as following:

1. Flexible deployment. IoTDB provides users one-click installation tool on the cloud, once-decompressed-used terminal tool and the bridge tool between cloud platform and terminal tool (Data Synchronization Tool).
2. Low cost on hardware. IoTDB can reach a high compression ratio of disk storage
3. Efficient directory structure. IoTDB supports efficient organization for complex time series data structure from intelligent networking devices, organization for time series data from devices of the same type, fuzzy searching strategy for massive and complex directory of time series data.
4. High-throughput read and write. IoTDB supports millions of low-power devices' strong connection data access, high-speed data read and write for intelligent networking devices and mixed devices mentioned above.
5. Rich query semantics. IoTDB supports time alignment for time series data across devices and sensors, computation in time series field (frequency domain transformation) and rich aggregation function support in time dimension.
6. Easy to get start. IoTDB supports SQL-Like language, JDBC standard API and import/export tools which is easy to use.
7. Intense integration with Open Source Ecosystem. IoTDB supports Hadoop, Spark, etc. analysis ecosystems and Grafana visualization tool.

For the latest information about IoTDB, please visit our [IoTDB official website](https://iotdb.apache.org/).

# Prerequisites

IoTDB requires Java (>= 1.8).
To use IoTDB, JRE should be installed. To compile IoTDB, JDK should be installed.

If you want to compile and install IoTDB from source code, JDK and Maven (>= 3.1) are required.
While Maven is not mandatory to be installed standalone, you can use the provided Maven wrapper, `./mvnw.sh` on Linux/OS X or `.\mvnw.cmd` on Windows, to facilitate development.

If you want to use Hadoop or Spark to analyze IoTDB data file (called as TsFile), you need to compile the hadoop and spark modules.

# Quick Start

This short guide will walk you through the basic process of using IoTDB. For a more-complete guide, please visit our website's [User Guide](https://iotdb.apache.org/#/Documents/0.8.0/chap1/sec1).

## Installation from source code

You can get the released source code from https://iotdb.apache.org/#/Download, or from the git repository https://github.com/apache/incubator-iotdb/tree/master

Now suppose your directory is like this:

```
> pwd
/workspace/incubator-iotdb

> ls -l
incubator-iotdb/     <-- root path
|
+- server/
|
+- jdbc/
|
+- tsfile/
|
...
|
+- pom.xml
```

Let `$IOTDB_HOME = /workspace/incubator-iotdb/server/target/iotdb-server-{project.version}`

Let `$IOTDB_CLI_HOME = /workspace/incubator-iotdb/client/target/iotdb-client-{project.version}`

Note:
* if `IOTDB_HOME` is not explicitly assigned, 
then by default `IOTDB_HOME` is the direct parent directory of `sbin/start-server.sh` on Unix/OS X 
(or that of `sbin\start-server.bat` on Windows).

* if `IOTDB_CLI_HOME` is not explicitly assigned, 
then by default `IOTDB_CLI_HOME` is the direct parent directory of `sbin/start-cli.sh` on 
Unix/OS X (or that of `sbin\start-cli.bat` on Windows).

If you are not the first time that building IoTDB, remember deleting the following files:

```
> rm -rf $IOTDB_HOME/data/
> rm -rf $IOTDB_HOME/lib/
```

Then under the root path of incubator-iotdb, you can build IoTDB using Maven:

```
> pwd
/workspace/incubator-iotdb

> mvn clean package -pl server -am -Dmaven.test.skip=true
```

If successful, you will see the the following text in the terminal:

```
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO]
[INFO] Apache IoTDB (incubating) Project Parent POM ....... SUCCESS [  6.405 s]
[INFO] TsFile ............................................. SUCCESS [ 10.435 s]
[INFO] Service-rpc ........................................ SUCCESS [  4.170 s]
[INFO] IoTDB Jdbc ......................................... SUCCESS [  3.252 s]
[INFO] IoTDB Server ....................................... SUCCESS [  8.072 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

Otherwise, you may need to check the error statements and fix the problems.

After build, the IoTDB project will be at the folder "server/target/iotdb-server-{project.version}". The folder will include the following contents:

```
server/target/iotdb-server-{project.version}  <-- root path
|
+- sbin/       <-- script files for starting and stopping the server
|
+- tools/       <-- script files of tools
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
```

> NOTE: Directories "service-rpc/target/generated-sources/thrift" and "server/target/generated-sources/antlr3" need to be added to sources roots to avoid compilation errors. 

## Configure

Before starting to use IoTDB, you need to config the configuration files first. For your convenience, we have already set the default config in the files.

In total, we provide users three kinds of configurations module: environment config module (`iotdb-env.bat`, `iotdb-env.sh`), system config module (`tsfile-format.properties`, `iotdb-engine.properties`) and log config module (`logback.xml`). All of these kinds of configuration files are put in iotdb/config folder.

For more, you are advised to check our documentation [Chapter4: Deployment and Management](https://iotdb.apache.org/#/Documents/0.8.0/chap4/sec1) in detail.

## Start

### Start Server

After that we start the server. Running the startup script: 

```
# Unix/OS X
> $IOTDB_HOME/sbin/start-server.sh

# Windows
> $IOTDB_HOME\sbin\start-server.bat
```

### Stop Server

The server can be stopped with ctrl-C or the following script:

```
# Unix/OS X
> $IOTDB_HOME/sbin/stop-server.sh

# Windows
> $IOTDB_HOME\sbin\stop-server.bat
```

### Start Client

Now let's trying to read and write some data from IoTDB using our Client. To start the client, you need to explicit the server's IP and PORT as well as the USER_NAME and PASSWORD. 

```
# You can first build cli project
> pwd
/workspace/incubator-iotdb

> mvn clean package -pl client -am -Dmaven.test.skip=true
```

After build, the IoTDB client will be at the folder "client/target/iotdb-client-{project.version}".

```
# Unix/OS X
> $IOTDB_CLI_HOME/sbin/start-cli.sh -h <IP> -p <PORT> -u <USER_NAME>

# Windows
> $IOTDB_CLI_HOME\sbin\start-cli.bat -h <IP> -p <PORT> -u <USER_NAME>
```

> NOTE: In the system, we set a default user in IoTDB named 'root'. The default password for 'root' is 'root'. You can use this default user if you are making the first try or you didn't create users by yourself.

The command line client is interactive so if everything is ready you should see the welcome logo and statements:

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
### Have a try
Now, you can use IoTDB SQL to operate IoTDB, and when you've had enough fun, you can input 'quit' or 'exit' command to leave the client. 

But lets try something slightly more interesting:

``` 
IoTDB> SET STORAGE GROUP TO root.vehicle
It costs xxxs
IoTDB> CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE
It costs xxxs
```
Till now, we have already create a table called root.vehicle and add a column called d0.s0 in the table. Let's take a look at what we have done by 'SHOW TIMESERIES' command.

``` 
IoTDB> SHOW TIMESERIES
===  Timeseries Tree  ===

{
        "root":{
                "vehicle":{
                        "d0":{
                                "s0":{
                                        "args":"{}",
                                        "StorageGroup":"root.vehicle",
                                        "DataType":"INT32",
                                        "Compressor":"UNCOMPRESSED",
                                        "Encoding":"RLE"
                                }
                        }
                }
        }
}
```
Insert time series data is the basic operation of IoTDB, you can use 'INSERT' command to finish this:

```
IoTDB> insert into root.vehicle.d0(timestamp,s0) values(1,101);
It costs xxxs
```
The data we've just inserted displays like this:

```
IoTDB> SELECT d0.s0 FROM root.vehicle
+-----------------------------+------------------+
|                         Time|root.vehicle.d0.s0|
+-----------------------------+------------------+
|1970-01-01T08:00:00.001+08:00|               101|
+-----------------------------+------------------+
Total line number = 1
It costs xxxs
```

If your session looks similar to what's above, congrats, your IoTDB is operational!

For more on what commands are supported by IoTDB SQL, see our documentation [Chapter 5: IoTDB SQL Documentation](https://iotdb.apache.org/#/Documents/0.8.0/chap5/sec1).


# Usage of import-csv.sh

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
> $IOTDB_CLI_HOME/tools/import-csv.sh -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv>

# Windows
> $IOTDB_CLI_HOME\tools\import-csv.bat -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv>
```

### Error data file

`csvInsertError.error`

# Usage of export-csv.sh

### Run export shell
```
# Unix/OS X
> $IOTDB_CLI_HOME/tools/export-csv.sh -h <ip> -p <port> -u <username> -pw <password> -td <xxx.csv> [-tf <time-format>]

# Windows
> $IOTDB_CLI_HOME\tools\export-csv.bat -h <ip> -p <port> -u <username> -pw <password> -td <xxx.csv> [-tf <time-format>]
```