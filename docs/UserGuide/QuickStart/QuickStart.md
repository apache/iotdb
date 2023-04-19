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

# Quick Start

This short guide will walk you through the basic process of using IoTDB. For a more-complete guide, please visit our website's [User Guide](../IoTDB-Introduction/What-is-IoTDB.md).

## Prerequisites

To use IoTDB, you need to have:

1. Java >= 1.8 (Please make sure the environment path has been set)
2. Set the max open files num as 65535 to avoid "too many open files" problem.

## Installation

IoTDB provides you three installation methods, you can refer to the following suggestions, choose one of them:

* Installation from source code. If you need to modify the code yourself, you can use this method.
* Installation from binary files. Download the binary files from the official website. This is the recommended method, in which you will get a binary released package which is out-of-the-box.
* Using Docker：The path to the dockerfile is [github](https://github.com/apache/iotdb/blob/master/docker/src/main)


## Download

You can download the binary file from:
[Download Page](https://iotdb.apache.org/Download/)

## Configurations

Configuration files are under "conf" folder

  * environment config module (`datanode-env.bat`, `datanode-env.sh`), 
  * system config module (`iotdb-datanode.properties`)
  * log config module (`logback.xml`). 

For more, see [Config](../Reference/DataNode-Config-Manual.md) in detail.

## Start

You can go through the following step to test the installation, if there is no error after execution, the installation is completed. 

### Start IoTDB
IoTDB is a database based on distributed system. To launch IoTDB, you can first start standalone mode (i.e. 1 ConfigNode and 1 DataNode) to check.

Users can start IoTDB standalone mode by the start-standalone script under the sbin folder.

```
# Unix/OS X
> bash sbin/start-standalone.sh
```
```
# Windows
> sbin\start-standalone.bat
```

Note: Currently, To run standalone mode, you need to ensure that all addresses are set to 127.0.0.1, and replication factors set to 1, which is by now the default setting.
Besides, it's recommended to use SimpleConsensus in this mode, since it brings additional efficiency.
### Use Cli

IoTDB offers different ways to interact with server, here we introduce basic steps of using Cli tool to insert and query data.

After installing IoTDB, there is a default user 'root', its default password is also 'root'. Users can use this
default user to login Cli to use IoTDB. The startup script of Cli is the start-cli script in the folder sbin. When executing the script, user should assign
IP, PORT, USER_NAME and PASSWORD. The default parameters are "-h 127.0.0.1 -p 6667 -u root -pw -root".

Here is the command for starting the Cli:

```
# Unix/OS X
> bash sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root

# Windows
> sbin\start-cli.bat -h 127.0.0.1 -p 6667 -u root -pw root
```

The command line client is interactive so if everything is ready you should see the welcome logo and statements:

```
 _____       _________  ______   ______
|_   _|     |  _   _  ||_   _ `.|_   _ \
  | |   .--.|_/ | | \_|  | | `. \ | |_) |
  | | / .'`\ \  | |      | |  | | |  __'.
 _| |_| \__. | _| |_    _| |_.' /_| |__) |
|_____|'.__.' |_____|  |______.'|_______/  version x.x.x


Successfully login at 127.0.0.1:6667
IoTDB>
```

### Basic commands for IoTDB

Now, let us introduce the way of creating timeseries, inserting data and querying data. 

The data in IoTDB is organized as timeseries, in each timeseries there are some data-time pairs, and every timeseries is owned by a database. Before defining a timeseries, we should define a database using create DATABASE, and here is an example: 

``` 
IoTDB> create database root.ln
```

We can also use SHOW DATABASES to check created databases:

```
IoTDB> SHOW DATABASES
+-----------------------------------+
|                           Database|
+-----------------------------------+
|                            root.ln|
+-----------------------------------+
Database number = 1
```

After the database is set, we can use CREATE TIMESERIES to create new timeseries. When we create a timeseries, we should define its data type and the encoding scheme. We create two timeseries as follow:

```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

To query the specific timeseries, use SHOW TIMESERIES \<Path\>. \<Path\> represents the path of the timeseries. Its default value is null, which means querying all the timeseries in the system(the same as using "SHOW TIMESERIES root"). Here are the examples:

1. Query all timeseries in the system:

```
IoTDB> SHOW TIMESERIES
+-------------------------------+---------------+--------+--------+
|                     Timeseries|       Database|DataType|Encoding|
+-------------------------------+---------------+--------+--------+
|       root.ln.wf01.wt01.status|        root.ln| BOOLEAN|   PLAIN|
|  root.ln.wf01.wt01.temperature|        root.ln|   FLOAT|     RLE|
+-------------------------------+---------------+--------+--------+
Total timeseries number = 2
```

2. Query a specific timeseries(root.ln.wf01.wt01.status):

```
IoTDB> SHOW TIMESERIES root.ln.wf01.wt01.status
+------------------------------+--------------+--------+--------+
|                    Timeseries|      Database|DataType|Encoding|
+------------------------------+--------------+--------+--------+
|      root.ln.wf01.wt01.status|       root.ln| BOOLEAN|   PLAIN|
+------------------------------+--------------+--------+--------+
Total timeseries number = 1
```

Insert timeseries data is the basic operation of IoTDB, you can use ‘INSERT’ command to finish this. Before insert you should assign the timestamp and the suffix path name:

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

The data we’ve just inserted displays like this:

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

We can also query several timeseries data at once like this:

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

For more on what commands are supported by IoTDB SQL, see [SQL Reference](../Reference/SQL-Reference.md).

### Stop IoTDB

The server can be stopped with ctrl-C or the following script:

```
# Unix/OS X
> bash sbin/stop-standalone.sh

# Windows
> sbin\stop-standalone.bat
```
Note: In Linux, please add the "sudo" as far as possible, or else the stopping process may fail.
More explanations are in Cluster/Cluster-setup.md.

### Administration management

There is a default user in IoTDB after the initial installation: root, and the default password is root. This user is an administrator user, who cannot be deleted and has all the privileges. Neither can new privileges be granted to the root user nor can privileges owned by the root user be deleted.

You can alter the password of root using the following command：
```
ALTER USER <username> SET PASSWORD <password>;
Example: IoTDB > ALTER USER root SET PASSWORD 'newpwd';
```

More about administration management：[Administration Management](https://iotdb.apache.org/UserGuide/V1.0.x/Administration-Management/Administration.html)


## Basic configuration

The configuration files is in the `conf` folder, includes:

* environment configuration (`datanode-env.bat`, `datanode-env.sh`),
* system configuration (`iotdb-datanode.properties`)
* log configuration (`logback.xml`).
