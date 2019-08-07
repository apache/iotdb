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
        - Start Client
        - Have a try
        - Stop Server

<!-- /TOC -->
# Quick Start

This short guide will walk you through the basic process of using IoTDB. For a more-complete guide, please visit our website’s documents.

## Prerequisites

To use IoTDB, you need to have:

1. Java >= 1.8 (Please make sure the environment path has been set)
2. Maven >= 3.0 (If you want to compile and install IoTDB from source code)

## Installation

IoTDB provides you two installation methods, you can refer to the following suggestions, choose one of them:

* Installation from source code. If you need to modify the code yourself, you can use this method.
* Installation from binary files. Download the binary files from the official website. This is the recommended method, in which you will get a binary released package which is out-of-the-box.(Comming Soon...)

Here in the Quick Start, we give a brief introduction of using source code to install IoTDB. For further information, please refer to Chapter 4 of the User Guide.

### Installation from source code

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
+- client/
|
...
|
+- pom.xml
```

Let `$IOTDB_HOME = /workspace/incubator-iotdb/server/target/iotdb-server`

Let `$IOTDB_CLI_HOME = /workspace/incubator-iotdb/client/target/iotdb-client`

Note:
* if `IOTDB_HOME` is not explicitly assigned, 
then by default `IOTDB_HOME` is the direct parent directory of `sbin/start-server.sh` on Unix/OS X 
(or that of `sbin\start-server.bat` on Windows).

* if `IOTDB_CLI_HOME` is not explicitly assigned, 
then by default `IOTDB_CLI_HOME` is the direct parent directory of `sbin/start-client.sh` on 
Unix/OS X (or that of `sbin\start-client.bat` on Windows).

If you are not the first time that building IoTDB, remember deleting the following files:

```
> rm -rf $IOTDB_HOME/data/
> rm -rf $IOTDB_HOME/lib/
```

Then under the root path of incubator-iotdb, you can build IoTDB using Maven:

```
> pwd
/workspace/incubator-iotdb

# Unix/OS X
> mvn clean package -pl server -am -Dmaven.test.skip=true

# Windows
> mvn clean package -pl server -am '-Dmaven.test.skip=true'
```

Note: If you are a Windows user, you should use quoting `'-Dmaven.test.skip=true'` in the following commands.

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

After building, the IoTDB project will be at the subfolder named iotdb. The folder will include the following contents:

```
$IOTDB_HOME/
|
+- sbin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
```

## Configure

Before starting to use IoTDB, you need to config the configuration files first. For your convenience, we have already set the default config in the files.

In total, we provide users three kinds of configurations module: 

* environment config module (`iotdb-env.sh`(Linux or OSX), `iotdb-env.bat`(Windows))
* system config module (`tsfile-format.properties`, `iotdb-engine.properties`)
* log config module (`logback.xml`)

The configuration files of the three configuration items are located in the IoTDB installation directory: `$IOTDB_HOME/conf` folder. For more, you are advised to check Chapter 4 of the User Guide to give you the details.

## Start

### Start Server

After that we start the server. Running the startup script: 

```
# Unix/OS X
> $IOTDB_HOME/sbin/start-server.sh

# Windows
> $IOTDB_HOME\sbin\start-server.bat
```

### Start Client

Now let's trying to read and write some data from IoTDB using our Client. To start the client, you need to explicit the server's IP and PORT as well as the USER_NAME and PASSWORD. 

```
# You can first build cli project
> pwd
/workspace/incubator-iotdb

> mvn clean package -pl client -am -Dmaven.test.skip=true

# Unix/OS X
> $IOTDB_CLI_HOME/sbin/start-client.sh -h <IP> -p <PORT> -u <USER_NAME>

# Windows
> $IOTDB_CLI_HOME\sbin\start-client.bat -h <IP> -p <PORT> -u <USER_NAME>
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

Now, you can use IoTDB SQL to operate IoTDB, and when you’ve had enough fun, you can input 'quit' or 'exit' command to leave the client. 

But lets try something slightly more interesting:

``` 
IoTDB> SET STORAGE GROUP TO root.ln
execute successfully.
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
execute successfully.
```
Till now, we have already create a table called root.vehicle and add a column called d0.s0 in the table. Let's take a look at what we have done by 'SHOW TIMESERIES' command.

``` 
IoTDB> SHOW TIMESERIES
===  Timeseries Tree  ===

{
	"root":{
		"ln":{
			"wf01":{
				"wt01":{
					"status":{
						"args":"{}",
						"StorageGroup":"root.ln",
						"DataType":"BOOLEAN",
						"Compressor":"UNCOMPRESSED",
						"Encoding":"PLAIN"
					}
				}
			}
		}
	}
}
```

For a further try, create a timeseries again and use SHOW TIMESERIES to check result.

```
IoTDB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
IoTDB> SHOW TIMESERIES
===  Timeseries Tree  ===

{
	"root":{
		"ln":{
			"wf01":{
				"wt01":{
					"temperature":{
						"args":"{}",
						"StorageGroup":"root.ln",
						"DataType":"FLOAT",
						"Compressor":"UNCOMPRESSED",
						"Encoding":"RLE"
					},
					"status":{
						"args":"{}",
						"StorageGroup":"root.ln",
						"DataType":"BOOLEAN",
						"Compressor":"UNCOMPRESSED",
						"Encoding":"PLAIN"
					}
				}
			}
		}
	}
}
```
Now, for your conveniect, SHOW TIMESERIES clause also supports extention syntax, the pattern is (for further details, check Chapter 5 of the User Guide):

```
SHOW TIMESERIES <PATH>
```

Here is the example:

```
IoTDB> SHOW TIMESERIES root.ln.wf01.wt01
+------------------------------+--------------+--------+--------+
|                    Timeseries| Storage Group|DataType|Encoding|
+------------------------------+--------------+--------+--------+
|      root.ln.wf01.wt01.status|       root.ln| BOOLEAN|   PLAIN|
| root.ln.wf01.wt01.temperature|       root.ln|   FLOAT|     RLE|
+------------------------------+--------------+--------+--------+
Total timeseries number = 2
Execute successfully.
```
We can also use SHOW STORAGE GROUP to check created storage group:

```
IoTDB> show storage group
+-----------------------------------+
|                      Storage Group|
+-----------------------------------+
|                            root.ln|
+-----------------------------------+
Total storage group number = 1
Execute successfully.
It costs 0.006s
```


Insert timeseries data is the basic operation of IoTDB, you can use 'INSERT' command to finish this:

```
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
execute successfully.
IoTDB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71) execute successfully.
```
The data we've just inserted displays like this:

```
IoTDB> SELECT status FROM root.ln.wf01.wt01
+-----------------------+------------------------+
|                   Time|root.ln.wf01.wt01.status|
+-----------------------+------------------------+
|1970-01-01T08:00:00.100|                    true|
|1970-01-01T08:00:00.200|                   false|
+-----------------------+------------------------+
record number = 1
execute successfully.
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
```

If your session looks similar to what’s above, congrats, your IoTDB is operational!

For more on what commands are supported by IoTDB SQL, see Chapter 5 of the User Guide. It will give you help.

### Stop Server

The server can be stopped with ctrl-C or the following script:

```
# Unix/ OS X
> $IOTDB_HOME/bin/stop-server.sh

# Windows
> $IOTDB_HOME\bin\stop-server.bat
```

