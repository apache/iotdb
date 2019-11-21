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
# Chapter 4: Client
## Outline
<!-- TOC -->
- Command Line Interface(CLI)
    - Running Cli/Shell
    - Cli/Shell Parameters
    - Cli/shell tool with -e parameter

<!-- /TOC -->
# Command Line Interface(CLI)
IoTDB provides Cli/shell tools for users to interact with IoTDB server in command lines. This document will show how Cli/shell tool works and what does it parameters mean.

> Note: In this document, \$IOTDB\_HOME represents the path of the IoTDB installation directory.

## Build client from source code

Under the root path of incubator-iotdb:

```
> mvn clean package -pl client -am -DskipTests
```

After build, the IoTDB client will be at the folder "client/target/iotdb-client-{project.version}".

## Running Cli/Shell

After installation, there is a default user in IoTDB: `root`, and the
default password is `root`. Users can use this username to try IoTDB Cli/Shell tool. The client startup script is the `start-client` file under the \$IOTDB\_HOME/bin folder. When starting the script, you need to specify the IP and PORT. (Make sure the IoTDB server is running properly when you use Cli/Shell tool to connect it.)

Here is an example where the server is started locally and the user has not changed the running port. The default port is
6667 </br>
If you need to connect to the remote server or changes
the port number of the server running, set the specific IP and PORT at -h and -p.</br>
You also can set your own environment variable at the front of the start script ("/sbin/start-client.sh" for linux and "/sbin/start-client.bat" for windows)

The Linux and MacOS system startup commands are as follows:

```
  Shell > ./sbin/start-client.sh -h 127.0.0.1 -p 6667 -u root -pw root
```
The Windows system startup commands are as follows:

```
  Shell > \sbin\start-client.bat -h 127.0.0.1 -p 6667 -u root -pw root
```
After using these commands, the client can be started successfully. The successful status will be as follows: 

```
 _____       _________  ______   ______
|_   _|     |  _   _  ||_   _ `.|_   _ \
  | |   .--.|_/ | | \_|  | | `. \ | |_) |
  | | / .'`\ \  | |      | |  | | |  __'.
 _| |_| \__. | _| |_    _| |_.' /_| |__) |
|_____|'.__.' |_____|  |______.'|_______/  version <version>


IoTDB> login successfully
IoTDB>
```
Enter ```quit``` or `exit` can exit Client. The client will shows `quit normally` 

## Cli/Shell Parameters

|Parameter name|Parameter type|Required| Description| Example |
|:---|:---|:---|:---|:---|
|-disableIS08601 |No parameters | No |If this parameter is set, IoTDB will print the timestamp in digital form|-disableIS08601|
|-h <`host`> |string, no quotation marks|Yes|The IP address of the IoTDB server|-h 10.129.187.21|
|-help|No parameters|No|Print help information for IoTDB|-help|
|-p <`port`>|int|Yes|The port number of the IoTDB server. IoTDB runs on port 6667 by default|-p 6667|
|-pw <`password`>|string, no quotation marks|No|The password used for IoTDB to connect to the server. If no password is entered, IoTDB will ask for password in Cli command|-pw root|
|-u <`username`>|string, no quotation marks|Yes|User name used for IoTDB to connect the server|-u root|
|-maxPRC <`maxPrintRowCount`>|int|No|Set the maximum number of rows that IoTDB returns|-maxPRC 10|
|-e <`execute`> |string|No|manipulate IoTDB in batches without entering client input mode|-e "show storage group"|

Following is a client command which connects the host with IP
10.129.187.21, port 6667, username "root", password "root", and prints the timestamp in digital form. The maximum number of lines displayed on the IoTDB command line is 10.

The Linux and MacOS system startup commands are as follows:

```
  Shell > ./sbin/start-client.sh -h 10.129.187.21 -p 6667 -u root -pw root -disableIS08601 -maxPRC 10
```
The Windows system startup commands are as follows:

```
  Shell > \sbin\start-client.bat -h 10.129.187.21 -p 6667 -u root -pw root -disableIS08601 -maxPRC 10
```
## Cli/shell tool with -e parameter

-e parameter is designed for the Cli/shell tool in the situation where you would like to manipulate IoTDB in batches through scripts. By using the -e parameter, you can operate IoTDB without entering the client's input mode. 

In order to avoid confusion between statements and other parameters, the current situation only supports the -e parameter as the last parameter.

The usage of -e parameter for Cli/shell is as follows:

```
  Shell > ./sbin/start-client.sh -h {host} -p {port} -u {user} -pw {password} -e {sql for iotdb}
```

In order to better explain the use of -e parameter, take following as an example.

Suppose you want to create a storage group root.demo to a newly launched IoTDB, create a timeseries root.demo.s1 and insert three data points into it. With -e parameter, you could write a shell like this:

```
# !/bin/bash

host=127.0.0.1
port=6667
user=root
pass=root

./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "set storage group to root.demo"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "create timeseries root.demo.s1 WITH DATATYPE=INT32, ENCODING=RLE"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(1,10)"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(2,11)"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(3,12)"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "select s1 from root.demo"
```

The print results are shown in the figure, which are consistent with the client and jdbc operations.

![img](https://issues.apache.org/jira/secure/attachment/12976042/12976042_image-2019-07-27-15-47-12-045.png)

It should be noted that the use of the -e parameter in shell scripts requires attention to the escaping of special characters. 
