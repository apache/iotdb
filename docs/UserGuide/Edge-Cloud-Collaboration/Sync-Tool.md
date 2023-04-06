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

# Edge-Cloud Collaboration

## 1.Introduction

The Sync Tool is an IoTDB suite tool that continuously uploads the timeseries data from the edge (sender) to the cloud(receiver).

On the sender side of the sync-tool, the sync module is embedded in the IoTDB engine. The receiver side of the sync-tool supports IoTDB (standalone/cluster).

You can use SQL commands to start or close a synchronization task at the sender, and you can check the status of the synchronization task at any time. At the receiving end, you can set the IP white list to specify the access IP address range of sender.

## 2.Model definition

![pipe2.png](https://alioss.timecho.com/docs/img/UserGuide/System-Tools/Sync-Tool/pipe2.png?raw=true)

Two machines A and B, which are installed with iotdb, we want to continuously synchronize the data from A to B. To better describe this process, we introduce the following concepts.

- Pipe
  - It refers to a synchronization task. In the above case, we can see that there is a data flow pipeline connecting A and B.
  - A pipe has three states, RUNNING, STOP and DROP, which respectively indicate running, pause and permanent cancellation.
- PipeSink
  - It refers to the receiving end. In the above case, pipesink is machine B. At present, the pipesink type only supports IoTDB, that is, the receiver is the IoTDB instance installed on B.
  - Pipeserver: when the type of pipesink is IoTDB, you need to open the pipeserver service of IoTDB to process the pipe data.

## 3.Precautions for Use

- The sender side of the sync-tool currently supports IoTDB version 1.0 **only if data_replication_factor is set to 1**. The receiver side supports any IoTDB version 1.0 configuration
- A normal Pipe has two states: RUNNING indicates that it is synchronizing data to the receiver, and STOP indicates that synchronization to the receiver is suspended.
- When one or more senders send data to a receiver, there should be no intersection between the respective device path sets of these senders and receivers, otherwise unexpected errors may occur.
  - e.g. When sender A includes path `root.sg.d.s`, sender B also includes the path `root.sg.d.s`, sender A deletes database `root.sg` will also delete all data of B stored in the path `root.sg.d.s` at receiver.
- The two "ends" do not support synchronization with each other.
- The Sync Tool only synchronizes insertions. If no database is created on the receiver, a database of the same level as the sender will be automatically created. Currently, deletion operation is not guaranteed to be synchronized and do not support TTL settings, trigger and other operations.
  - If TTL is set on the sender side, all unexpired data in the IoTDB and all future data writes and deletions will be synchronized to the receiver side when Pipe is started.
- When operating a synchronization task, ensure that all DataNode nodes in `SHOW DATANODES` that are in the Running state are connected, otherwise the execution will fail.

## 4.Quick Start

Execute the following SQL statements at the sender and receiver to quickly start a data synchronization task between two IoTDB. For complete SQL statements and configuration matters, please see the `parameter configuration`and `SQL` sections. For more usage examples, please refer to the `usage examples` section.

### 4.1 Receiver

- Start sender IoTDB and receiver IoTDB.

- Create a PipeSink with IoTDB type.

```
IoTDB> CREATE PIPESINK central_iotdb AS IoTDB (ip='There is your goal IP', port='There is your goal port')
```

- Establish a Pipe (before creation, ensure that receiver IoTDB has been started).

```
IoTDB> CREATE PIPE my_pipe TO central_iotDB
```

- Start this Pipe.

```
IoTDB> START PIPE my_pipe
```

- Show Pipe's status.

```
IoTDB> SHOW PIPES
```

- Stop this Pipe.

```
IoTDB> STOP PIPE my_pipe
```

- Continue this Pipe.

```
IoTDB> START PIPE my_pipe
```

- Drop this Pipe (delete all information about this pipe).

```
IoTDB> DROP PIPE my_pipe
```

## 5.Parameter Configuration

All parameters are in `$IOTDB_ HOME$/conf/iotdb-common.properties`, after all modifications are completed, execute `load configuration` and it will take effect immediately.

### 5.1 Sender

| **Parameter Name** | **max_number_of_sync_file_retry**                            |
| ------------------ | ------------------------------------------------------------ |
| Description        | The maximum number of retries when the sender fails to synchronize files to the receiver. |
| Data type          | Int : [0,2147483647]                                         |
| Default value      | 5                                                            |



### 5.2 Receiver

| **Parameter Name** | **ip_white_list**                                            |
| ------------------ | ------------------------------------------------------------ |
| Description        | Set the white list of IP addresses of the sender of the synchronization, which is expressed in the form of network segments, and multiple network segments are separated by commas. When the sender synchronizes data to the receiver, the receiver allows synchronization only when the IP address of the sender is within the network segment set in the white list. If the whitelist is empty, the receiver does not allow any sender to synchronize data. By default, the receiver rejects the synchronization request of all IP addresses except 127.0.0.1. When configuring this parameter, please ensure that all DataNode addresses on the sender are set. |
| Data type          | String                                                       |
| Default value      | 127.0.0.1/32                                                    |

## 6.SQL

### SHOW PIPESINKTYPE

- Show all PipeSink types supported by IoTDB.

```
IoTDB> SHOW PIPESINKTYPE
IoTDB>
+-----+
| type|
+-----+
|IoTDB|
+-----+
```

### CREATE PIPESINK

* Create a PipeSink with IoTDB type, where IP and port are optional parameters.

```
IoTDB> CREATE PIPESINK <PipeSinkName> AS IoTDB [(ip='127.0.0.1',port=6667);]
```

### DROP PIPESINK

- Drop the pipesink with PipeSinkName parameter.

```
IoTDB> DROP PIPESINK <PipeSinkName>
```

### SHOW PIPESINK

- Show all PipeSinks' definition, the results set has three columns, name, PipeSink’s type and PipeSink‘s attributes.

```
IoTDB> SHOW PIPESINKS
IoTDB> SHOW PIPESINK [PipeSinkName]
IoTDB> 
+-----------+-----+------------------------+
|       name| type|              attributes|
+-----------+-----+------------------------+
|my_pipesink|IoTDB|ip='127.0.0.1',port=6667|
+-----------+-----+------------------------+
```

### CREATE PIPE

- Create a pipe.

  - At present, the SELECT statement only supports `**` (i.e. data in all timeseries), the FROM statement only supports `root`, and the WHERE statement only supports the start time of the specified time. The start time can be specified in the form of yyyy-mm-dd HH:MM:SS or a timestamp.
  

```
IoTDB> CREATE PIPE my_pipe TO my_iotdb [FROM (select ** from root WHERE time>='yyyy-mm-dd HH:MM:SS' )]
```

### STOP PIPE

- Stop the Pipe with PipeName.

```
IoTDB> STOP PIPE <PipeName>
```

### START PIPE

- Continue the Pipe with PipeName.

```
IoTDB> START PIPE <PipeName>
```

### DROP PIPE

- Drop the pipe with PipeName(delete all information about this pipe).

```
IoTDB> DROP PIPE <PipeName>
```

### SHOW PIPE

> This statement can be executed on both senders and receivers.

- Show all Pipe's status.

  - `create time`: the creation time of this pipe.

  - `name`: the name of this pipe.

  - `role`: the current role of this IoTDB in pipe, there are two possible roles.
    - Sender, the current IoTDB is the synchronous sender
    - Receiver, the current IoTDB is the synchronous receiver

  - `remote`: information about the opposite end of the Pipe.
    - When role is sender, the value of this field is the PipeSink name.
    - When role is receiver, the value of this field is the sender's IP.


  - `status`: the Pipe's status.
  - `attributes`: the attributes of Pipe
    - When role is sender, the value of this field is the synchronization start time of the Pipe and whether to synchronize the delete operation.
    - When role is receiver, the value of this field is the name of the database corresponding to the synchronization connection created on this DataNode.

  - `message`: the status message of this pipe. When pipe runs normally, this column is NORMAL. When an exception occurs, messages may appear in  following two states.
    - WARN, this indicates that a data loss or other error has occurred, but the pipe will remain running.
    - ERROR, This indicates a problem where the network connection works but the data cannot be transferred, for example, the IP of the sender is not in the whitelist of the receiver or the version of the sender is not compatible with that of the receiver.
    - When the ERROR status appears, it is recommended to check the DataNode logs after STOP PIPE, check the receiver configuration or network conditions, and then START PIPE again.


```
IoTDB> SHOW PIPES
IoTDB>
+-----------------------+--------+--------+-------------+---------+------------------------------------+-------+
|            create time|   name |    role|       remote|   status|                          attributes|message|
+-----------------------+--------+--------+-------------+---------+------------------------------------+-------+
|2022-03-30T20:58:30.689|my_pipe1|  sender|  my_pipesink|     STOP|SyncDelOp=false,DataStartTimestamp=0| NORMAL|
+-----------------------+--------+--------+-------------+---------+------------------------------------+-------+ 
|2022-03-31T12:55:28.129|my_pipe2|receiver|192.168.11.11|  RUNNING|             Database='root.vehicle'| NORMAL|
+-----------------------+--------+--------+-------------+---------+------------------------------------+-------+
```

- Show the pipe status with PipeName. When the PipeName is empty，it is the same with `Show PIPES`.

```
IoTDB> SHOW PIPE [PipeName]
```

## 7. Usage Examples

### Goal

- Create a synchronize task from sender IoTDB to receiver IoTDB.
- Sender wants to synchronize the data after 2022-3-30 00:00:00.
- Sender does not want to synchronize the deletions.
- Receiver only wants to receive data from this sender(sender ip 192.168.0.1).

### Receiver

- `vi conf/iotdb-common.properties`  to config the parameters，set the IP white list to 192.168.0.1/32 to receive and only receive data from sender.

```
####################
### PIPE Server Configuration
####################
# White IP list of Sync client.
# Please use the form of IPv4 network segment to present the range of IP, for example: 192.168.0.0/16
# If there are more than one IP segment, please separate them by commas
# The default is to reject all IP to sync except 0.0.0.0
# Datatype: String
ip_white_list=192.168.0.1/32
```

### Sender

- Create PipeSink with IoTDB type, input ip address 192.168.0.1, port 6667.

```
IoTDB> CREATE PIPESINK my_iotdb AS IoTDB (IP='192.168.0.2'，PORT=6667)
```

- Create Pipe connect to my_iotdb, input the start time 2022-03-30 00:00:00 in WHERE statments. The following two SQL statements are equivalent

```
IoTDB> CREATE PIPE p TO my_iotdb FROM (select ** from root where time>='2022-03-30 00:00:00')
IoTDB> CREATE PIPE p TO my_iotdb FROM (select ** from root where time>= 1648569600000)
```

- Start the Pipe p

```
IoTDB> START PIPE p
```

- Show the status of pipe p.

```
IoTDB> SHOW PIPE p
```

### Result Verification

Execute SQL on sender.

```
CREATE DATABASE root.vehicle;
CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE;
CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN;
CREATE TIMESERIES root.vehicle.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE;
CREATE TIMESERIES root.vehicle.d1.s3 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN;
insert into root.vehicle.d0(timestamp,s0) values(now(),10);
insert into root.vehicle.d0(timestamp,s0,s1) values(now(),12,'12');
insert into root.vehicle.d0(timestamp,s1) values(now(),'14');
insert into root.vehicle.d1(timestamp,s2) values(now(),16.0);
insert into root.vehicle.d1(timestamp,s2,s3) values(now(),18.0,true);
insert into root.vehicle.d1(timestamp,s3) values(now(),false);
flush;
```

Execute SELECT statements, the same results can be found on sender and receiver.

```
IoTDB> select ** from root.vehicle
+-----------------------------+------------------+------------------+------------------+------------------+
|             Time|root.vehicle.d0.s0|root.vehicle.d0.s1|root.vehicle.d1.s3|root.vehicle.d1.s2|
+-----------------------------+------------------+------------------+------------------+------------------+
|2022-04-03T20:08:17.127+08:00|        10|       null|       null|       null|
|2022-04-03T20:08:17.358+08:00|        12|        12|       null|       null|
|2022-04-03T20:08:17.393+08:00|       null|        14|       null|       null|
|2022-04-03T20:08:17.538+08:00|       null|       null|       null|       16.0|
|2022-04-03T20:08:17.753+08:00|       null|       null|       true|       18.0|
|2022-04-03T20:08:18.263+08:00|       null|       null|       false|       null|
+-----------------------------+------------------+------------------+------------------+------------------+
Total line number = 6
It costs 0.134s
```

## 8.Q&A

- Execute `CREATE PIPESINK demo as IoTDB` get message `PIPESINK [demo] already exists in IoTDB.`

  - Cause by: Current PipeSink already exists
  - Solution: Execute `DROP PIPESINK demo` to drop PipeSink and recreate it.
- Execute `DROP PIPESINK pipesinkName` get message `Can not drop PIPESINK [demo], because PIPE [mypipe] is using it.`

  - Cause by: It is not allowed to delete PipeSink that is used by a running PIPE.
  - Solution: Execute `SHOW PIPE` on the sender side to stop using the PipeSink's PIPE.

- Execute `CREATE PIPE p to demo`  get message  `PIPE [p] is STOP, please retry after drop it.`
  - Cause by: Current Pipe already exists
  - Solution: Execute `DROP PIPE p` to drop Pipe and recreate it.
- Execute `CREATE PIPE p to demo` get message  `Fail to create PIPE [p] because Connection refused on DataNode: {id=2, internalEndPoint=TEndPoint(ip:127.0.0.1, port:10732)}.`
  - Cause by: There are some DataNodes with the status Running cannot be connected.
  - Solution: Execute `SHOW DATANODES`, and check for unreachable DataNode networks, or wait for their status to change to Unknown and re-execute the statement.
- Execute `START PIPE p`  get message  `Fail to start PIPE [p] because Connection refused on DataNode: {id=2, internalEndPoint=TEndPoint(ip:127.0.0.1, port:10732)}.`
  - Cause by: There are some DataNodes with the status Running cannot be connected.
  - Solution: Execute `SHOW DATANODES`, and check for unreachable DataNode networks, or wait for their status to change to Unknown and re-execute the statement.
- Execute `STOP PIPE p`  get message  `Fail to stop PIPE [p] because Connection refused on DataNode: {id=2, internalEndPoint=TEndPoint(ip:127.0.0.1, port:10732)}.`
  - Cause by: There are some DataNodes with the status Running cannot be connected.
  - Solution: Execute `SHOW DATANODES`, and check for unreachable DataNode networks, or wait for their status to change to Unknown and re-execute the statement.
- Execute `DROP PIPE p`  get message  `Fail to DROP_PIPE because Fail to drop PIPE [p] because Connection refused on DataNode: {id=2, internalEndPoint=TEndPoint(ip:127.0.0.1, port:10732)}. Please execute [DROP PIPE p] later to retry.`
  - Cause by: There are some DataNodes with the status Running cannot be connected. Pipe has been deleted on some nodes and the status has been set to ***DROP***.
  - Solution: Execute `SHOW DATANODES`, and check for unreachable DataNode networks, or wait for their status to change to Unknown and re-execute the statement.
- Sync.log prompts `org.apache.iotdb.commons.exception.IoTDBException: root.** already been created as database`
  - Cause by: The synchronization tool attempts to automatically create a database at the sender at the receiver. This is a normal phenomenon.
  - Solution: No intervention is required.