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

On the sender side of the sync, the sync module is embedded in the IoTDB engine. You should start an IoTDB before using the Sync tool.

You can use SQL commands to start or close a synchronization task at the sender, and you can check the status of the synchronization task at any time. At the receiving end, you can set the IP white list to specify the access IP address range of sender.

## 2.Model definition

![pipe2.png](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/System%20Tools/Sync-Tool/pipe2.png?raw=true)

Two machines A and B, which are installed with iotdb, we want to continuously synchronize the data from A to B. To better describe this process, we introduce the following concepts.

- Pipe
  - It refers to a synchronization task. In the above case, we can see that there is a data flow pipeline connecting A and B.
  - A pipe has three states, RUNNING, STOP and DROP, which respectively indicate running, pause and permanent cancellation.
- PipeSink
  - It refers to the receiving end. In the above case, pipesink is machine B. At present, the pipesink type only supports IoTDB, that is, the receiver is the IoTDB instance installed on B.
  - Pipeserver: when the type of pipesink is IoTDB, you need to open the pipeserver service of IoTDB to process the pipe data.

## 3.Precautions for Use

- The Sync Tool only supports for many-to-one, that is, one sender should connect to exactly one receiver. One receiver can receive data from more senders.
- The sender can only have one pipe in non drop status. If you want to create a new pipe, please drop the current pipe.
- When one or more senders send data to a receiver, there should be no intersection between the respective device path sets of these senders and receivers, otherwise unexpected errors may occur.
  - e.g. When sender A includes path `root.sg.d.s`, sender B also includes the path `root.sg.d.s`, sender A deletes storage group `root.sg` will also delete all data of B stored in the path `root.sg.d.s` at receiver.
- Synchronization between the two machines is not supported at present.
- The Sync Tool only synchronizes insertions, deletions, metadata creations and deletions, do not support TTL settings, trigger and other operations.
  - If TTL is set on the sender side, all unexpired data in the IoTDB and all future data writes and deletions will be synchronized to the receiver side when Pipe is started.

## 4.Quick Start

Execute the following SQL statements at the sender and receiver to quickly start a data synchronization task between two IoTDB. For complete SQL statements and configuration matters, please see the `parameter configuration`and `SQL` sections. For more usage examples, please refer to the `usage examples` section.

### 4.1 Receiver

- Start PipeServer.

```
IoTDB> START PIPESERVER
```

- Stop PipeServer(should execute after dropping all pipes which connect to this receiver).

```
IOTDB> STOP PIPESERVER
```

### 4.2 Sender

- Create a pipesink with IoTDB type.

```
IoTDB> CREATE PIPESINK central_iotdb AS IoTDB (IP='There is your goal IP')
```

- Establish a pipe(before creation, ensure that PipeServer is running on receiver).

```
IoTDB> CREATE PIPE my_pipe TO central_iotDB
```

- Start this pipe.

```
IoTDB> START PIPE my_pipe
```

- Show pipe's status.

```
IoTDB> SHOW PIPES
```

- Stop this pipe.

```
IoTDB> STOP PIPE my_pipe
```

- Continue this pipe.

```
IoTDB> START PIPE my_pipe
```

- Drop this pipe(delete all information about this pipe).

```
IoTDB> DROP PIPE my_pipe
```

## 5.Parameter Configuration

All parameters are in `$IOTDB_ HOME$/conf/iotdb-engine`, after all modifications are completed, execute `load configuration` and it will take effect immediately.

### 5.1 Sender

| **Parameter Name** | **max_number_of_sync_file_retry**                            |
| ------------------ | ------------------------------------------------------------ |
| Description        | The maximum number of retries when the sender fails to synchronize files to the receiver. |
| Data type          | Int : [0,2147483647]                                         |
| Default value      | 5                                                            |



### 5.2 Receiver

| **Parameter Name** | **ip_white_list**                                            |
| ------------------ | ------------------------------------------------------------ |
| Description        | Set the white list of IP addresses of the sending end of the synchronization, which is expressed in the form of network segments, and multiple network segments are separated by commas. When the sender synchronizes data to the receiver, the receiver allows synchronization only when the IP address of the sender is within the network segment set in the white list. If the whitelist is empty, the receiver does not allow any sender to synchronize data. By default, the receiving end accepts the synchronization request of all IP addresses. |
| Data type          | String                                                       |
| Default value      | 0.0.0.0/0                                                    |



| **Parameter Name** | **sync_server_port**                                         |
| ------------------ | ------------------------------------------------------------ |
| Description        | The port which the receiver listens, please ensure this port is not occupied by other applications. |
| Data type          | Short Int : [0,65535]                                        |
| Default value      | 6670                                                         |



## 6.SQL

### 6.1 Sender

- Create a pipesink with IoTDB type, where IP and port are optional parameters.

```
IoTDB> CREATE PIPESINK <PipeSinkName> AS IoTDB [(IP='127.0.0.1',port=6670);]
```

- Show all pipesink types supported by IoTDB.

```Plain%20Text
IoTDB> SHOW PIPESINKTYPE
IoTDB>
+-----+
| type|
+-----+
|IoTDB|
+-----+
```

- Show all pipesinks' definition, the results set has three columns, name, pipesink's type and pipesink's attributes.

```
IoTDB> SHOW PIPESINKS
IoTDB> SHOW PIPESINK [PipeSinkName]
IoTDB> 
+-----------+-----+------------------------+
|       name| type|              attributes|
+-----------+-----+------------------------+
|my_pipesink|IoTDB|ip='127.0.0.1',port=6670|
+-----------+-----+------------------------+
```

- Drop the pipesink with PipeSinkName parameter.

```
IoTDB> DROP PIPESINK <PipeSinkName>
```

- Create a pipe.
- At present, the SELECT statement only supports `**` (i.e. data in all timeseries), the FROM statement only supports `root`, and the WHERE statement only supports the start time of the specified time.
- If the `SyncDelOp` parameter is true, the deletions of sender will not be synchronized to receiver.

```
IoTDB> CREATE PIPE my_pipe TO my_iotdb [FROM (select ** from root WHERE time>='yyyy-mm-dd HH:MM:SS' )] [WITH SyncDelOp=true]
```

- Show all pipes' status.

> This statement can be executed on both senders and receivers.

- Create time, the creation time of this pipe.
- Name, the name of this pipe.
- Role, the current role of this IoTDB in pipe, there are two possible roles.
  - Sender, the current IoTDB is the synchronous sender
  - Receiver, the current IoTDB is the synchronous receiver
- Remote, information about the opposite end of the pipe.
  - When role is receiver, the value of this field is the sender's IP.
  - When role is sender, the value of this field is the pipeSink name.

- Status, this pipe's status.
- Message, the status message of this pipe. When pipe runs normally, this column is usually empty. When an exception occurs, messages may appear in  following two states.
  - WARN, this indicates that a data loss or other error has occurred, but the pipe will remain running.
  - ERROR, this indicates that the network is interrupted for a long time or there is a problem at the receiving end. The pipe is stopped and set to STOP state.

```
IoTDB> SHOW PIPES
IoTDB>
+-----------------------+--------+--------+-------------+---------+-------+
|            create time|   name |    role|       remote|   status|message|
+-----------------------+--------+--------+-------------+---------+-------+
|2022-03-30T20:58:30.689|my_pipe1|  sender|  my_pipesink|     STOP|       |
+-----------------------+--------+--------+-------------+---------+-------+ 
|2022-03-31T12:55:28.129|my_pipe2|receiver|192.168.11.11|  RUNNING|       |
+-----------------------+--------+--------+-------------+---------+-------+
```

- Show the pipe status with PipeName. When the PipeName is empty，it is the same with `Show PIPES`.

```
IoTDB> SHOW PIPE [PipeName]
```

- Stop the pipe with PipeName.

```
IoTDB> STOP PIPE <PipeName>
```

- Continue the pipe with PipeName.

```
IoTDB> START PIPE <PipeName>
```

- Drop the pipe with PipeName(delete all information about this pipe).

```
IoTDB> DROP PIPE <PipeName>
```

#### 6.2 Receiver

- Start the PipeServer service.

```
IoTDB> START PIPESERVER
```

- Stop the PipeServer service.

```
IoTDB> STOP PIPESERVER
```

- Show the information of PipeServer.
  - True means the PipeServer is running, otherwise not.

```
IoTDB> SHOW PIPESERVER STATUS
+----------+
|    enalbe|
+----------+
|true/false|
+----------+
```

## 7. Usage Examples

##### Goal

- Create a synchronize task from sender IoTDB to receiver IoTDB.
- Sender wants to synchronize the data after 2022-3-30 00:00:00.
- Sender does not want to synchronize the deletions.
- Sender has an unstable network environment, needs more retries.
- Receiver only wants to receive data from this sender(sender ip 192.168.0.1).

##### **Receiver**

- `vi conf/iotdb-datanode.properties`  to config the parameters，set the IP white list to 192.168.0.1/1 to receive and only receive data from sender.

```
####################
### PIPE Server Configuration
####################
# PIPE server port to listen
# Datatype: int
# pipe_server_port=6670

# White IP list of Sync client.
# Please use the form of network segment to present the range of IP, for example: 192.168.0.0/16
# If there are more than one IP segment, please separate them by commas
# The default is to allow all IP to sync
# Datatype: String
ip_white_list=192.168.0.1/1
```

- Start PipeServer service at receiver.

```
IoTDB> START PIPESERVER
```

- Show PipeServer's status, a `True` result means running correctly.

```
IoTDB> SHOW PIPESERVER STATUS
```

##### Sender

- Config the `max_number_of_sync_file_retry` parameter to 10.

```
####################
### PIPE Sender Configuration
####################
# The maximum number of retry when syncing a file to receiver fails.
max_number_of_sync_file_retry=10
```

- Create pipesink with IoTDB type, input ip address 192.168.0.1, port 6670.

```
IoTDB> CREATE PIPESINK my_iotdb AS IoTDB (IP='192.168.0.2'，PORT=6670)
```

- Create pipe connect to my_iotdb, input the start time 2022-03-30 00:00:00 in WHERE statments, set the `SyncDelOp` to false.

```
IoTDB> CREATE PIPE p TO my_iotdb FROM (select ** from root where time>='2022-03-30 00:00:00') WITH SyncDelOp=false
```

- Start the pipe p

```
IoTDB> START PIPE p
```

- Show the status of pipe p.

```
IoTDB> SHOW PIPE p
```

##### Result Verification

Execute SQL on sender.

```
SET STORAGE GROUP TO root.vehicle;
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

- Execute 

  ```
  STOP PIPESERVER
  ```

  to close IoTDB PipeServer service with message.

  ```
  Msg: 328: Failed to stop pipe server because there is pipe still running.
  ```

  - Cause by: There is a running pipe connected to this receiver.
  - Solution: Execute `STOP PIPE <PipeName>` to stop pipe, then stop PipeServer service.

- Execute 

  ```
  CREATE PIPE mypipe
  ```

  get message.

  ```
  Msg: 411: Create transport for pipe mypipe error, because CREATE request connects to receiver 127.0.0.1:6670 error..
  ```

  - Cause by: The receiver is not started or the receiver cannot be connected.
  - Solution: Execute `SHOW PIPESERVER` on the receiver side to check if the receiver side is started, if not use `START PIPESERVER` to start; check if the whitelist in `iotdb-datanode.properties` on the receiver side contains the sender ip.

- Execute 

  ```
  DROP PIPESINK pipesinkName
  ```

  get message.

  ```
  Msg: 411: Can not drop pipeSink demo, because pipe mypipe is using it.
  ```

  - Cause by: It is not allowed to delete PipeSink that is used by a running PIPE.
  - Solution: Execute `SHOW PIPE` on the sender side to stop using the PipeSink's PIPE.

- Sender creates PIPE prompt.

  ```
  Msg: 411: Pipe p is RUNNING, please retry after drop it.
  ```

  - Cause by: There is already a running PIPE.
  - Solution: Execute `DROP PIPE p ` and retry.
