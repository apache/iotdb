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

# Introduction
IoTDB cluster version provides nodetool, a shell tool for users to monitor the working status of the specified cluster. 
Users can obtain the status of the cluster by running a variety of instructions.

The following describes the usage and examples of each instruction, 
where $IOTDB_CLUSTER_HOME indicates the path of the distributed IoTDB installation directory.

# Prerequisites
To use the nodetool, you need to enable JMX service. Please refer to [JMX tool](JMX-Tool.md) for details.

# Instructions
## Get Started
The nodetool shell tool startup script is located at $IOTDB_CLUSTER_HOME/sbin folder,
you can specify the IP address and port of the cluster during startup.

IP is the IP (or hostname) of the node that you expect to connect to,
and port is the JMX service port specified when the IoTDB cluster is started.

The default values are `127.0.0.1` and `31999`, respectively.

If you need to monitor the remote cluster or modify the JMX service port number,
set the actual IP and port with the `-h` and `-p` options.

When JMX authentication service is enabled, the username and password of JMX service should be specified. 
The default values are `iotdb` and `passw!d`, respectively. Please use the start parameters `-u` and `-pw`
to specify the username and password of the JMX service.

## Explains
In a distributed system, a node is identified by node IP, metadata port, data port and cluster port \<IP:METAPORT:DATAPORT:CLUSTERPORT>.
### Show The Ring Of Node
Distributed IoTDB uses consistent hash to support data distribution.

You can know each node in the cluster by command `ring`, which prints node ring information.

1.Input
> ring

2.Output

> The output is a multi-line string, and each line of string is a key value pair, 
> where the key represents the node identifier, and the value represents the node (IP:METAPORT:DATAPORT:CLUSTERPORT), the format is \<key -> value>.

3.Examples

> Suppose that the current cluster runs on three nodes: 127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668, and 127.0.0.1:9007:40014:6669.

Examples of input instructions for different systems are as follows:

Linux and MacOS：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 ring
```

Windows：

```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 ring
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
Node Identifier                                 Node 
330411070           ->          127.0.0.1:9003:40010:6667
330454032           ->          127.0.0.1:9005:40012:6668 
330496472           ->          127.0.0.1:9007:40014:6669
```
 
The above output shows that there are three nodes in the current cluster,
and the output results are ordered by their identifier ascendant.

### Query data partition and metadata partition
The time series metadata of distributed iotdb is divided into multiple data groups according to their storage groups,
in which the storage group and data partition are many to one relationship.

That is, all metadata of a storage group only exists in the same data group,
and a data group may contain multiple storage groups.

The data is divided into multiple data groups according to its storage group and timestamp,
and the time partition granularity is decided by a configuration (currently unavailable).

The data partition is composed of several replica nodes to ensure high availability of data,
and one of the nodes plays the role of leader.

Through this instruction, the user can know the metadata of a certain path,
 and the nodes under which the data is stored.

1.Input
> The instruction for querying data partition information is `partition`.
> The parameters are described as follows:

|Parameter|Description|Examples|
| --- | --- | --- |
|-m | --metadata	Query metadata partition, by default only query data partition|	-m |
|-path | --path 	Required parameter, the path to be queried. If the path has no corresponding storage group, the query fails|	-path root.guangzhou.d1|
|-st | --StartTime	The system uses the current partition time by default|	-st 1576724778159 |
|-et | --EndTime	It is used when querying data partition.<br>The end time is the current system time by default. <br> If the end time is less than the start time, the end time is the start time by default|-et 1576724778159 |

2.Output

> The output is a multi-line string, and each line of string is a key-value pair, where the key represents the partition,
> and the value represents the data group in the format of \< key -> value>.

3.Examples

> Suppose that the current cluster runs on three nodes: 127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668, and 127.0.0.1:9007:40014:6669.
> 
> The number of copies is 2 and there are 3 storage groups:{ root.beijing , root.shanghai , root.guangzhou}.

+ Partition of query data (default time range, time partition interval is one day)

Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1
```
Windows：
```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
DATA<root.guangzhou.d1, 1576723735188, 1576723735188>	->	[127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668]
```

+ Partition of query data (specified time range, time partition interval is one day)


Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -st 1576624778159 -et 1576724778159
```
Windows：
```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -st 1576624778159 -et 1576724778159
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
DATA<root.guangzhou.d1, 1576627200000, 1576713599999>	->	[127.0.0.1:9007:40014:6669, 127.0.0.1:9003:40010:6667] 
DATA<root.guangzhou.d1, 1576713600000, 1576724778159>	->	[127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668] 
DATA<root.guangzhou.d1, 1576624778159, 1576627199999>	->	[127.0.0.1:9005:40012:6668, 127.0.0.1:9007:40014:6669]
```

+ Query metadata partition

Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -m
```
Windows：
```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -m
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
DATA<root.guangzhou.d1, 1576723735188, 1576723735188>	->	[127.0.0.1:9003:40010, 127.0.0.1:9004:40011]
```
The above output shows that the data group to which root.guangzhou.d1 belongs contains two nodes,
of which 127.0.0.1:9003:40010 is the header node.


### Query the number of slots managed by the node
Distributed IoTDB divides data into a fixed number of (10000 by default) slots,
and the leader of the cluster management group divides the slots among data groups.

Through this instruction, you can know the number of slots managed by each data group.

1. Input
> The command to query the data partition information corresponding to the node is `host`.
> 
> The parameters are described as follows:

|Parameter|Description|Examples|
|---|---|---|
|-a or --all |Query the number of slots managed by all data groups. By default only data groups of the query node are shown|-a|

2.Output

> The output is a multi-line string, in which each line is a key-value pair, where the key represents the data group,
> and the value represents the number of slots managed, and the format is \<key -> value>.

3.Examples

> Suppose that the current cluster runs on three nodes: 127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668, and 127.0.0.1:9007:40014:6669,
> and the number of copies is 2.

+ Default Partition Group

Linux and MacOS：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 host
```

Windows：
```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 host
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
Raft group                                                 Slot Number
(127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668)      ->                3333
(127.0.0.1:9007:40014:6669, 127.0.0.1:9003:40010:6667)      ->                3334
```
+ All Partition Groups

Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 host -a
```
Windows：
```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 host -a
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
Raft group                                                 Slot Number
(127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668)      ->                3333
(127.0.0.1:9005:40012:6668, 127.0.0.1:9007:40014:6669)      ->                3333
(127.0.0.1:9007:40014:6669, 127.0.0.1:9003:40010:6667)      ->                3334 
```

### Query node status
Distributed IoTDB contains multiple nodes.
For any node, there is a possibility that it cannot provide services normally due to network or hardware problems.

Through this instruction, you can know the current status of all nodes in the cluster.

1.Input
> status

2.Output
> The output is a multi-line string, where each line is a key-value pair, where the key represents the node (IP: METAPORT:DATAPORT),
> the value indicates the status of the node, "on" is normal, "off" is abnormal, and the format is \< key -> value>.

3.Examples
> Suppose that the current cluster runs on three nodes: 127.0.0.1:9003:40010:6667, 127.0.0.1:9005:40012:6668, and 127.0.0.1:9007:40014:6669,
> and the number of copies is 2.

Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 status
```
Windows：
```
Shell > .\sbin\nodetool.bat -h 127.0.0.1 -p 31999 status
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
Node                                Status 
127.0.0.1:9003:40010:6667          ->        on 
127.0.0.1:9005:40012:6668          ->        off
127.0.0.1:9007:40014:6669          ->        on 

```
The above output indicates that 127.0.0.1:9003:40010:6667 nodes and 127.0.0.1:9007:40014:6669 nodes are in normal state,
and 127.0.0.1:9005:40012:6668 nodes cannot provide services.
