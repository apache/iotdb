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
IoTDB cluster version provides nodetool shell tool for users to monitor the working status of the specified cluster. 
Users can obtain the status of the cluster by running a variety of instructions.

The following describes the usage and examples of each instruction, 
where $IOTDB_CLUSTER_HOME indicates the path of the IoTDB distributed installation directory.
# Instructions
## Get Started
The nodetool shell tool startup script is located at $IOTDB_CLUSTER_HOME/bin folder, 
you can specify the IP address and port of the cluster at startup.

IP is the IP of the node that the user expects to connect to,
and port is the JMX service port specified when the IoTDB cluster is started.

The default values are 127.0.0.1 and 31999, respectively.

If you need to monitor the remote cluster or modify the JMX service port number,
use the actual IP and port at the -H and -P entries.

## Explains
In a distributed system, a node is identified by node IP, metadata port, data port and cluster port \<METAPORT:DATAPORT:CLUSTERPORT>.
### Show The Ring Of Node
IoTDB cluster version uses consistent hash to achieve data distribution.

Users can know the location of each node in the ring by printing hash ring information.

1.Input
> ring

2.Output

> The output is a multi line string, and each line of string is a key value pair, 
> where the key represents the token value and the value represents the node (IP:METAPORT:DATAPORT:CLUSTERPORT), the format is \<key -> value>.

3.Examples

> Suppose that the current cluster runs on three nodes: 127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561, and 127.0.0.1:9007:40014:55562.

Examples of input instructions for different systems are as follows:

Linux and MacOS：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 ring
```

Windows：

```
Shell > \sbin\nodetool.bat -h 127.0.0.1 -p 31999 ring
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
Node Identifier                                 Node 
330411070           ->          127.0.0.1:9003:40010:55560 
330454032           ->          127.0.0.1:9005:40012:55561 
330496472           ->          127.0.0.1:9007:40014:55562
```
 
The above output shows that there are three nodes in the current cluster,
and the output results are output clockwise according to their positions in the ring.

### Query data partition and metadata partition
The time series metadata of iotdb cluster version is divided into multiple data partitions according to storage groups,
in which the storage group and data partition are many to one relationship.

That is, the same storage group only exists in the same data partition,
and a data partition contains multiple storage groups.

The data is divided into multiple data partitions according to the storage group and time interval,
and the partition granularity is <storage group, time range>.

The data partition is composed of replica nodes to ensure high availability of data,
and one of the nodes plays the role of leader.

Through this instruction, the user can know the metadata under a certain path
 or the nodes under which the data is stored.

1.Input
> The instruction for querying data partition information is partition.
> The parameters are described as follows:

|Parameter|Description|Examples|
| --- | --- | --- |
|-m | --metadata	Query metadata partition, the default is query data partition|	-m |
|-path | --path 	Required parameter, the path to be queried. If the path has no corresponding storage group, the query fails|	-path root.guangzhou.d1|
|-st | --StartTime	The system uses the current partition time by default|	-st 1576724778159 |
|-et | --EndTime	It is used when querying data partition.<br>The end time is the current system time by default. <br> If the end time is less than the start time, the end time is the start time by default|-et 1576724778159 |

2.Output

> The output is a multi line string, and each line of string is a key value pair, where the key represents the partition,
> and the value represents the data group in the format of \< key -> value>.

3.Examples

> Suppose that the current cluster runs on three nodes: 127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561, and 127.0.0.1:9007:40014:55562.
> 
> The number of copies is 2 and there are 3 storage groups:{ root.beijing , root.shanghai , root.guangzhou}.

+ Partition of query data (default time interval, time dimension is partitioned by day)

Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1
```
Windows：
```
Shell > \sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
DATA<root.guangzhou.d1, 1576723735188, 1576723735188>	->	[127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561]
```

+ Partition of query data (specified time interval, time dimension is partitioned by day)


Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -st 1576624778159 -et 1576724778159
```
Windows：
```
Shell > \sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -st 1576624778159 -et 1576724778159
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
DATA<root.guangzhou.d1, 1576627200000, 1576713599999>	->	[127.0.0.1:9007:40014:55562, 127.0.0.1:9003:40010:55560] 
DATA<root.guangzhou.d1, 1576713600000, 1576724778159>	->	[127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561] 
DATA<root.guangzhou.d1, 1576624778159, 1576627199999>	->	[127.0.0.1:9005:40012:55561, 127.0.0.1:9007:40014:55562]
```

+ Query metadata partition

Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -m
```
Windows：
```
Shell > \sbin\nodetool.bat -h 127.0.0.1 -p 31999 partition -path root.guangzhou.d1 -m
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
DATA<root.guangzhou.d1, 1576723735188, 1576723735188>	->	[127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561]
```
The above output shows that the data partition to which root.t1.d1 belongs contains two nodes,
of which 127.0.0.1:9003:40010:55560 is the header node.


### Query the number of slots managed by the node
IoTDB cluster version divides the hash ring into a fixed number of (10000) slots,
and the leader of the cluster management group divides the slots into data groups.

Through this instruction, the user can know the number of slots managed by the data group.

1. Input
> The command to query the data partition information corresponding to the node is host.
> 
> The parameters are described as follows:

|Parameter|Description|Examples|
|---|---|---|
|-a or --all |Query the number of slots managed by all data groups. The default is the data group of the query node|-a|

2.Output

> The output is a multi line string, in which each line string is a key value pair, where the key represents the data group,
> and the value represents the number of slots managed, and the format is \<key -> value>.

3.Examples

> Suppose that the current cluster runs on three nodes: 127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561, and 127.0.0.1:9007:40014:55562,
> and the number of copies is 2.

+ Default Partition Group

Linux and MacOS：

```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 host
```
Windows：
```
Shell > \sbin\nodetool.bat -h 127.0.0.1 -p 31999 host
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
Raft group                                                 Slot Number
(127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561)      ->                3333
(127.0.0.1:9007:40014:55562, 127.0.0.1:9003:40010:55560)      ->                3334
```
+ All Partition Groups

Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 host -a
```
Windows：
```
Shell > \sbin\nodetool.bat -h 127.0.0.1 -p 31999 host -a
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
Raft group                                                 Slot Number
(127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561)      ->                3333
(127.0.0.1:9005:40012:55561, 127.0.0.1:9007:40014:55562)      ->                3333
(127.0.0.1:9007:40014:55562, 127.0.0.1:9003:40010:55560)      ->                3334 
```

### Query node status
IoTDB cluster version contains multiple nodes.
For any node, there is the possibility that it cannot provide services normally due to network and hardware problems.

Through this instruction, the user can know the current status of all nodes in the cluster.

1.Input
> status

2.Output
> The output is a multi line string, where each line string is a key value pair, where the key represents the node (IP: METAPORT:DATAPORT),
> the value indicates the state of the node, "on" is normal, "off" is abnormal, and the format is \< key -> value>.

3.Examples
> Suppose that the current cluster runs on three nodes: 127.0.0.1:9003:40010:55560, 127.0.0.1:9005:40012:55561, and 127.0.0.1:9007:40014:55562,
> and the number of copies is 2.

Linux and MacOS：
```
Shell > ./sbin/nodetool.sh -h 127.0.0.1 -p 31999 status
```
Windows：
```
Shell > \sbin\nodetool.bat -h 127.0.0.1 -p 31999 status
```

Press enter to execute the command. 

The output of the example instruction is as follows:
```
Node                                Status 
127.0.0.1:9003:40010:55560          ->        on 
127.0.0.1:9005:40012:55561          ->        off
127.0.0.1:9007:40014:55562          ->        on 

```
The above output indicates that 127.0.0.1:9003:40010:55560 nodes and 127.0.0.1:9007:40014:55562 nodes are in normal state,
and 127.0.0.1:9005:40012:55561 nodes cannot provide services.
