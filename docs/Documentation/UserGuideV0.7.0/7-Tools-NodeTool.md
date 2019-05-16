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

- [Cli/shell tool](#clishell-tool)
    - [Running Cli/Shell](#running-clishell)
    - [Cli/Shell Parameters](#clishell-parameters)

<!-- /TOC -->
# NodeTool/Shell tool
IoTDB provides NodeTool/Shell tools for users to monitor the working status of IoTDB Cluster in command lines, and users are able to run multiple commands to get specific status. This document will show how NodeTool/Shell tool works and the ways to use all the commands, and one example will be provided for each command.

> Note: In this document, \$IOTDB\_HOME represents the path of the IoTDB installation directory.

## Running NodeTool/Shell

The nodetool startup script is the `nodetool` file under the \$IOTDB\_HOME/bin folder. When starting the script, you are able to specify the IP and PORT. (Make sure the IoTDB cluster is running properly on the host that you use NodeTool/Shell tool to connect to.) The IP should be IP of the host that users intend to connect to, and PORT should be the JMX service port of the IoTDB server. The default IP is 127.0.0.1 and default PORT is 31999. If users intend to monitor remote cluster or the JMX service port is modified, the specific IP and PORT must be set at -h and -p.

## Cli/Shell Commands

### Print Hash Ring (ring)

IoTDB Cluster uses consistent hash to achieve data distribution. Each node's token on the hash ring determines the scope of its data. Therefore, users are able to get the data jurisdiction of each node by printing the hash ring information. IoTDB Cluster supports the enabling of virtual nodes, and the virtual node hash ring is printed by default.

#### Input

The command to print hash ring is `ring`, parametor specification:

|Parameter name| Description| Example |
|:---|:---|:---|
| -p \| --physical | Whether to print physical node hash ring, default `false` | `-p` |

#### Output

The output is multiple string lines, each string line represents a key-value pair, where the key is token and the value is IP of node. The format of each line is `key -> value`.

#### Example

Assume that the IoTDB Cluster is running on 3 nodes: 192.168.130.14, 192.168.130.16 and 192.168.130.18, and the number of virtual nodes is 2.

* Print virtual node hash ring

	The Linux and MacOS system startup commands are as follows:
	```
	  Shell > ./bin/nodetool.sh -h 192.168.130.14 ring	```
	  
	The Windows system startup commands are as follows:
	```
	  Shell > \bin\nodetool.bat -h 192.168.130.14 ring	```
	  
	After using the command, the successful output will be as follows: 
	
	```
	-522854693	->	192.168.130.16
	-230397323	->	192.168.130.18
	139451582	->	192.168.130.14
	772471848	->	192.168.130.14
	1054453781	->	192.168.130.18
	1460348238	->	192.168.130.16
	```
	The above output indicates that the current cluster contains 3 nodes, among which the data with the hash value of -230397322 to 772471848 belongs to the 192.168.130.14 node, the data of -522854692 to -230397323, 772471849 to 1054453781 belong to the 192.168.130.18 node, and the rest belong to the 192.168.130.16 node.

* Print physical node hash ring

	The Linux and MacOS system startup commands are as follows:
	```
	  Shell > ./bin/nodetool.sh -h 192.168.130.14 ring -p	```
	  
	The Windows system startup commands are as follows:
	```
	  Shell > \bin\nodetool.bat -h 192.168.130.14 ring -p	```
	  
	After using the command, the successful output will be as follows: 
	
	```
	-1591386697	->	192.168.130.14
	1977445627	->	192.168.130.18
	2049847333	->	192.168.130.16
	```
	The above output indicates that the current cluster contains 3 nodes, among which the data with the hash value in 1977445628 to 2049847333 belongs to the 192.168.130.16 node, the data from -1591386696 to 1977445627 belongs to the 192.168.130.18 node, and the rest belong to the 192.168.130.14 node.
	
### Query Data Group Info and Leader for Storage Gruop (storagegroup)

The data of the IoTDB cluster is divided into multiple data partitions according to the storage group. The storage group and the data partition have a many-to-one relationship, that is, the same storage group exists only in the same data partition, and one data partition contains multiple storage groups. A data partition consists of multiple nodes, the number of nodes is the number of replicas, and one of the nodes acts as a leader. With this command, users can know which nodes of all the storage groups are stored, and which node acts as the current leader.

#### Input

The command to query data group information for storage group is `storagegroup`, parametor specification:

|Parameter name| Description| Example |
|:---|:---|:---|
| -sg \| --storagegroup <storage group path> | The path of the storage group that need to query, metadata group is queried by default | `-sg root.t1.d1` |

#### Output

The output is one string line that represents multiple node IPs, where each IP is separated by commas and the first one acts as the leader.

#### Example

Assume that the IoTDB Cluster is running on 3 nodes: 192.168.130.14, 192.168.130.16 and 192.168.130.18, and number of replicas is 2.

* No storage group

	The Linux and MacOS system startup commands are as follows:
	```
	  Shell > ./bin/nodetool.sh -h 192.168.130.14 storagegroup	```
	  
	The Windows system startup commands are as follows:
	```
	  Shell > \bin\nodetool.bat -h 192.168.130.14 storagegroup	```
	  
	After using the command, the successful output will be as follows: 
	
	```
	192.168.130.14 (leader), 192.168.130.16, 192.168.130.18
	```
	The above output indicates that the current metadata group contains 3 nodes, among which 192.168.130.14 acts as leader.
	
* Set storage group

	The Linux and MacOS system startup commands are as follows:
	```
	  Shell > ./bin/nodetool.sh -h 192.168.130.14 storagegroup -sg root.t1.d1
	```
	  
	The Windows system startup commands are as follows:
	```
	  Shell > \bin\nodetool.bat -h 192.168.130.14 storagegroup -sg root.t1.d1
	```
	  
	After using the command, the successful output will be as follows: 
	
	```
	192.168.130.14 (leader), 192.168.130.18
	```
	The above output indicates that the data partition which `root.t1.d1` belongs to contains 2 nodes, among which 192.168.130.14 acts as leader.
	
### Query Data Partition Info and Storage Group for Host (host)

The data partition and storage group of the IoTDB Cluster has a one-to-many relationship, that is, the same data partition contains multiple storage groups. A data partition consists of multiple nodes, the number of nodes is the number of replicas, and one of the nodes acts as leader. With this command, users are able to know all the data partitions to which the connected node belongs, and all the storage groups contained in each data partition.

#### Input

The command to query data partition information for host is `host`, parametor specification:

|Parameter name| Description| Example |
|:---|:---|:---|
| -i \| --ip <ip> | The IP of queried host, default is `127.0,0,1` | `-i 192.168.130.14` |
| -p \| --port <port> | The PORT of queried host, default is port in configuration of connected node | `-p 8888` |
| -d \| --detail | Whether output the path of storage groups, default only output number | `-d` |

#### Output

The output is multiple string lines, each string line represents a key-value pair, where the key is data partition info (multiple node IPs) and the value is info of storage groups that belong to this data partition. The format of each line is `key -> value`. If `-d` is set to true, storage group info is multiple storage group paths, otherwise it's number of storage groups.

#### Example

Assume that the IoTDB Cluster is running on 3 nodes: 192.168.130.14, 192.168.130.16 and 192.168.130.18, and number of replicas is 2, number of storage groups is 10.

* `-d` is false

	The Linux and MacOS system startup commands are as follows:
	```
	  Shell > ./bin/nodetool.sh -h 192.168.130.14 host -i 192.168.130.14
	```
	  
	The Windows system startup commands are as follows:
	```
	  Shell > \bin\nodetool.bat -h 192.168.130.14 host -i 192.168.130.14
	```
	  
	After using the command, the successful output will be as follows: 
	
	```
	(192.168.130.14, 192.168.130.18)	->	4
	(192.168.130.14, 192.168.130.16)	->	4
	```
	The above output indicates that node 192.168.130.14 belongs to 2 data partitions, among which each data partition contains 4 storage groups.
	
* `-d` is true

	The Linux and MacOS system startup commands are as follows:
	```
	  Shell > ./bin/nodetool.sh -h 192.168.130.14 host -i 192.168.130.14 -d
	```
	  
	The Windows system startup commands are as follows:
	```
	  Shell > \bin\nodetool.bat -h 192.168.130.14 host -i 192.168.130.14 -d
	```
	  
	After using these commands, the successful output will be as follows: 
	
	```
	(192.168.130.14, 192.168.130.18)	->	(root.group_2, root.group_6, root.group_8, root.group_9)
	(192.168.130.14, 192.168.130.16)	->	(root.group_1, root.group_4, root.group_5, root.group_7)
	```
	The above output indicates that node 192.168.130.14 belongs to 2 data partitions, where the first data partition contains root.group_2, root.group_6, root.group_8, root.group_9 for a total of 4 storage groups, and the second data partition contains root.group_1, Root.group_4, root.group_5, root.group_7 for a total of 4 storage groups.

### Query Replica Lag Info of All Storage Groups (lag)

IoTDB Cluster contains one metadata group and multiple data partitions, each data group contains one leader and multiple replicas, and there may be differences between each replica and the latest data. With this command, users are able to know the difference between each replica and the latest data in each data group, and the specific difference is represented by the difference between committed log index.

#### Input

The command to query replica lag is `lag`, no additional parameters are needed.

#### Output

The output is multiple string lines, where is divided into multiple data groups info. The first line of each data group info is data group ID, each line after the ID represents a key-value pair, where the key is IP of the replica holder and the value is difference of committed log index between replica and leader. The format of each line is `key -> value`.

#### Example

Assume that the IoTDB Cluster is running on 3 nodes: 192.168.130.14, 192.168.130.16 and 192.168.130.18, and number of replicas is 3.

The Linux and MacOS system startup commands are as follows:
```
  Shell > ./bin/nodetool.sh -h 192.168.130.14 lag
```
  
The Windows system startup commands are as follows:
```
  Shell > \bin\nodetool.bat -h 192.168.130.14 lag
```
  
After using the command, the successful output will be as follows: 
	
```
metadata:
	192.168.130.18	->	0
	192.168.130.14 (leader)	->	0
	192.168.130.16	->	0
data-group-0:
	192.168.130.18	->	0
	192.168.130.14 (leader)	->	0
	192.168.130.16	->	0
```
The above output indicates that the current cluster contains 2 data groups, wherein the metadata group contains 3 nodes and 2 replicas, of which 192.168.130.14 acts as leader, each replica has a lag of 0; and the data partition data-group-0 contains 3 nodes and 2 replicas, of which 192.168.130.14 acts as leader, and each replica has a lag of 0.

### Query Number of Query Tasks among Cluster (query)

In IoTDB Cluster, each node belongs to multiple data partitions, where there may be multiple concurrent query tasks running on each data partition. With this command, users are able to know the query task load and the total query task load of each data group on all nodes of cluster, and the task load is represented by the number of tasks.

#### Input

The command to query number of query tasks is `query`, no additional parameters are needed.

#### Output

The output is multiple string lines, where is divided into multiple nodes info. The first line of each node info is node IP, each line after the IP represents a key-value pair, where the key is data group ID and the value is number of query tasks running on this data partition. The format of each line is `key -> value`. The last line represents the total number of query tasks running on cluster.

#### Example

Assume that the IoTDB Cluster is running on 3 nodes: 192.168.130.14, 192.168.130.16 and 192.168.130.18, and number of replicas is 2.

The Linux and MacOS system startup commands are as follows:
```
  Shell > ./bin/nodetool.sh -h 192.168.130.14 query
```
  
The Windows system startup commands are as follows:
```
  Shell > \bin\nodetool.bat -h 192.168.130.14 query
```
  
After using the command, the successful output will be as follows: 
	
```
192.168.130.14:
  data-group-0  ->   1
  data-group-1  ->   3
192.168.130.16:
  data-group-2  ->   2
  data-group-1  ->   0
192.168.130.18:
  data-group-0  ->   0
  data-group-2  ->   1
Total  ->   7

```
The above output indicates that 7 query tasks are running on cluster. Moreover, node 192.168.130.14 contains 2 data partitions and 4 query tasks are running on it, wherein 1 query task is running on data partition data-group-0, and 3 query tasks are running on data partition data-group-1; node 192.168.130.16 contains 2 data partitions and 2 query tasks are running on it, wherein 2 query tasks is running on data partition data-group-2, and no query task is running on data partition data-group-0; node 192.168.130.18 contains 2 data partitions and 1 query task is running on it, wherein no query task is running on data partition data-group-0, and 1 query task is running on data partition data-group-2.