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

# DataNode Configuration Parameters

We use the same configuration files for IoTDB DataNode and Standalone version, all under the `conf`.

* `datanode-env.sh/bat`：Environment configurations, in which we could set the memory allocation of DataNode and Standalone.

* `iotdb-datanode.properties`：IoTDB DataNode system configurations.

## Hot Modification Configuration

For the convenience of users, IoTDB provides users with hot modification function, that is, modifying some configuration parameters in `iotdb-datanode.properties` and `iotdb-common.properties` during the system operation and applying them to the system immediately. 
In the parameters described below, these parameters whose way of `Effective` is `hot-load` support hot modification.

Trigger way: The client sends the command(sql) `load configuration` to the IoTDB server.

## Environment Configuration File（datanode-env.sh/bat）

The environment configuration file is mainly used to configure the Java environment related parameters when DataNode is running, such as JVM related configuration. This part of the configuration is passed to the JVM when the DataNode starts.

The details of each parameter are as follows:

* MAX\_HEAP\_SIZE

|Name|MAX\_HEAP\_SIZE|
|:---:|:---|
|Description|The maximum heap memory size that IoTDB can use |
|Type|String|
|Default| On Linux or MacOS, the default is one quarter of the memory. On Windows, the default value for 32-bit systems is 512M, and the default for 64-bit systems is 2G.|
|Effective|After restarting system|

* HEAP\_NEWSIZE

|Name|HEAP\_NEWSIZE|
|:---:|:---|
|Description|The minimum heap memory size that IoTDB will use when startup |
|Type|String|
|Default| On Linux or MacOS, the default is min{cores * 100M, one quarter of MAX\_HEAP\_SIZE}. On Windows, the default value for 32-bit systems is 512M, and the default for 64-bit systems is 2G.|
|Effective|After restarting system|

* MAX\_DIRECT\_MEMORY\_SIZE

|Name|MAX\_DIRECT\_MEMORY\_SIZE|
|:---:|:---|
|Description|The max direct memory that IoTDB could use|
|Type|String|
|Default| Equal to the MAX\_HEAP\_SIZE|
|Effective|After restarting system|

* JMX\_LOCAL

|Name|JMX\_LOCAL|
|:---:|:---|
|Description|JMX monitoring mode, configured as yes to allow only local monitoring, no to allow remote monitoring|
|Type|Enum String: "true", "false"|
|Default|true|
|Effective|After restarting system|

* JMX\_PORT

|Name|JMX\_PORT|
|:---:|:---|
|Description|JMX listening port. Please confirm that the port is not a system reserved port and is not occupied|
|Type|Short Int: [0,65535]|
|Default|31999|
|Effective|After restarting system|

* JMX\_IP

|Name|JMX\_IP|
|:---:|:---|
|Description|JMX listening address. Only take effect if JMX\_LOCAL=false. 0.0.0.0 is never allowed|
|Type|String|
|Default|127.0.0.1|
|Effective|After restarting system|

## JMX Authorization

We **STRONGLY RECOMMENDED** you CHANGE the PASSWORD for the JMX remote connection.

The user and passwords are in ${IOTDB\_CONF}/conf/jmx.password.

The permission definitions are in ${IOTDB\_CONF}/conf/jmx.access.

## DataNode/Standalone Configuration File (iotdb-datanode.properties)

### Data Node RPC Configuration

* dn\_rpc\_address

|Name| dn\_rpc\_address                               |
|:---:|:-----------------------------------------------|
|Description| The client rpc service listens on the address. |
|Type| String                                         |
|Default| 0.0.0.0                                        |
|Effective| After restarting system                        |

* dn\_rpc\_port

|Name| dn\_rpc\_port |
|:---:|:---|
|Description| The client rpc service listens on the port.|
|Type|Short Int : [0,65535]|
|Default| 6667 |
|Effective|After restarting system|

* dn\_internal\_address

|Name| dn\_internal\_address |
|:---:|:---|
|Description| DataNode internal service host/IP |
|Type| string |
|Default| 127.0.0.1 |
|Effective|After restarting system|

* dn\_internal\_port

|Name| dn\_internal\_port             |
|:---:|:-------------------------------|
|Description| DataNode internal service port |
|Type| int                            |
|Default| 10730                           |
|Effective| After restarting system        |

* dn\_mpp\_data\_exchange\_port

|Name| mpp\_data\_exchange\_port |
|:---:|:---|
|Description| MPP data exchange port |
|Type| int |
|Default| 10740 |
|Effective|After restarting system|

* dn\_schema\_region\_consensus\_port

|Name| dn\_schema\_region\_consensus\_port |
|:---:|:---|
|Description| DataNode Schema replica communication port for consensus |
|Type| int |
|Default| 10750 |
|Effective|After restarting system|

* dn\_data\_region\_consensus\_port

|Name| dn\_data\_region\_consensus\_port |
|:---:|:---|
|Description| DataNode Data replica communication port for consensus |
|Type| int |
|Default| 10760 |
|Effective|After restarting system|

* dn\_join\_cluster\_retry\_interval\_ms

|Name| dn\_join\_cluster\_retry\_interval\_ms                                    |
|:---:|:--------------------------------------------------------------------------|
|Description| The time of data node waiting for the next retry to join into the cluster |
|Type| long                                                                      |
|Default| 5000                                                                      |
|Effective| After restarting system                                                   |

### Target Config Nodes

* dn\_target\_config\_node\_list

|Name| dn\_target\_config\_node\_list                  |
|:---:|:------------------------------------------------|
|Description| ConfigNode Address for DataNode to join cluster |
|Type| String                                          |
|Default| 127.0.0.1:10710                                 |
|Effective| After restarting system                         |

### Connection Configuration

* dn\_rpc\_thrift\_compression\_enable

|Name| dn\_rpc\_thrift\_compression\_enable |
|:---:|:---|
|Description| Whether enable thrift's compression (using GZIP).|
|Type|Boolean|
|Default| false |
|Effective|After restarting system|

* dn\_rpc\_advanced\_compression\_enable

|Name| dn\_rpc\_advanced\_compression\_enable |
|:---:|:---|
|Description| Whether enable thrift's advanced compression.|
|Type|Boolean|
|Default| false |
|Effective|After restarting system|

* dn\_rpc\_selector\_thread\_count

|Name| dn\_rpc\_selector\_thread\_count       |
|:---:|:-----------------------------------|
|Description| The number of rpc selector thread. |
|Type| int                                |
|Default| false                              |
|Effective| After restarting system            |

* dn\_rpc\_min\_concurrent\_client\_num

|Name| dn\_rpc\_min\_concurrent\_client\_num  |
|:---:|:-----------------------------------|
|Description| Minimum concurrent rpc connections |
|Type| Short Int : [0,65535]              |
|Description| 1                                  |
|Effective| After restarting system            |

* dn\_rpc\_max\_concurrent\_client\_num

|Name| dn\_rpc\_max\_concurrent\_client\_num |
|:---:|:---|
|Description| Max concurrent rpc connections|
|Type| Short Int : [0,65535] |
|Description| 65535 |
|Effective|After restarting system|

* dn\_thrift\_max\_frame\_size

|Name| dn\_thrift\_max\_frame\_size |
|:---:|:---|
|Description| Max size of bytes of each thrift RPC request/response|
|Type| Long |
|Unit|Byte|
|Default| 536870912 |
|Effective|After restarting system|

* dn\_thrift\_init\_buffer\_size

|Name| dn\_thrift\_init\_buffer\_size |
|:---:|:---|
|Description| Initial size of bytes of buffer that thrift used |
|Type| long |
|Default| 1024 |
|Effective|After restarting system|

* dn\_core\_client\_count\_for\_each\_node\_in\_client\_manager

|     Name     | dn\_core\_client\_count\_for\_each\_node\_in\_client\_manager |
|:------------:|:--------------------------------------------------------------|
| Description  | Number of core clients routed to each node in a ClientManager |
|     Type     | int                                                           |
|   Default    | 200                                                           |
|  Effective   | After restarting system                                       |

* dn\_max\_client\_count\_for\_each\_node\_in\_client\_manager

|      Name      | dn\_max\_client\_count\_for\_each\_node\_in\_client\_manager |
|:--------------:|:-------------------------------------------------------------|
|  Description   | Number of max clients routed to each node in a ClientManager |
|      Type      | int                                                          |
|    Default     | 300                                                          |
|   Effective    | After restarting system                                      |

### Dictionary Configuration

* dn\_system\_dir

|    Name     | dn\_system\_dir                                                             |
|:-----------:|:----------------------------------------------------------------------------|
| Description | The directories of system files. It is recommended to use an absolute path. |
|    Type     | String                                                                      |
|   Default   | data/datanode/system (Windows: data\\datanode\\system)                      |
|  Effective  | After restarting system                                                     |

* dn\_data\_dirs

|    Name     | dn\_data\_dirs                                                                                                                                                                                                                                                              |
|:-----------:|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Description | The directories of data files. Multiple directories are separated by comma. The starting directory of the relative path is related to the operating system. It is recommended to use an absolute path. If the path does not exist, the system will automatically create it. |
|    Type     | String[]                                                                                                                                                                                                                                                                    |
|   Default   | data/datanode/data (Windows: data\\datanode\\data)                                                                                                                                                                                                                          |
|  Effective  | After restarting system                                                                                                                                                                                                                                                     |

* dn\_multi\_dir\_strategy

|    Name     | dn\_multi\_dir\_strategy                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|:-----------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Description | IoTDB's strategy for selecting directories for TsFile in tsfile_dir. You can use a simple class name or a full name of the class. The system provides the following three strategies: <br>1. SequenceStrategy: IoTDB selects the directory from tsfile\_dir in order, traverses all the directories in tsfile\_dir in turn, and keeps counting;<br>2. MaxDiskUsableSpaceFirstStrategy: IoTDB first selects the directory with the largest free disk space in tsfile\_dir;<br>You can complete a user-defined policy in the following ways:<br>1. Inherit the cn.edu.tsinghua.iotdb.conf.directories.strategy.DirectoryStrategy class and implement its own Strategy method;<br>2. Fill in the configuration class with the full class name of the implemented class (package name plus class name, UserDfineStrategyPackage);<br>3. Add the jar file to the project. |
|    Type     | String                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|   Default   | SequenceStrategy                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|  Effective  | hot-load                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

* dn\_consensus\_dir

|    Name     | dn\_consensus\_dir                                                             |
|:-----------:|:-------------------------------------------------------------------------------|
| Description | The directories of consensus files. It is recommended to use an absolute path. |
|    Type     | String                                                                         |
|   Default   | data/datanode/consensus                                                        |
|  Effective  | After restarting system                                                        |

* dn\_wal\_dirs

|    Name     | dn\_wal\_dirs                                                            |
|:-----------:|:-------------------------------------------------------------------------|
| Description | Write Ahead Log storage path. It is recommended to use an absolute path. |
|    Type     | String                                                                   |
|   Default   | data/datanode/wal                                                        |
|  Effective  | After restarting system                                                  |

* dn\_tracing\_dir

|    Name     | dn\_tracing\_dir                                                            |
|:-----------:|:----------------------------------------------------------------------------|
| Description | The tracing root directory path. It is recommended to use an absolute path. |
|    Type     | String                                                                      |
|   Default   | datanode/tracing                                                            |
|  Effective  | After restarting system                                                     |

* dn\_sync\_dir

|    Name     | dn\_sync\_dir                                                             |
|:-----------:|:--------------------------------------------------------------------------|
| Description | The directories of sync files. It is recommended to use an absolute path. |
|    Type     | String                                                                    |
|   Default   | data/datanode/sync                                                        |
|  Effective  | After restarting system                                                   |

### Metric Configuration

## Enable GC log

GC log is off by default.
For performance tuning, you may want to collect the GC info. 

To enable GC log, just add a parameter "printgc" when you start the DataNode.

```bash
nohup sbin/start-datanode.sh printgc >/dev/null 2>&1 &
```
Or
```cmd
sbin\start-datanode.bat printgc
```

GC log is stored at `IOTDB_HOME/logs/gc.log`.
There will be at most 10 gc.log.* files and each one can reach to 10MB.
