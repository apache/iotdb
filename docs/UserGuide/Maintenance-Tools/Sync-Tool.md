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

# Collaboration of Edge and Cloud

## TsFile Sync Tool

### Introduction
The Sync Tool is an IoTDB suite tool that periodically uploads persistent tsfiles from the sender disk to the receiver and loads them.

On the sender side of the sync, the sync module is a separate process, independent of the IoTDB process. It can be started and closed through a separate script (see Sections `Usage` for details). The frequency cycle of the sync can be set by the user. 

On the receiver side of the sync, the sync module is embedded in the engine of IoTDB and is in the same process with IoTDB. The receiver module listens for a separate port, which can be set by the user (see Section `Configuration` for details). Before using it, it needs to set up a whitelist at the sync receiver, which is expressed as a network segment. The receiver only accepts the data transferred from the sender located in the whitelist segment, as detailed in Section `Configuration`. 

The sync tool has a many-to-one sender-receiver mode - that is, one sync receiver can receive data from multiple sync senders simultaneously while one sync sender can only send data to one sync receiver.

> Note: Before using the sync tool, the client and server need to be configured separately. The configuration is detailed in Sections Configuration.

### Application Scenario
In the case of a factory application, there are usually multiple sub-factories and multiple general(main) factories. Each sub-factory uses an IoTDB instance to collect data, and then synchronize the data to the general factory for backup or analysis. A general factory can receive data from multiple sub-factories and a sub-factory can also synchronize data to multiple general factories. In this scenario, each IoTDB instance manages different devices. 
​      
In the sync module, each sub-factory is a sender, a general factory is a receiver, and senders periodically synchronizes the data to receivers. In the scenario above, the data of one device can only be collected by one sender, so there is no device overlap between the data synchronized by multiple senders. Otherwise, the application scenario of the sync module is not satisfied.

When there is an abnormal scenario, namely, two or more senders synchronize the data of the same device (whose storage group is set as root.sg) to the same receiver, the root.sg data of the sender containing the device data received later by the receiver will be rejected. Example: the engine 1 synchronizes the storage groups root.sg1 and root.sg2 to the receiver, and the engine 2 synchronizes the storage groups root.sg2 and root.sg3 to the receiver. All of them include the time series root.sg2.d0.s0. 
If the receiver receives the data of root.sg2.d0.s0 of the sender 1 first, the receiver will reject the data of root.sg2 of the sender 2.

### Precautions for Use

Statement "alter timeseries add tag" will not effect the receiver when a sync-tool is running.

### Configuration
#### Sync Receiver
The parameter configuration of the sync receiver is located in the configuration file `iotdb-engine.properties` of IoTDB, and its directory is `$IOTDB_HOME/conf/iotdb-engine.properties`. In this configuration file, there are four parameters related to the sync receiver. The configuration instructions are as follows:

|parameter: is_sync_enable||
|--- |--- |
|Description |Sync function switch, which is configured as true to indicate that the receiver is allowed to receive the data from the sender and load it. When set to false, it means that the receiver is not allowed to receive the data from any sender. |
|Type|Boolean|
|Default|false|
|Modalities for Entry into Force after Modification|Restart receiver|


|parameter: IP_white_list||
|--- |--- |
|Description |Set up a white list of sender IP addresses, which is expressed in the form of network segments and separated by commas between multiple network segments. When the sender transfers data to the receiver, only when the IP address of the sender is within the network segment set by the whitelist can the receiver allow the sync operation. If the whitelist is empty, the receiver does not allow any sender to sync data. The default receiver accepts all IP sync requests.|
|Type|String|
|Default|0.0.0.0/0|
|Modalities for Entry into Force after Modification|Restart receiver|


|parameter: sync_server_port||
|--- |--- |
|Description |Sync receiver port to listen. Make sure that the port is not a system reserved port and is not occupied. This paramter is valid only when the parameter is_sync_enable is set to TRUE. |
|Type|Short Int : [0,65535]|
|Default|5555|
|Modalities for Entry into Force after Modification|Restart receiver|

#### Sync Sender
The parameters of the sync sender are configured in a separate configuration file iotdb-sync-client.properties with the installation directory of ```$IOTDB_HOME/conf/iotdb-sync-client.properties```. In this configuration file, there are five parameters related to the sync sender. The configuration instructions are as follows:

|parameter: server_ip||
|--- |--- |
|Description |IP address of sync receiver. |
|Type|String|
|Default|127.0.0.1|
|Modalities for Entry into Force after Modification|Restart client|


|parameter: server_port||
|--- |--- |
|Description |Listening port of sync receiver, it is necessary to ensure that the port is consistent with the configuration of the listening port set in receiver. |
|Type|Short Int : [0,65535]|
|Default|5555|
|Modalities for Entry into Force after Modification|Restart client|


|parameter: sync_period_in_second||
|--- |--- |
|Description |The period time of sync process, the time unit is second. |
|Type|Int : [0,2147483647]|
|Default|600|
|Modalities for Entry into Force after Modification|Restart client|


|parameter: iotdb_schema_directory||
|--- |--- |
|Description |The absolute path of the sender's IoTDB schema file, such as `$IOTDB_HOME/data/system/schema/mlog.txt` (if the user does not manually set the path of schema metadata, the path is the default path of IoTDB engine). This parameter is not valid by default and is set manually when the user needs it. |
|Type|String|
|Modalities for Entry into Force after Modification|Restart client|

|parameter: sync_storage_groups||
|--- |--- |
|Description |This parameter represents storage groups that participate in the synchronization task, which distinguishes each storage group by comma. If the list is empty, it means that all storage groups participate in synchronization. By default, it is an empty list. |
|Type|String|
|Example|root.sg1, root.sg2|
|Modalities for Entry into Force after Modification|Restart client|


|parameter: max_number_of_sync_file_retry||
|--- |--- |
|Description |The maximum number of retry when syncing a file to receiver fails. |
|Type|Int : [0,2147483647]|
|Example|5|
|Modalities for Entry into Force after Modification|Restart client|


### Usage
#### Start Sync Receiver
1. Set up parameters of sync receiver. For example:

```
	####################
	### Sync Server Configuration
	####################
	# Whether to open the sync_server_port for receiving data from sync client, the default is closed
	is_sync_enable=false
	# Sync server port to listen
	sync_server_port=5555
	# White IP list of Sync client.
	# Please use the form of network segment to present the range of IP, for example: 192.168.0.0/16
	# If there are more than one IP segment, please separate them by commas
	# The default is to allow all IP to sync
	ip_white_list=0.0.0.0/0
```

2. Start IoTDB engine, and the sync receiver will start at the same time, and the LOG log will start with the sentence `IoTDB: start SYNC ServerService successfully` indicating the successful start of the return receiver.


#### Stop Sync Receiver
Stop IoTDB and the sync receiver will be closed at the same time.

#### Start Sync Sender
1. Set up parameters of sync sender. For example:

```
	# Sync receiver server address
	server_ip=127.0.0.1
	# Sync receiver server port
	server_port=5555
	# The period time of sync process, the time unit is second.
	sync_period_in_second=600
	# This parameter represents storage groups that participate in the synchronization task, which distinguishes each storage group by comma.
	# If the list is empty, it means that all storage groups participate in synchronization.
	# By default, it is empty list.
	# sync_storage_groups = root.sg1, root.sg2
	# The maximum number of retry when syncing a file to receiver fails.
	max_number_of_sync_file_retry=5
```

2. Start sync sender
Users can use the scripts under the ```$IOTDB_HOME/tools``` folder to start the sync sender.
For Linux and Mac OS X users:
```
  Shell >$IOTDB_HOME/tools/start-sync-client.sh
```
For Windows users:
```
  Shell >$IOTDB_HOME\tools\start-sync-client.bat
```


#### Stop Sync Sender
Users can use the scripts under the ```$IOTDB_HOME/tools``` folder to stop the sync sender.
For Linux and Mac OS X users:
```
  Shell >$IOTDB_HOME/tools/stop-sync-client.sh
```
For Windows users:
```
  Shell >$IOTDB_HOME\tools\stop-sync-client.bat
```

