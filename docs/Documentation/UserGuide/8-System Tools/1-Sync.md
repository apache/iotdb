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

# Chapter 8: System Tools

## Data Import

<!-- TOC -->

- [Chapter 7: System Tools](#chapter-7-system-tools)
    - [Data Import](#data-import)
- [Introduction](#introduction)
- [Configuration](#configuration)
    - [Sync Receiver](#sync-receiver)
    - [Sync Sender](#sync-sender)
- [Usage](#usage)
    - [Start Sync Receiver](#start-sync-receiver)
    - [Stop Sync Receiver](#stop-sync-receiver)
    - [Start Sync Sender](#start-sync-sender)
    - [Stop Sync Sender](#stop-sync-sender)

<!-- /TOC -->
# Introduction
The Sync Tool is an IoTDB suite tool that periodically uploads persistent tsfiles from the sender disk to the receiver and loads them.

On the sender side of the sync, the sync module is a separate process, independent of the IoTDB process. It can be started and closed through a separate script (see Sections `Usage` for details). The frequency cycle of the sync can be set by the user. 

On the receiver side of the sync, the sync module is embedded in the engine of IoTDB and is in the same process with IoTDB. The receiver module listens for a separate port, which can be set by the user (see Section `Configuration` for details). Before using it, it needs to set up a whitelist at the sync receiver, which is expressed as a network segment. The receiver only accepts the data transferred from the sender located in the whitelist segment, as detailed in Section `Configuration`. 

The sync tool has a many-to-one sender-receiver mode - that is, one sync receiver can receive data from multiple sync senders simultaneously while one sync sender can only send data to one sync receiver.

> Note: Before using the sync tool, the client and server need to be configured separately. The configuration is detailed in Sections Configuration.
# Configuration
## Sync Receiver
The parameter configuration of the sync receiver is located in the configuration file `iotdb-engine.properties` of IoTDB, and its directory is `$IOTDB_HOME/conf/iotdb-engine.properties`. In this configuration file, there are four parameters related to the sync receiver. The configuration instructions are as follows:

<table>
   <tr>
      <td colspan="2">parameter: is_sync_enable</td>
   </tr>
   <tr>
      <td width="30%">Description</td>
      <td>Sync function switch, which is configured as true to indicate that the receiver is allowed to receive the data from the sender and load it. When set to false, it means that the receiver is not allowed to receive the data from any sender. </td>
   </tr>
   <tr>
      <td>Type</td>
      <td>Boolean</td>
   </tr>
   <tr>
      <td>Default</td>
      <td>false</td>
   </tr>
   <tr>
      <td>Modalities for Entry into Force after Modification</td>
      <td>Restart receiver</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">parameter: IP_white_list</td>
   </tr>
   <tr>
      <td width="30%">Description</td>
      <td>Set up a white list of sender IP addresses, which is expressed in the form of network segments and separated by commas between multiple network segments. When the sender transfers data to the receiver, only when the IP address of the sender is within the network segment set by the whitelist can the receiver allow the sync operation. If the whitelist is empty, the receiver does not allow any sender to sync data. The default receiver accepts all IP sync requests.</td>
   </tr>
   <tr>
      <td>Type</td>
      <td>String</td>
   </tr>
   <tr>
      <td>Default</td>
      <td>0.0.0.0/0</td>
   </tr>
   <tr>
      <td>Modalities for Entry into Force after Modification</td>
      <td>Restart receiver</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">parameter: update_historical_data_possibility</td>
   </tr>
   <tr>
      <td width="30%">Description</td>
      <td>The processing strategy chosen by the sync receiver when merging the sync data.<br/>
        1. If the sync data accounts for more than 50% of the update of the historical data (compared with the latest timestamp of the local storage group data),then it is recommended this parameter be set to TRUE, which has a greater impact on the write performance and reduce CPU usage.<br/>
        2. If the sync data accounts for less than 50% of the update of the historical data (compared with the latest timestamp of the local storage group data),then it is recommended this parameter be set to FALSE,which has little impact on the write performance and takes up a large amount of CPU power.<br/>
</td>
   </tr>
   <tr>
      <td>Type</td>
      <td>Boolean</td>
   </tr>
   <tr>
      <td>Default</td>
      <td>false</td>
   </tr>
   <tr>
      <td>Modalities for Entry into Force after Modification</td>
      <td>Restart receiver</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">parameter: sync_server_port</td>
   </tr>
   <tr>
      <td width="30%">Description</td>
      <td>Sync receiver port to listen. Make sure that the port is not a system reserved port and is not occupied. This paramter is valid only when the parameter is_sync_enable is set to TRUE.</td>
   </tr>
   <tr>
      <td>Type</td>
      <td>Short Int : [0,65535]</td>
   </tr>
   <tr>
      <td>Default</td>
      <td>5555</td>
   </tr>
   <tr>
      <td>Modalities for Entry into Force after Modification</td>
      <td>Restart receiver</td>
   </tr>
</table>

## Sync Sender
The parameters of the sync sender are configured in a separate configuration file iotdb-sync-client.properties with the installation directory of ```$IOTDB_HOME/conf/iotdb-sync-client.properties```. In this configuration file, there are five parameters related to the sync sender. The configuration instructions are as follows:
<table>
   <tr>
      <td colspan="2">parameter: server_ip</td>
   </tr>
   <tr>
      <td width="30%">Description</td>
      <td>IP address of sync receiver.</td>
   </tr>
   <tr>
      <td>Type</td>
      <td>String</td>
   </tr>
   <tr>
      <td>Default</td>
      <td>127.0.0.1</td>
   </tr>
   <tr>
      <td>Modalities for Entry into Force after Modification</td>
      <td>Restart client</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">parameter: server_port</td>
   </tr>
   <tr>
      <td width="30%">Description</td>
      <td>Listening port of sync receiver, it is necessary to ensure that the port is consistent with the configuration of the listening port set in receiver.</td>
   </tr>
   <tr>
      <td>Type</td>
      <td>Short Int : [0,65535]</td>
   </tr>
   <tr>
      <td>Default</td>
      <td>5555</td>
   </tr>
   <tr>
      <td>Modalities for Entry into Force after Modification</td>
      <td>Restart client</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">parameter: sync_period_in_second</td>
   </tr>
   <tr>
      <td width="30%">Description</td>
      <td>The period time of sync process, the time unit is second.</td>
   </tr>
   <tr>
      <td>Type</td>
      <td>Int : [0,2147483647]</td>
   </tr>
   <tr>
      <td>Default</td>
      <td>600</td>
   </tr>
   <tr>
      <td>Modalities for Entry into Force after Modification</td>
      <td>Restart client</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">parameter: iotdb_schema_directory</td>
   </tr>
   <tr>
      <td width="30%">Description</td>
      <td>The absolute path of the sender's IoTDB schema file, such as $IOTDB_HOME/data/system/schema/mlog.txt (if the user does not manually set the path of schema metadata, the path is the default path of IoTDB engine). This parameter is not valid by default and is set manually when the user needs it.</td>
   </tr>
   <tr>
      <td>Type</td>
      <td>String</td>
   </tr>
   <tr>
      <td>Modalities for Entry into Force after Modification</td>
      <td>Restart client</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">parameter: iotdb_bufferWrite_directory</td>
   </tr>
   <tr>
      <td width="30%">Description</td>
      <td>The absolute path of the buffer write data (tsfile file) directory of the IoTDB at the sender, such as: $IOTDB_HOME/data/data/settled (if the user does not set the data path manually, the path is the default path of IoTDB engine). This parameter is not valid by default, and is set manually when the user needs it. This parameter needs to be guaranteed to belong to the same IoTDB as the parameter iotdb_schema_directory.</td>
   </tr>
   <tr>
      <td>Type</td>
      <td>String</td>
   </tr>
   <tr>
      <td>Modalities for Entry into Force after Modification</td>
      <td>Restart client</td>
   </tr>
</table>

# Usage
## Start Sync Receiver
1. Set up parameters of sync receiver. For example:
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/26211279/59494502-daaa4380-8ebf-11e9-8bce-363e2433005a.png">
2. Start IoTDB engine, and the sync receiver will start at the same time, and the LOG log will start with the sentence `IoTDB: start SYNC ServerService successfully` indicating the successful start of the return receiver.
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/26211279/59494513-df6ef780-8ebf-11e9-83e1-ee8ae64b76d0.png">

## Stop Sync Receiver
Stop IoTDB and the sync receiver will be closed at the same time.

## Start Sync Sender
1. Set up parameters of sync sender. For example:
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/26211279/59494559-f9a8d580-8ebf-11e9-875e-355199c1a1e9.png">
2. Start sync sender
Users can use the scripts under the ```$IOTDB_HOME/bin``` folder to start the sync sender.
For Linux and Mac OS X users:
```
  Shell >$IOTDB_HOME/bin/start-sync-client.sh
```
For Windows users:
```
  Shell >$IOTDB_HOME/bin/start-sync-client.bat
```
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/26211279/59494951-dc283b80-8ec0-11e9-9575-5d8578c08ceb.png">

## Stop Sync Sender
Users can use the scripts under the ```$IOTDB_HOME/bin``` folder to stop the sync sender.
For Linux and Mac OS X users:
```
  Shell >$IOTDB_HOME/bin/stop-sync-client.sh
```
For Windows users:
```
  Shell >$IOTDB_HOME/bin/stop-sync-client.bat
```