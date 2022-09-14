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

## 端云协同 

### TsFile 同步工具

#### 介绍

同步工具是定期将本地磁盘中新增的已持久化的 tsfile 文件上传至云端并加载到 IoTDB 的套件工具。

在同步工具的发送端，同步模块是一个独立的进程，独立于本地的 IoTDB。通过独立的脚本进行启动和关闭（详见本章节`使用方式`)，同步的频率周期可由用户设置。

在同步工具的接收端，同步模块内嵌于 IoTDB 的引擎，和 IoTDB 处于同一个进程中。同步模块监听一个独立的端口，该端口可由用户设置（详见本章节`配置参数`)。用户使用前，需要在同步接收端设置同步白名单，以网段形式表示，接收端的同步模块只接受位于白名单网段中的发送端同步的数据。

同步工具具有多对一的发送-接受模式，即一个同步接收端可以同时接受多个同步发送端传输的数据，一个同步发送端只能向一个同步接收端发送数据。

> 注意：在使用同步工具前，同步工具的接收端和发送端需要单独配置。

#### 应用场景

以一个工厂应用为例，通常有多个分厂和多个总厂，每个分厂中使用一个 IoTDB 实例收集数据，然后将数据定时汇总到总厂中进行备份或者分析等，一个总厂可以接收来自多个分厂的数据，在这种场景下每个 IoTDB 实例所管理的设备各不相同。

在 sync 模块中，每个分厂是发送端，总厂是接收端，发送端定时将数据同步给接收端，在上述应用场景下一个设备的数据只能由一个发送端来收集，因此多个发送端同步的数据之间必须是没有设备重叠的，否则不符合 sync 功能的应用场景。

当出现异常场景时，即两个或两个以上的发送端向同一个接收端同步相同设备（其存储组设为 root.sg) 的数据时，后被接收端收到的含有该设备数据的发送端的 root.sg 数据将会被拒绝接收。示例：发送端 1 向接收端同步存储组 root.sg1 和 root.sg2, 发送端 2 向接收端同步存储组 root.sg2 和 root.sg3, 
均包括时间序列 root.sg2.d0.s0, 若接收端先接收到发送端 1 的 root.sg2.d0.s0 的数据，那么接收端将拒绝发送端 2 的 root.sg2 同步的数据。

#### 注意事项

sync功能开启之后，发送端的 alter timeseries add tag 语句将不会被同步到接收端

#### 配置参数

##### 同步工具接收端
同步工具接收端的参数配置位于 IoTDB 的配置文件 iotdb-engine.properties 中，其安装目录为$IOTDB_HOME/conf/iotdb-engine.properties。在该配置文件中，有四个参数和同步接收端有关，配置说明如下：

|参数名|is_sync_enable|
|--- |--- |
|描述|同步功能开关，配置为 true 表示接收端允许接收同步的数据并加载，设置为 false 的时候表示接收端不允许接收同步的数据|
|类型|Boolean|
|默认值|false|
|改后生效方式|重启服务生效|

|参数名|ip_white_list|
|--- |--- |
|描述|设置同步功能发送端 IP 地址的白名单，以网段的形式表示，多个网段之间用逗号分隔。发送端向接收端同步数据时，只有当该发送端 IP 地址处于该白名单设置的网段范围内，接收端才允许同步操作。如果白名单为空，则接收端不允许任何发送端同步数据。默认接收端接受全部 IP 的同步请求。|
|类型|String|
|默认值|0.0.0.0/0|
|改后生效方式|重启服务生效|

|参数名|sync_server_port|
|--- |--- |
|描述|同步接收端服务器监听接口，请确认该端口不是系统保留端口并且未被占用。参数 is_sync_enable 设置为 true 时有效，参数 is_sync_enable 设置为 false 时无效|
|类型|Short Int : [0,65535]|
|默认值|5555|
|改后生效方式|重启服务生效|

##### 同步工具发送端
同步功能发送端的参数配置在一个单独的配置文件中，其安装目录为```$IOTDB_HOME/conf/iotdb-sync-client.properties```。在该配置文件中，有五个参数和同步发送端有关，配置说明如下：

|参数名|server_ip|
|--- |--- |
|描述|同步接收端的 IP 地址|
|类型|String|
|默认值|127.0.0.1|
|改后生效方式|重启同步功能发送端生效|

|参数名|server_port|
|--- |--- |
|描述|同步接收端服务器监听端口，需要保证该端口和同步接收端配置的监听端口一致|
|类型|Short Int : [0,65535]|
|默认值|5555|
|改后生效方式|重启同步功能发送端生效|

|参数名|sync_period_in_second|
|--- |--- |
|描述|同步周期，两次同步任务开始时间的间隔，单位为秒 (s)|
|类型|Int : [0,2147483647]|
|默认值|600|
|改后生效方式|重启同步功能发送端生效|

|参数名|sync_storage_groups|
|--- |--- |
|描述|进行同步的存储组列表，存储组间用逗号分隔；若列表设置为空表示同步所有存储组，默认为空|
|类型|String|
|示例|root.sg1, root.sg2|
|改后生效方式|重启同步功能发送端生效|

|参数名|max_number_of_sync_file_retry|
|--- |--- |
|描述|发送端同步文件到接收端失败时的最大重试次数|
|类型|Int : [0,2147483647]|
|示例|5|
|改后生效方式|重启同步功能发送端生效|

#### 使用方式

##### 启动同步功能接收端

1. 配置接收端的参数，例如：

```
	####################
	### Sync Server Configuration
	####################

	# Whether to open the sync_server_port for receiving data from sync client, the default is closed
	is_sync_enable=true

	# Sync server port to listen
	sync_server_port=5555

	# White IP list of Sync client.
	# Please use the form of network segment to present the range of IP, for example: 192.168.0.0/16
	# If there are more than one IP segment, please separate them by commas
	# The default is to allow all IP to sync
	ip_white_list=0.0.0.0/0
```

2. 启动 IoTDB 引擎，同步功能接收端会同时启动，启动时 LOG 日志会出现`IoTDB: start SYNC ServerService successfully`字样，表示同步接收端启动成功。

##### 关闭同步功能接收端

关闭 IoTDB，同步功能接收端会同时关闭。

##### 启动同步功能发送端
1. 配置发送端的参数

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
2. 启动同步功能发送端

用户可以使用```$IOTDB_HOME/tools```文件夹下的脚本启动同步功能的发送端
Linux 系统与 MacOS 系统启动命令如下：
```
  Shell >$IOTDB_HOME/tools/start-sync-client.sh
```
Windows 系统启动命令如下：
```
  Shell >$IOTDB_HOME\tools\start-sync-client.bat
```

##### 关闭同步功能发送端

用户可以使用```$IOTDB_HOME/tools```文件夹下的脚本关闭同步功能的发送端。
Linux 系统与 MacOS 系统停止命令如下：
```
  Shell >$IOTDB_HOME/tools/stop-sync-client.sh
```
Windows 系统停止命令如下：
```
  Shell >$IOTDB_HOME\tools\stop-sync-client.bat
```
