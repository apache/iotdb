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

# 第9章: 同步工具
<!-- TOC -->

- [同步工具](#同步工具)
- [介绍](#介绍)
- [配置参数](#配置参数)
    - [同步工具接收端](#同步工具接收端)
    - [同步工具发送端](#同步工具发送端)
- [使用方式](#使用方式)
    - [启动同步功能接收端](#启动同步功能接收端)
    - [关闭同步功能接收端](#关闭同步功能接收端)
    - [启动同步功能发送端](#启动同步功能发送端)
    - [关闭同步功能发送端](#关闭同步功能发送端)

<!-- /TOC -->
# 介绍
同步工具是定期将本地磁盘中和新增的已持久化的tsfile文件上传至云端并加载到IoTDB套件工具。

在同步工具的发送端，同步模块是一个独立的进程，独立于本地的IoTDB。通过独立的脚本进行启动和关闭(详见章节`使用方式`)，同步的频率周期可由用户设置。

在同步工具的的接收端，同步模块内嵌于IoTDB的引擎，和IoTDB处于同一个进程中。同步模块监听一个独立的端口，该端口可由用户设置(详见章节`配置参数`)。用户使用前，需要在同步接收端设置同步白名单，以网段形式表示，接收端的同步模块只接受位于白名单网段中的发送端同步的数据. 

同步工具具有多对一的发送-接受模式，即一个同步接收端可以同时接受多个同步发送端传输的数据，一个同步发送端只能向一个同步接收端发送数据

> 注意：在使用同步工具前，同步工具的接收端和发送端需要单独配置。
# 配置参数
## 同步工具接收端
同步工具接收端的参数配置位于IoTDB的配置文件iotdb-engine.properties中，其安装目录为$IOTDB_HOME/conf/iotdb-engine.properties。在该配置文件中，有四个参数和同步接收端有关，配置说明如下:

<table>
   <tr>
      <td colspan="2">参数名: is_sync_enable</td>
   </tr>
   <tr>
      <td width="20%">描述</td>
      <td>同步功能开关，配置为true表示接收端允许接收同步的数据并加载，设置为false的时候表示接收端不允许接收同步的数据</td>
   </tr>
   <tr>
      <td>类型</td>
      <td>Boolean</td>
   </tr>
   <tr>
      <td>默认值</td>
      <td>false</td>
   </tr>
   <tr>
      <td>改后生效方式</td>
      <td>重启服务器生效</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">参数名: IP_white_list</td>
   </tr>
   <tr>
      <td width="20%">描述</td>
      <td>设置同步功能发送端IP地址的白名单，以网段的形式表示，多个网段之间用逗号分隔。发送端向接收端同步数据时，只有当该发送端IP地址处于该白名单设置的网段范围内，接收端才允许同步操作。如果白名单为空，则接收端不允许任何发送端同步数据。默认接收端接受全部IP的同步请求。</td>
   </tr>
   <tr>
      <td>类型</td>
      <td>String</td>
   </tr>
   <tr>
      <td>默认值</td>
      <td>0.0.0.0/0</td>
   </tr>
   <tr>
      <td>改后生效方式</td>
      <td>重启服务器生效</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">参数名: update_historical_data_possibility</td>
   </tr>
   <tr>
      <td width="20%">描述</td>
      <td>同步服务端在合并同步的数据时选择的处理策略。如果同步的数据对历史数据(相比本地该存储组数据的最新时间戳)更新占比超过50%，则建议选择策略1，将参数设置为true，使用该策略对IoTDB系统的写入性能产生较大影响，对机器的CPU占用较小；如果同步的数据对历史数据更新占比少于50%，则建议选择策略2，将参数设置为false，使用该策略对IoTDB系统的写入性能产生较小影响，对机器CPU的占用较大。<br/>
</td>
   </tr>
   <tr>
      <td>类型</td>
      <td>Boolean</td>
   </tr>
   <tr>
      <td>默认值</td>
      <td>false</td>
   </tr>
   <tr>
      <td>改后生效方式</td>
      <td>重启服务器生效</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">参数名: sync_server_port</td>
   </tr>
   <tr>
      <td width="20%">描述</td>
      <td>同步接收端服务器监听接口，请确认该端口不是系统保留端口并且未被占用。参数is_sync_enable设置为true时有效，参数is_sync_enable设置为false时无效</td>
   </tr>
   <tr>
      <td>类型</td>
      <td>Short Int : [0,65535]</td>
   </tr>
   <tr>
      <td>默认值</td>
      <td>5555</td>
   </tr>
   <tr>
      <td>改后生效方式</td>
      <td>重启服务器生效</td>
   </tr>
</table>

## 同步工具发送端
同步功能发送端的参数配置在一个单独的配置文件中，其安装目录为```$IOTDB_HOME/conf/iotdb-sync-client.properties```。在该配置文件中，有五个参数和同步发送端有关，配置说明如下：
<table>
   <tr>
      <td colspan="2">参数名: server_ip</td>
   </tr>
   <tr>
      <td width="20%">描述</td>
      <td>同步接收端的IP地址</td>
   </tr>
   <tr>
      <td>类型</td>
      <td>String</td>
   </tr>
   <tr>
      <td>默认值</td>
      <td>127.0.0.1</td>
   </tr>
   <tr>
      <td>改后生效方式</td>
      <td>重启同步功能发送端生效</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">参数名: server_port</td>
   </tr>
   <tr>
      <td width="20%">描述</td>
      <td>同步接收端服务器监听端口，需要保证该端口和同步接收端配置的监听端口一致</td>
   </tr>
   <tr>
      <td>类型</td>
      <td>Short Int : [0,65535]</td>
   </tr>
   <tr>
      <td>默认值</td>
      <td>5555</td>
   </tr>
   <tr>
      <td>改后生效方式</td>
      <td>重启同步功能发送端生效</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">参数名: sync_period_in_second</td>
   </tr>
   <tr>
      <td width="20%">描述</td>
      <td>同步周期，两次同步任务开始时间的间隔，单位为秒(s)</td>
   </tr>
   <tr>
      <td>类型</td>
      <td>Int : [0,2147483647]</td>
   </tr>
   <tr>
      <td>默认值</td>
      <td>600</td>
   </tr>
   <tr>
      <td>改后生效方式</td>
      <td>重启同步功能发送端生效</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">参数名: iotdb_schema_directory</td>
   </tr>
   <tr>
      <td width="20%">描述</td>
      <td>同步发送端的IoTDB schema文件的绝对路径，例如$IOTDB_HOME /data/system/schema/mlog.txt(若用户未手动设置schema元数据的路径，则该路径为默认路径)，该参数默认不生效，用户有需求时进行手动设置</td>
   </tr>
   <tr>
      <td>类型</td>
      <td>String</td>
   </tr>
   <tr>
      <td>改后生效方式</td>
      <td>重启同步功能发送端生效</td>
   </tr>
</table>

<table>
   <tr>
      <td colspan="2">参数名: iotdb_bufferWrite_directory</td>
   </tr>
   <tr>
      <td width="20%">描述</td>
      <td>同步发送端的IoTDB 的bufferWrite数据(tsfile文件)目录的绝对路径，定位至bufferWrite目录下，例如： $IOTDB_HOME /data/data/settled(若用户未手动设置数据路径，则该路径为默认路径)，该参数默认不生效，用户有需求时进行手动设置。该参数需要保证和参数iotdb_schema_directory属于同一个IoTDB</td>
   </tr>
   <tr>
      <td>类型</td>
      <td>String</td>
   </tr>
   <tr>
      <td>改后生效方式</td>
      <td>重启同步功能发送端生效</td>
   </tr>
</table>

# 使用方式
## 启动同步功能接收端
1. 配置接收端的参数，例如：
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/26211279/59494502-daaa4380-8ebf-11e9-8bce-363e2433005a.png">
2. 启动IoTDB引擎，同步功能接收端会同时启动，启动时LOG日志会出现`IoTDB: start SYNC ServerService successfully`字样，表示同步接收端启动成功，如图所示：
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/26211279/59494513-df6ef780-8ebf-11e9-83e1-ee8ae64b76d0.png">

## 关闭同步功能接收端
关闭IoTDB，同步功能接收端会同时关闭。

## 启动同步功能发送端
1. 配置发送端的参数， 如图所示:
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/26211279/59494559-f9a8d580-8ebf-11e9-875e-355199c1a1e9.png">
2. 启动同步功能发送端

用户可以使用```$IOTDB_HOME/bin```文件夹下的脚本启动同步功能的发送端
Linux系统与MacOS系统启动命令如下：
```
  Shell >$IOTDB_HOME/bin/start-sync-client.sh
```
Windows系统启动命令如下：
```
  Shell >$IOTDB_HOME\bin\start-sync-client.bat
```
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/26211279/59494951-dc283b80-8ec0-11e9-9575-5d8578c08ceb.png">

## 关闭同步功能发送端

用户可以使用```$IOTDB_HOME/bin```文件夹下的脚本关闭同步功能的发送端。
Linux系统与MacOS系统停止命令如下：
```
  Shell >$IOTDB_HOME/bin/stop-sync-client.sh
```
Windows系统停止命令如下：
```
  Shell >$IOTDB_HOME\bin\stop-sync-client.bat
```

