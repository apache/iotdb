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
# 第4章 客户端
## 概览
- Cli / Shell工具
    - Cli / Shell安装
    - Cli / Shell运行方式
    - Cli / Shell运行参数
    - Cli / Shell的-e参数

<!-- /TOC -->

# 命令行接口(CLI)
IOTDB为用户提供Client/Shell工具用于启动客户端和服务端程序。下面介绍每个Client/Shell工具的运行方式和相关参数。
> \$IOTDB\_HOME表示IoTDB的安装目录所在路径。

## Cli / Shell安装
在incubator-iotdb的根目录下执行

```
> mvn clean package -pl client -am -DskipTests
```

在生成完毕之后，IoTDB的cli工具位于文件夹"client/target/iotdb-client-{project.version}"中。

## Cli  / Shell运行方式
安装后的IoTDB中有一个默认用户：`root`，默认密码为`root`。用户可以使用该用户尝试运行IoTDB客户端以测试服务器是否正常启动。客户端启动脚本为$IOTDB_HOME/bin文件夹下的`start-client`脚本。启动脚本时需要指定运行IP和PORT。以下为服务器在本机启动，且用户未更改运行端口号的示例，默认端口为6667。若用户尝试连接远程服务器或更改了服务器运行的端口号，请在-h和-p项处使用服务器的IP和PORT。</br>
用户也可以在启动脚本的最前方设置自己的环境变量，如JAVA_HOME等 (对于linux用户，脚本路径为："/sbin/start-client.sh"； 对于windows用户，脚本路径为："/sbin/start-client.bat")



Linux系统与MacOS系统启动命令如下：

```
  Shell > ./sbin/start-client.sh -h 127.0.0.1 -p 6667 -u root -pw root
```
Windows系统启动命令如下：

```
  Shell > \sbin\start-client.bat -h 127.0.0.1 -p 6667 -u root -pw root
```
回车后即可成功启动客户端。启动后出现如图提示即为启动成功。
```
 _____       _________  ______   ______
|_   _|     |  _   _  ||_   _ `.|_   _ \
  | |   .--.|_/ | | \_|  | | `. \ | |_) |
  | | / .'`\ \  | |      | |  | | |  __'.
 _| |_| \__. | _| |_    _| |_.' /_| |__) |
|_____|'.__.' |_____|  |______.'|_______/  version <version>


IoTDB> login successfully
IoTDB>
```
输入`quit`或`exit`可退出Client结束本次会话，Client输出`quit normally`表示退出成功。

## Cli / Shell运行参数

|参数名|参数类型|是否为必需参数| 说明| 例子 |
|:---|:---|:---|:---|:---|
|-disableIS08601 |没有参数 | 否 |如果设置了这个参数，IoTDB将以数字的形式打印时间戳(timestamp)。|-disableIS08601|
|-h <`host`> |string类型，不需要引号|是|IoTDB客户端连接IoTDB服务器的IP地址。|-h 10.129.187.21|
|-help|没有参数|否|打印IoTDB的帮助信息|-help|
|-p <`port`>|int类型|是|IoTDB连接服务器的端口号，IoTDB默认运行在6667端口。|-p 6667|
|-pw <`password`>|string类型，不需要引号|否|IoTDB连接服务器所使用的密码。如果没有输入密码IoTDB会在Cli端提示输入密码。|-pw root|
|-u <`username`>|string类型，不需要引号|是|IoTDB连接服务器锁使用的用户名。|-u root|
|-maxPRC <`maxPrintRowCount`>|int类型|否|设置IoTDB返回客户端命令行中所显示的最大行数。|-maxPRC 10|
|-e <`execute`> |string类型|否|在不进入客户端输入模式的情况下，批量操作IoTDB|-e "show storage group"|


下面展示一条客户端命令，功能是连接IP为10.129.187.21的主机，端口为6667 ，用户名为root，密码为root，以数字的形式打印时间戳，IoTDB命令行显示的最大行数为10。

Linux系统与MacOS系统启动命令如下：

```
  Shell >./sbin/start-client.sh -h 10.129.187.21 -p 6667 -u root -pw root -disableIS08601 -maxPRC 10
```
Windows系统启动命令如下：

```
  Shell > \sbin\start-client.bat -h 10.129.187.21 -p 6667 -u root -pw root -disableIS08601 -maxPRC 10
```
## Cli / Shell的-e参数
当您想要通过脚本的方式通过Cli / Shell对IoTDB进行批量操作时，可以使用-e参数。通过使用该参数，您可以在不进入客户端输入模式的情况下操作IoTDB。

为了避免SQL语句和其他参数混淆，现在只支持-e参数作为最后的参数使用。

针对Client/Shell工具的-e参数用法如下：

```
  Shell > ./sbin/start-client.sh -h {host} -p {port} -u {user} -pw {password} -e {sql for iotdb}
```

为了更好的解释-e参数的使用，可以参考下面的例子。

假设用户希望对一个新启动的IoTDB进行如下操作：

1.创建名为root.demo的存储组

2.创建名为root.demo.s1的时间序列

3.向创建的时间序列中插入三个数据点

4.查询验证数据是否插入成功

那么通过使用Client/Shell工具的-e参数，可以采用如下的脚本：

```
# !/bin/bash

host=127.0.0.1
port=6667
user=root
pass=root

./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "set storage group to root.demo"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "create timeseries root.demo.s1 WITH DATATYPE=INT32, ENCODING=RLE"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(1,10)"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(2,11)"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(3,12)"
./sbin/start-client.sh -h ${host} -p ${port} -u ${user} -pw ${pass} -e "select s1 from root.demo"
```

打印出来的结果显示在下图，通过这种方式进行的操作与客户端的输入模式以及通过JDBC进行操作结果是一致的。

![img](https://issues.apache.org/jira/secure/attachment/12976042/12976042_image-2019-07-27-15-47-12-045.png)

需要特别注意的是，在脚本中使用-e参数时要对特殊字符进行转义。