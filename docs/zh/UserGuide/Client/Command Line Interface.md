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

# 命令行接口(CLI)

## 概览

- Cli / Shell工具
  - Cli / Shell安装
  - Cli / Shell运行方式
  - Cli / Shell运行参数
  - Cli / Shell的-e参数

<!-- /TOC -->

IOTDB为用户提供cli/Shell工具用于启动客户端和服务端程序。下面介绍每个cli/Shell工具的运行方式和相关参数。
> \$IOTDB\_HOME表示IoTDB的安装目录所在路径。

## Cli / Shell安装
在iotdb的根目录下执行

```
> mvn clean package -pl cli -am -DskipTests
```

在生成完毕之后，IoTDB的cli工具位于文件夹"cli/target/iotdb-cli-{project.version}"中。

## Cli  / Shell运行方式
安装后的IoTDB中有一个默认用户：`root`，默认密码为`root`。用户可以使用该用户尝试运行IoTDB客户端以测试服务器是否正常启动。客户端启动脚本为$IOTDB_HOME/bin文件夹下的`start-cli`脚本。启动脚本时需要指定运行IP和RPC PORT。以下为服务器在本机启动，且用户未更改运行端口号的示例，默认端口为6667。若用户尝试连接远程服务器或更改了服务器运行的端口号，请在-h和-p项处使用服务器的IP和RPC PORT。</br>
用户也可以在启动脚本的最前方设置自己的环境变量，如JAVA_HOME等 (对于linux用户，脚本路径为："/sbin/start-cli.sh"； 对于windows用户，脚本路径为："/sbin/start-cli.bat")



Linux系统与MacOS系统启动命令如下：

```
  Shell > sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root
```
Windows系统启动命令如下：

```
  Shell > sbin\start-cli.bat -h 127.0.0.1 -p 6667 -u root -pw root
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
输入`quit`或`exit`可退出cli结束本次会话，cli输出`quit normally`表示退出成功。

## Cli / Shell运行参数

|参数名|参数类型|是否为必需参数| 说明| 例子 |
|:---|:---|:---|:---|:---|
|-disableISO8601 |没有参数 | 否 |如果设置了这个参数，IoTDB将以数字的形式打印时间戳(timestamp)。|-disableISO8601|
|-h <`host`> |string类型，不需要引号|是|IoTDB客户端连接IoTDB服务器的IP地址。|-h 10.129.187.21|
|-help|没有参数|否|打印IoTDB的帮助信息|-help|
|-p <`rpcPort`>|int类型|是|IoTDB连接服务器的端口号，IoTDB默认运行在6667端口。|-p 6667|
|-pw <`password`>|string类型，不需要引号|否|IoTDB连接服务器所使用的密码。如果没有输入密码IoTDB会在Cli端提示输入密码。|-pw root|
|-u <`username`>|string类型，不需要引号|是|IoTDB连接服务器锁使用的用户名。|-u root|
|-maxPRC <`maxPrintRowCount`>|int类型|否|设置IoTDB返回客户端命令行中所显示的最大行数。|-maxPRC 10|
|-e <`execute`> |string类型|否|在不进入客户端输入模式的情况下，批量操作IoTDB|-e "show storage group"|
|-c | 空 | 否 | 如果服务器设置了 `rpc_thrift_compression_enable=true`, 则CLI必须使用 `-c` | -c |

下面展示一条客户端命令，功能是连接IP为10.129.187.21的主机，端口为6667 ，用户名为root，密码为root，以数字的形式打印时间戳，IoTDB命令行显示的最大行数为10。

Linux系统与MacOS系统启动命令如下：

```
  Shell > sbin/start-cli.sh -h 10.129.187.21 -p 6667 -u root -pw root -disableISO8601 -maxPRC 10
```
Windows系统启动命令如下：

```
  Shell > sbin\start-cli.bat -h 10.129.187.21 -p 6667 -u root -pw root -disableISO8601 -maxPRC 10
```

## 使用OpenID作为用户名认证登录

若对应的IoTDB服务器开启了使用OpenID Connect (OIDC)作为权限认证服务，那么就不再需要使用用户名密码进行登录。
替而代之的是使用Token，以及空密码。
此时，登录命令如下：

```
  Shell > sbin/start-cli.sh -h 10.129.187.21 -p 6667 -u {my-access-token} -pw ""
```

其中，需要将{my-access-token} （注意，包括{}）替换成你的token。

如何获取token取决于你的OIDC设置。 最简单的一种情况是使用`password-grant`。例如，假设你在用keycloack作为你的OIDC服务，
并且你在keycloack中有一个被定义成publich的`iotdb`客户的realm，那么你可以使用如下`curl`命令获得token。
（注意例子中的{}和里面的内容需要替换成具体的服务器地址和realm名字）： 
```
curl -X POST "https://{your-keycloack-server}/auth/realms/{your-realm}/protocol/openid-connect/token" \                                                                                                                       ✔  1613  11:09:38
 -H "Content-Type: application/x-www-form-urlencoded" \
 -d "username={username}" \
 -d "password={password}" \
 -d 'grant_type=password' \
 -d "client_id=iotdb"
```

示例结果如下：

```json
{"access_token":"eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJxMS1XbTBvelE1TzBtUUg4LVNKYXAyWmNONE1tdWNXd25RV0tZeFpKNG93In0.eyJleHAiOjE1OTAzOTgwNzEsImlhdCI6MTU5MDM5Nzc3MSwianRpIjoiNjA0ZmYxMDctN2NiNy00NTRmLWIwYmQtY2M2ZDQwMjFiNGU4IiwiaXNzIjoiaHR0cDovL2F1dGguZGVtby5wcmFnbWF0aWNpbmR1c3RyaWVzLmRlL2F1dGgvcmVhbG1zL0lvVERCIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6ImJhMzJlNDcxLWM3NzItNGIzMy04ZGE2LTZmZThhY2RhMDA3MyIsInR5cCI6IkJlYXJlciIsImF6cCI6ImlvdGRiIiwic2Vzc2lvbl9zdGF0ZSI6IjA2MGQyODYyLTE0ZWQtNDJmZS1iYWY3LThkMWY3ODQ2NTdmMSIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsibG9jYWxob3N0OjgwODAiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iLCJpb3RkYl9hZG1pbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoiZW1haWwgcHJvZmlsZSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ1c2VyIn0.nwbrJkWdCNjzFrTDwKNuV5h9dDMg5ytRKGOXmFIajpfsbOutJytjWTCB2WpA8E1YI3KM6gU6Jx7cd7u0oPo5syHhfCz119n_wBiDnyTZkFOAPsx0M2z20kvBLN9k36_VfuCMFUeddJjO31MeLTmxB0UKg2VkxdczmzMH3pnalhxqpnWWk3GnrRrhAf2sZog0foH4Ae3Ks0lYtYzaWK_Yo7E4Px42-gJpohy3JevOC44aJ4auzJR1RBj9LUbgcRinkBy0JLi6XXiYznSC2V485CSBHW3sseXn7pSXQADhnmGQrLfFGO5ZljmPO18eFJaimdjvgSChsrlSEmTDDsoo5Q","expires_in":300,"refresh_expires_in":1800,"refresh_token":"eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJhMzZlMGU0NC02MWNmLTQ5NmMtOGRlZi03NTkwNjQ5MzQzMjEifQ.eyJleHAiOjE1OTAzOTk1NzEsImlhdCI6MTU5MDM5Nzc3MSwianRpIjoiNmMxNTBiY2EtYmE5NC00NTgxLWEwODEtYjI2YzhhMmI5YmZmIiwiaXNzIjoiaHR0cDovL2F1dGguZGVtby5wcmFnbWF0aWNpbmR1c3RyaWVzLmRlL2F1dGgvcmVhbG1zL0lvVERCIiwiYXVkIjoiaHR0cDovL2F1dGguZGVtby5wcmFnbWF0aWNpbmR1c3RyaWVzLmRlL2F1dGgvcmVhbG1zL0lvVERCIiwic3ViIjoiYmEzMmU0NzEtYzc3Mi00YjMzLThkYTYtNmZlOGFjZGEwMDczIiwidHlwIjoiUmVmcmVzaCIsImF6cCI6ImlvdGRiIiwic2Vzc2lvbl9zdGF0ZSI6IjA2MGQyODYyLTE0ZWQtNDJmZS1iYWY3LThkMWY3ODQ2NTdmMSIsInNjb3BlIjoiZW1haWwgcHJvZmlsZSJ9.ayNpXdNX28qahodX1zowrMGiUCw2AodlHBQFqr8Ui7c","token_type":"bearer","not-before-policy":0,"session_state":"060d2862-14ed-42fe-baf7-8d1f784657f1","scope":"email profile"}
```

## Cli / Shell的-e参数
当您想要通过脚本的方式通过Cli / Shell对IoTDB进行批量操作时，可以使用-e参数。通过使用该参数，您可以在不进入客户端输入模式的情况下操作IoTDB。

为了避免SQL语句和其他参数混淆，现在只支持-e参数作为最后的参数使用。

针对cli/Shell工具的-e参数用法如下：

Linux系统与MacOS指令:

```
  Shell > sbin/start-cli.sh -h {host} -p {rpcPort} -u {user} -pw {password} -e {sql for iotdb}
```

Windows系统指令
```
  Shell > sbin\start-cli.bat -h {host} -p {rpcPort} -u {user} -pw {password} -e {sql for iotdb}
```

在Windows环境下，-e参数的SQL语句需要使用` `` `对于`" "`进行替换

为了更好的解释-e参数的使用，可以参考下面在Linux上执行的例子。

假设用户希望对一个新启动的IoTDB进行如下操作：

1.创建名为root.demo的存储组

2.创建名为root.demo.s1的时间序列

3.向创建的时间序列中插入三个数据点

4.查询验证数据是否插入成功

那么通过使用cli/Shell工具的-e参数，可以采用如下的脚本：

```
# !/bin/bash

host=127.0.0.1
rpcPort=6667
user=root
pass=root

./sbin/start-cli.sh -h ${host} -p ${rpcPort} -u ${user} -pw ${pass} -e "set storage group to root.demo"
./sbin/start-cli.sh -h ${host} -p ${rpcPort} -u ${user} -pw ${pass} -e "create timeseries root.demo.s1 WITH DATATYPE=INT32, ENCODING=RLE"
./sbin/start-cli.sh -h ${host} -p ${rpcPort} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(1,10)"
./sbin/start-cli.sh -h ${host} -p ${rpcPort} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(2,11)"
./sbin/start-cli.sh -h ${host} -p ${rpcPort} -u ${user} -pw ${pass} -e "insert into root.demo(timestamp,s1) values(3,12)"
./sbin/start-cli.sh -h ${host} -p ${rpcPort} -u ${user} -pw ${pass} -e "select s1 from root.demo"
```

打印出来的结果显示在下图，通过这种方式进行的操作与客户端的输入模式以及通过JDBC进行操作结果是一致的。

![img](https://issues.apache.org/jira/secure/attachment/12976042/12976042_image-2019-07-27-15-47-12-045.png)

需要特别注意的是，在脚本中使用-e参数时要对特殊字符进行转义。