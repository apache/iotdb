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

## SQL 命令行终端 (CLI)

IOTDB 为用户提供 cli/Shell 工具用于启动客户端和服务端程序。下面介绍每个 cli/Shell 工具的运行方式和相关参数。
> \$IOTDB\_HOME 表示 IoTDB 的安装目录所在路径。

### 安装
在 iotdb 的根目录下执行

```shell
> mvn clean package -pl cli -am -DskipTests
```

在生成完毕之后，IoTDB 的 cli 工具位于文件夹"cli/target/iotdb-cli-{project.version}"中。

### 运行

#### Cli 运行方式
安装后的 IoTDB 中有一个默认用户：`root`，默认密码为`root`。用户可以使用该用户尝试运行 IoTDB 客户端以测试服务器是否正常启动。客户端启动脚本为$IOTDB_HOME/sbin 文件夹下的`start-cli`脚本。启动脚本时需要指定运行 IP 和 RPC PORT。以下为服务器在本机启动，且用户未更改运行端口号的示例，默认端口为 6667。若用户尝试连接远程服务器或更改了服务器运行的端口号，请在-h 和-p 项处使用服务器的 IP 和 RPC PORT。<br>
用户也可以在启动脚本的最前方设置自己的环境变量，如 JAVA_HOME 等 （对于 linux 用户，脚本路径为："/sbin/start-cli.sh"； 对于 windows 用户，脚本路径为："/sbin/start-cli.bat")

Linux 系统与 MacOS 系统启动命令如下：

```shell
Shell > sbin/start-cli.sh -h 127.0.0.1 -p 6667 -u root -pw root
```
Windows 系统启动命令如下：

```shell
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
```
输入`quit`或`exit`可退出 cli 结束本次会话，cli 输出`quit normally`表示退出成功。

#### Cli 运行参数

|参数名|参数类型|是否为必需参数| 说明| 例子 |
|:---|:---|:---|:---|:---|
|-disableISO8601 |没有参数 | 否 |如果设置了这个参数，IoTDB 将以数字的形式打印时间戳 (timestamp)。|-disableISO8601|
|-h <`host`> |string 类型，不需要引号|是|IoTDB 客户端连接 IoTDB 服务器的 IP 地址。|-h 10.129.187.21|
|-help|没有参数|否|打印 IoTDB 的帮助信息|-help|
|-p <`rpcPort`>|int 类型|是|IoTDB 连接服务器的端口号，IoTDB 默认运行在 6667 端口。|-p 6667|
|-pw <`password`>|string 类型，不需要引号|否|IoTDB 连接服务器所使用的密码。如果没有输入密码 IoTDB 会在 Cli 端提示输入密码。|-pw root|
|-u <`username`>|string 类型，不需要引号|是|IoTDB 连接服务器锁使用的用户名。|-u root|
|-maxPRC <`maxPrintRowCount`>|int 类型|否|设置 IoTDB 返回客户端命令行中所显示的最大行数。|-maxPRC 10|
|-e <`execute`> |string 类型|否|在不进入客户端输入模式的情况下，批量操作 IoTDB|-e "show storage group"|
|-c | 空 | 否 | 如果服务器设置了 `rpc_thrift_compression_enable=true`, 则 CLI 必须使用 `-c` | -c |

下面展示一条客户端命令，功能是连接 IP 为 10.129.187.21 的主机，端口为 6667 ，用户名为 root，密码为 root，以数字的形式打印时间戳，IoTDB 命令行显示的最大行数为 10。

Linux 系统与 MacOS 系统启动命令如下：

```shell
Shell > sbin/start-cli.sh -h 10.129.187.21 -p 6667 -u root -pw root -disableISO8601 -maxPRC 10
```
Windows 系统启动命令如下：

```shell
Shell > sbin\start-cli.bat -h 10.129.187.21 -p 6667 -u root -pw root -disableISO8601 -maxPRC 10
```

#### 使用 OpenID 作为用户名认证登录

OpenID Connect (OIDC) 使用 keycloack 作为 OIDC 服务权限认证服务。

##### 配置
配置位于 iotdb-engines.properties，设定 authorizer_provider_class 为 org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer 则开启了 openID 服务，默认情况下值为 org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer 表示没有开启 openID 服务。

```
authorizer_provider_class=org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer
```
如果开启了 openID 服务则 openID_url 为必填项，openID_url 值为 http://ip:port/auth/realms/{realmsName}

```
openID_url=http://127.0.0.1:8080/auth/realms/iotdb/
```

#### keycloack 配置

1、下载 keycloack 程序，在 keycloack/bin 中启动 keycloack

```shell
Shell >cd bin
Shell >./standalone.sh
```
2、使用 https://ip:port/auth 登陆 keycloack, 首次登陆需要创建用户

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/login_keycloak.png?raw=true)

3、点击 Administration Console 进入管理端

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/AdministrationConsole.png?raw=true)

4、在左侧的 Master 菜单点击 add Realm, 输入 Name 创建一个新的 Realm

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/add_Realm_1.png?raw=true)

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/add_Realm_2.png?raw=true)

5、点击左侧菜单 Clients，创建 client

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/client.png?raw=true)

6、点击左侧菜单 User，创建 user

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/user.png?raw=true)

7、点击新创建的用户 id，点击 Credentials 导航输入密码和关闭 Temporary 选项，至此 keyclork 配置完成

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/pwd.png?raw=true)

8、创建角色，点击左侧菜单的 Roles然后点击Add Role 按钮添加角色

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/add_role1.png?raw=true)

9、在Role Name 中输入`iotdb_admin`，点击save 按钮。提示：这里的`iotdb_admin`不能为其他名称否则即使登陆成功后也将无权限使用iotdb的查询、插入、创建存储组、添加用户、角色等功能

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/add_role2.png?raw=true)

10、点击左侧的User 菜单然后点击用户列表中的Edit的按钮为该用户添加我们刚创建的`iotdb_admin`角色

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/add_role3.png?raw=true)

11、选择Role Mappings ，在Available Role选择`iotdb_admin`角色然后点Add selected 按钮添加角色

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/add_role4.png?raw=true)

12、如果`iotdb_admin`角色在Assigned Roles中并且出现`Success Role mappings updated`提示，证明角色添加成功

![avatar](https://alioss.timecho.com/docs/img/UserGuide/CLI/Command-Line-Interface/add_role5.png?raw=true)

提示：如果用户角色有调整需要重新生成token并且重新登陆iotdb才会生效

以上步骤提供了一种 keycloak 登陆 iotdb 方式，更多方式请参考 keycloak 配置

若对应的 IoTDB 服务器开启了使用 OpenID Connect (OIDC) 作为权限认证服务，那么就不再需要使用用户名密码进行登录。
替而代之的是使用 Token，以及空密码。
此时，登录命令如下：

```shell
Shell > sbin/start-cli.sh -h 10.129.187.21 -p 6667 -u {my-access-token} -pw ""
```

其中，需要将{my-access-token} （注意，包括{}）替换成你的 token，即 access_token 对应的值。

如何获取 token 取决于你的 OIDC 设置。 最简单的一种情况是使用`password-grant`。例如，假设你在用 keycloack 作为你的 OIDC 服务，
并且你在 keycloack 中有一个被定义成 public 的`iotdb`客户的 realm，那么你可以使用如下`curl`命令获得 token。
（注意例子中的{}和里面的内容需要替换成具体的服务器地址和 realm 名字）：
```shell
curl -X POST "http://{your-keycloack-server}/auth/realms/{your-realm}/protocol/openid-connect/token" \ -H "Content-Type: application/x-www-form-urlencoded" \
 -d "username={username}" \
 -d "password={password}" \
 -d 'grant_type=password' \
 -d "client_id=iotdb-client"
```

示例结果如下：

```json
{"access_token":"eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJxMS1XbTBvelE1TzBtUUg4LVNKYXAyWmNONE1tdWNXd25RV0tZeFpKNG93In0.eyJleHAiOjE1OTAzOTgwNzEsImlhdCI6MTU5MDM5Nzc3MSwianRpIjoiNjA0ZmYxMDctN2NiNy00NTRmLWIwYmQtY2M2ZDQwMjFiNGU4IiwiaXNzIjoiaHR0cDovL2F1dGguZGVtby5wcmFnbWF0aWNpbmR1c3RyaWVzLmRlL2F1dGgvcmVhbG1zL0lvVERCIiwiYXVkIjoiYWNjb3VudCIsInN1YiI6ImJhMzJlNDcxLWM3NzItNGIzMy04ZGE2LTZmZThhY2RhMDA3MyIsInR5cCI6IkJlYXJlciIsImF6cCI6ImlvdGRiIiwic2Vzc2lvbl9zdGF0ZSI6IjA2MGQyODYyLTE0ZWQtNDJmZS1iYWY3LThkMWY3ODQ2NTdmMSIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsibG9jYWxob3N0OjgwODAiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iLCJpb3RkYl9hZG1pbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoiZW1haWwgcHJvZmlsZSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ1c2VyIn0.nwbrJkWdCNjzFrTDwKNuV5h9dDMg5ytRKGOXmFIajpfsbOutJytjWTCB2WpA8E1YI3KM6gU6Jx7cd7u0oPo5syHhfCz119n_wBiDnyTZkFOAPsx0M2z20kvBLN9k36_VfuCMFUeddJjO31MeLTmxB0UKg2VkxdczmzMH3pnalhxqpnWWk3GnrRrhAf2sZog0foH4Ae3Ks0lYtYzaWK_Yo7E4Px42-gJpohy3JevOC44aJ4auzJR1RBj9LUbgcRinkBy0JLi6XXiYznSC2V485CSBHW3sseXn7pSXQADhnmGQrLfFGO5ZljmPO18eFJaimdjvgSChsrlSEmTDDsoo5Q","expires_in":300,"refresh_expires_in":1800,"refresh_token":"eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJhMzZlMGU0NC02MWNmLTQ5NmMtOGRlZi03NTkwNjQ5MzQzMjEifQ.eyJleHAiOjE1OTAzOTk1NzEsImlhdCI6MTU5MDM5Nzc3MSwianRpIjoiNmMxNTBiY2EtYmE5NC00NTgxLWEwODEtYjI2YzhhMmI5YmZmIiwiaXNzIjoiaHR0cDovL2F1dGguZGVtby5wcmFnbWF0aWNpbmR1c3RyaWVzLmRlL2F1dGgvcmVhbG1zL0lvVERCIiwiYXVkIjoiaHR0cDovL2F1dGguZGVtby5wcmFnbWF0aWNpbmR1c3RyaWVzLmRlL2F1dGgvcmVhbG1zL0lvVERCIiwic3ViIjoiYmEzMmU0NzEtYzc3Mi00YjMzLThkYTYtNmZlOGFjZGEwMDczIiwidHlwIjoiUmVmcmVzaCIsImF6cCI6ImlvdGRiIiwic2Vzc2lvbl9zdGF0ZSI6IjA2MGQyODYyLTE0ZWQtNDJmZS1iYWY3LThkMWY3ODQ2NTdmMSIsInNjb3BlIjoiZW1haWwgcHJvZmlsZSJ9.ayNpXdNX28qahodX1zowrMGiUCw2AodlHBQFqr8Ui7c","token_type":"bearer","not-before-policy":0,"session_state":"060d2862-14ed-42fe-baf7-8d1f784657f1","scope":"email profile"}
```

### Cli 的批量操作
当您想要通过脚本的方式通过 Cli / Shell 对 IoTDB 进行批量操作时，可以使用-e 参数。通过使用该参数，您可以在不进入客户端输入模式的情况下操作 IoTDB。

为了避免 SQL 语句和其他参数混淆，现在只支持-e 参数作为最后的参数使用。

针对 cli/Shell 工具的-e 参数用法如下：

Linux 系统与 MacOS 指令：

```shell
Shell > sbin/start-cli.sh -h {host} -p {rpcPort} -u {user} -pw {password} -e {sql for iotdb}
```

Windows 系统指令
```shell
Shell > sbin\start-cli.bat -h {host} -p {rpcPort} -u {user} -pw {password} -e {sql for iotdb}
```

在 Windows 环境下，-e 参数的 SQL 语句需要使用` `` `对于`" "`进行替换

为了更好的解释-e 参数的使用，可以参考下面在 Linux 上执行的例子。

假设用户希望对一个新启动的 IoTDB 进行如下操作：

1. 创建名为 root.demo 的存储组

2. 创建名为 root.demo.s1 的时间序列

3. 向创建的时间序列中插入三个数据点

4. 查询验证数据是否插入成功

那么通过使用 cli/Shell 工具的 -e 参数，可以采用如下的脚本：

```shell
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

打印出来的结果显示如下，通过这种方式进行的操作与客户端的输入模式以及通过 JDBC 进行操作结果是一致的。

```shell
 Shell > ./shell.sh 
+-----------------------------+------------+
|                         Time|root.demo.s1|
+-----------------------------+------------+
|1970-01-01T08:00:00.001+08:00|          10|
|1970-01-01T08:00:00.002+08:00|          11|
|1970-01-01T08:00:00.003+08:00|          12|
+-----------------------------+------------+
Total line number = 3
It costs 0.267s
```

需要特别注意的是，在脚本中使用 -e 参数时要对特殊字符进行转义。
