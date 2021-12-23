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

# 系统集成

## Grafana-IoTDB

Grafana 是开源的指标量监测和可视化工具，可用于展示时序数据和应用程序运行分析。Grafana 支持 Graphite，InfluxDB 等国际主流时序数据库作为数据源。在 IoTDB 项目中，我们基于IoTDB Rest Service开发了 Grafana-Plugin 来展现 IoTDB 中时序数据 ，Grafana-Plugin 为您提供使用 Grafana 展示 IoTDB 数据库中的时序数据的可视化方法。

### Grafana 的安装与部署

#### 安装

* Grafana 组件下载地址：https://grafana.com/grafana/download
* 版本 >= 7.0.0

#### grafana-plugin 下载

* 插件名称: grafana-plugin
* 下载地址: https://github.com/apache/iotdb.git

执行下面的命令：

```
Shell > git clone https://github.com/apache/iotdb.git
```

#### grafana-plugin 编译

编译grafana-plugin生成的dist 目录，具体命令如下

* 方案一（适合开发者）

导入整个项目，maven 依赖安装完后，修改`iotdb/server/src/main/java/org/apache/iotdb/db/conf/rest/`目录下`IoTDBRestServiceConfig.java` 中的 `enableRestService=true`，然后启动IoTDB

编译grafana-plugin，这里支持两种通过maven 编译和通过yarn 编译,我们看到生成了dist 文件就是编译成功如下图所示
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/grafana-plugin-build.png?raw=true">

```shell
cd iotdb/grafana-plugin
mvn install package
```
or
```shell
cd iotdb/grafana-plugin
yarn install
yarn build
```

* 方案二（适合使用者）
```shell
cd iotdb
mvn clean package -pl grafana-plugin -am -Dmaven.test.skip=true
```

##### grafana-plugin 插件安装

具体方法是：拷贝dist目录到Grafana的插件目录中：`{Grafana文件目录}\data\plugins\`（Windows系统，启动Grafana后会自动创建`data\plugins`目录）或`/var/lib/grafana/plugins` （Linux系统，plugins目录需要手动创建）或`/usr/local/var/lib/grafana/plugins`（MacOS系统，具体位置参看使用`brew install`安装Grafana后命令行给出的位置提示。

找到相关的Grafana的配置文件（`{Grafana文件目录}\conf\defaults.ini`），并进行如下的配置，然后重启Grafana 服务

```
allow_loading_unsigned_plugins = iotdb
```

#### 启动 Grafana

进入 Grafana 的安装目录，使用以下命令启动 Grafana：
* Windows 系统：

```
Shell > bin\grafana-server.exe
```
* Linux 系统：

```
Shell > sudo service grafana-server start
```
* MacOS 系统：

```
Shell > brew services start grafana
```
更多详情，请点 [这里](https://grafana.com/docs/grafana/latest/installation/)

### IoTDB 安装

参见 [https://github.com/apache/iotdb](https://github.com/apache/iotdb)

### 配置 IoTDB Rest Service

进入{iotdb 目录}/conf，打开iotdb-rest.properties 文件
开启 REST 服务 设置`enable_rest_service=true`

配置REST 服务端口 ，设置`rest_service_port=18080`

```
# Is the REST service enabled
enable_rest_service=false

# the binding port of the REST service
rest_service_port=18080
```

### 使用 Grafana

Grafana 以网页的 dashboard 形式为您展示数据，在使用时请您打开浏览器，访问 `http://<ip>:<port>`

默认地址为 `http://localhost:3000/`

注：IP 为您的 Grafana 所在的服务器 IP，Port 为 Grafana 的运行端口（默认 3000）。默认登录的用户名和密码都是“admin”。

#### 添加 IoTDB 数据源

点击左侧的“设置”图标，选择`Data Source`选项，然后再点击`Add data source`。
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/datasource_1.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/datasource_2.png?raw=true">

选择Apache IoTDB 数据源，`URL`一栏填写 `http://<ip>:<port>`，IP 为您的 IoTDB-Grafana 连接器所在的服务器 IP，Port 为运行端口（默认 18080），输入Iotdb服务器的username 和password。之后确保 IoTDB 已经启动，点击“Save & Test”，出现“Success”提示表示配置成功。
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/datasource_3.png?raw=true">

#### 操作 Grafana

点击左侧的`Dashboards`图标，选择`manage`，如下图所示
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/manage.png?raw=true">

点击右上方的`New Dashboard` 图标，选择`Add an empty panel`，如下图所示
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/add%20empty%20panel.png?raw=true">

在SELECT 输入框、FROM输入框、WHERE输入框输入内容，其中WHERE输入框为非必填，如果一个序列查询多个表达式内容我们可以点击右侧加号来添加表达式，如果有多个序列需要添加多个FROM框，如下图所示
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/grafana_input.png?raw=true">

#### Grafana 添加支持变量

如果涉及的序列比较多，我们可以使用变量来帮助实现查询，点击右上角的设置按钮，如下图所示
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/setconf.png?raw=true">

选择`variables`，点击`Add variable` ，如下图所示
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/addvaribles.png?raw=true">

输入Name，Label，和Query 点击update按钮，如下图所示
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/variblesinput.png?raw=true">

应用variable，在`grafana  panel` 中输入变量点击`save` 按钮，如下图所示
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/applyvariables.png?raw=true">


更多关于 Grafana 操作详情可参看 Grafana 官方文档：http://docs.grafana.org/guides/getting_started/。

