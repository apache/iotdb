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



## Grafana 插件

Grafana 是开源的指标量监测和可视化工具，可用于展示时序数据和应用程序运行分析。

在 IoTDB 项目中，我们开发了 Grafana 插件，该插件通过调用 IoTDB REST 服务来展现 IoTDB 中时序数据 ，提供了众多时序数据的可视化方法。Grafana 插件相较于 IoTDB-Grafana-Connector 连接器执行效率更高、支持的查询种类更多。只要在您部署环境允许的情况下，*我们都推荐直接使用 Grafana 插件而不使用 IoTDB-Grafana-Connector 连接器*。



### 部署 Grafana 插件

#### 安装 Grafana 

* Grafana 组件下载地址：https://grafana.com/grafana/download
* 版本 >= 7.0.0



#### grafana-plugin 下载

* 插件名称: grafana-plugin
* 下载地址: https://github.com/apache/iotdb.git

执行下面的命令：

```shell
git clone https://github.com/apache/iotdb.git
```



#### grafana-plugin 编译

##### 方案一

我们需要编译 IoTDB 仓库 `grafana-plugin` 目录下的前端工程并生成 `dist` 目标目录，具体执行流程如下。

您可以采取下面任意一种编译方式：

* 使用 maven 编译，在 `grafana-plugin` 目录下执行：

```shell
mvn install package -P compile-grafana-plugin
```

* 或使用 yarn 编译，在 `grafana-plugin` 目录下执行：

```shell
yarn install
yarn build
```

如果编译成功，我们将看到生成的目标文件夹 `dist`，它包含了编译好的 Grafana 前端插件：

<img style="width:100%; max-width:333px; max-height:545px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/grafana-plugin-build.png?raw=true">

##### 方案二

我们也可以通过执行 IoTDB 项目的**打包指令**获取 `grafana-plugin ` 的前端工程和其他配套的 IoTDB 可执行文件。

在 IoTDB 仓库的根目录下执行：

```shell
 mvn clean package -pl distribution -am -DskipTests -P compile-grafana-plugin
```

如果编译成功，我们将看到 `distribution/target` 路径下包含了编译好的 Grafana 前端插件：

<img style="width:100%; max-width:333px; max-height:545px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/distribution.png?raw=true">



#### grafana-plugin 插件安装

* 拷贝上述生成的前端工程目标文件夹到 Grafana 的插件目录中 `${Grafana文件目录}\data\plugins\`。如果没有此目录可以手动建或者启动grafana会自动建立，当然也可以修改plugins的位置,具体请查看下面的修改Grafana 的插件目录位置说明。

* 修改Grafana的配置文件：找到配置文件（`${Grafana文件目录}\conf\defaults.ini`），并进行如下的修改：

  ```ini
  allow_loading_unsigned_plugins = iotdb
  ```
* 修改Grafana 的插件目录位置:找到配置文件（`${Grafana文件目录}\conf\defaults.ini`），并进行如下的修改：

  ```ini
  plugins = data/plugins
  ```
* 如果 Grafana 服务已启动，则需要重启服务。
更多详情，请点 [这里](https://grafana.com/docs/grafana/latest/plugins/installation/)


#### 启动 Grafana

进入 Grafana 的安装目录，使用以下命令启动 Grafana：
* Windows 系统：

```shell
bin\grafana-server.exe
```
* Linux 系统：

```shell
sudo service grafana-server start
```
* MacOS 系统：

```shell
brew services start grafana
```
更多详情，请点 [这里](https://grafana.com/docs/grafana/latest/installation/)



#### 配置 IoTDB REST 服务

进入 `{iotdb 目录}/conf`，打开 `iotdb-rest.properties` 文件，并作如下修改：

```properties
# Is the REST service enabled
enable_rest_service=true

# the binding port of the REST service
rest_service_port=18080
```

启动（重启）IoTDB 使配置生效，此时 IoTDB REST 服务处于运行状态。



### 使用 Grafana 插件

#### 访问 Grafana dashboard

Grafana 以网页的 dashboard 形式为您展示数据，在使用时请您打开浏览器，访问 `http://<ip>:<port>`。

注：IP 为您的 Grafana 所在的服务器 IP，Port 为 Grafana 的运行端口（默认 3000）。

在本地试用时，Grafana  dashboard 的默认地址为 `http://localhost:3000/`。

默认登录的用户名和密码都是 `admin`。



#### 添加 IoTDB 数据源

点击左侧的 `设置` 图标，选择 `Data Source` 选项，然后再点击 `Add data source`。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/datasource_1.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/datasource_2.png?raw=true">

选择 `Apache IoTDB` 数据源，`URL` 一栏填写  `http://<ip>:<port>`。

Ip 为您的 IoTDB 服务器所在的宿主机 IP，port 为 REST 服务的运行端口（默认 18080）。

输入 IoTDB 服务器的 username 和 password，点击 `Save & Test`，出现 `Success` 则提示配置成功。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/datasource_3.png?raw=true">



#### 创建一个新的 Panel

点击左侧的 `Dashboards` 图标，选择 `Manage`，如下图所示：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/manage.png?raw=true">

点击右上方的 `New Dashboard`  图标，选择 `Add an empty panel`，如下图所示：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/add-empty-panel.png?raw=true">

在 SELECT 输入框、FROM 输入框、WHERE输入框、CONTROL输入框中输入内容，其中 WHERE 和 CONTROL 输入框为非必填。

如果一个查询涉及多个表达式，我们可以点击 SELECT 输入框右侧的 `+` 来添加 SELECT 子句中的表达式，也可以点击 FROM 输入框右侧的 `+` 来添加路径前缀，如下图所示：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/grafana_input.png?raw=true">

SELECT 输入框中的内容可以是时间序列的后缀，可以是函数或自定义函数，可以是算数表达式，也可以是它们的嵌套表达式。您还可以使用 as 子句来重命名需要显示的结果序列名字。

下面是 SELECT 输入框中一些合法的输入举例：

*  `s1`
*  `top_k(s1, 'k'='1') as top`
*  `sin(s1) + cos(s1 + s2)` 
*  `udf(s1) as "中文别名"`

FROM 输入框中的内容必须是时间序列的前缀路径，比如 `root.sg.d`。

WHERE 输入框为非必须填写项目，填写内容应当是查询的过滤条件，比如 `time > 0`  或者 `s1 < 1024 and s2 > 1024`。

CONTROL 输入框为非必须填写项目，填写内容应当是控制查询类型、输出格式的特殊子句，下面是 CONTROL 输入框中一些合法的输入举例：

*  `group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00), 1d)`
*  `group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d)`
*  `GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00), 1m) FILL (PREVIOUSUNTILLAST)` 
*  `GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00), 1m) FILL (PREVIOUS, 1m)`
*  `GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00), 1m) FILL (LINEAR, 5m, 5m)`
*  `group by ((2017-11-01T00:00:00, 2017-11-07T23:00:00], 1d), level=1`
*  `group by ([0, 20), 2ms, 3ms), level=1`

提示：为了避免OOM问题，不推荐使用select * from root.xx.** 这种语句在Grafana plugin中使用。

#### 变量与模板功能的支持

本插件支持 Grafana 的变量与模板（ https://grafana.com/docs/grafana/v7.0/variables/）功能。

创建一个新的 Panel 后，点击右上角的设置按钮，如下图所示：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/setconf.png?raw=true">

选择 `Variables`，点击 `Add variable` ，如下图所示：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/addvaribles.png?raw=true">

输入 `Name`，`Label`，和 `Query`， 点击 `Update` 按钮，如下图所示：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/variblesinput.png?raw=true">

应用 Variables，在 `grafana  panel` 中输入变量点击 `save` 按钮，如下图所示

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/applyvariables.png?raw=true">



### 更多

更多关于 Grafana 操作详情可参看 Grafana 官方文档：http://docs.grafana.org/guides/getting_started/。

