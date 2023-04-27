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

## Grafana 连接器

Grafana 是开源的指标量监测和可视化工具，可用于展示时序数据和应用程序运行分析。Grafana 支持 Graphite，InfluxDB 等国际主流时序数据库作为数据源。在 IoTDB 项目中，我们开发了 Grafana 展现 IoTDB 中时序数据的连接器 IoTDB-Grafana-Connector，为您提供使用 Grafana 展示 IoTDB 数据库中的时序数据的可视化方法。

### Grafana 的安装与部署

#### 安装

* Grafana 组件下载地址：https://grafana.com/grafana/download
* 版本 >= 4.4.1

#### simple-json-datasource 数据源插件安装


* 插件名称: simple-json-datasource
* 下载地址: https://github.com/grafana/simple-json-datasource

##### windows系统
具体下载方法是：到Grafana的插件目录中：`{Grafana文件目录}\data\plugins\`（Windows系统，启动Grafana后会自动创建`data\plugins`目录）或`/var/lib/grafana/plugins` （Linux系统，plugins目录需要手动创建）或`/usr/local/var/lib/grafana/plugins`（MacOS系统，具体位置参看使用`brew install`安装Grafana后命令行给出的位置提示。

执行下面的命令：

```
Shell > git clone https://github.com/grafana/simple-json-datasource.git
```

##### linux系统
建议使用grafana-cli安装该插件，具体安装命令如下

```
sudo grafana-cli plugins install grafana-simple-json-datasource
sudo service grafana-server restart
```

##### 后续操作
然后重启Grafana服务器，在浏览器中登录Grafana，在“Add data source”页面中“Type”选项出现“SimpleJson”即为安装成功。

如果出现如下报错
```
Unsigned plugins were found during plugin initialization. Grafana Labs cannot guarantee the integrity of these plugins. We recommend only using signed plugins.
The following plugins are disabled and not shown in the list below:
```

请找到相关的grafana的配置文件（例如windows下的customer.ini，linux下rpm安装后为/etc/grafana/grafana.ini），并进行如下的配置

```
allow_loading_unsigned_plugins = "grafana-simple-json-datasource"
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
Shell > grafana-server --config=/usr/local/etc/grafana/grafana.ini --homepath /usr/local/share/grafana cfg:default.paths.logs=/usr/local/var/log/grafana cfg:default.paths.data=/usr/local/var/lib/grafana cfg:default.paths.plugins=/usr/local/var/lib/grafana/plugins
```
更多安装详情，请点 [这里](https://grafana.com/docs/grafana/latest/installation/)

### IoTDB 安装

参见 [https://github.com/apache/iotdb](https://github.com/apache/iotdb)

### Grafana-IoTDB-Connector 连接器安装

```shell
git clone https://github.com/apache/iotdb.git
```

### 启动 Grafana-IoTDB-Connector

 * 方案一（适合开发者）

导入整个项目，maven 依赖安装完后，直接运行`iotdb/grafana-connector/rc/main/java/org/apache/iotdb/web/grafana`目录下`TsfileWebDemoApplication.java`，这个 grafana 连接器采用 springboot 开发

 * 方案二（适合使用者）

```shell
cd iotdb
mvn clean package -pl grafana-connector -am -Dmaven.test.skip=true
cd grafana/target
java -jar iotdb-grafana-connector-{version}.war
  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 :: Spring Boot ::        (v1.5.4.RELEASE)
...
```

如果您需要配置属性，将`grafana/src/main/resources/application.properties`移动到 war 包同级目录下（`grafana/target`）

### 使用 Grafana

Grafana 以网页的 dashboard 形式为您展示数据，在使用时请您打开浏览器，访问 http://\<ip\>:\<port\>

默认地址为 http://localhost:3000/

注：IP 为您的 Grafana 所在的服务器 IP，Port 为 Grafana 的运行端口（默认 3000）。默认登录的用户名和密码都是“admin”。

#### 添加 IoTDB 数据源

点击左上角的“Grafana”图标，选择`Data Source`选项，然后再点击`Add data source`。
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/github/51664777-2766ae00-1ff5-11e9-9d2f-7489f8ccbfc2.png">

在编辑数据源的时候，`Type`一栏选择`Simplejson`，`URL`一栏填写 http://\<ip\>:\<port\>，IP 为您的 IoTDB-Grafana-Connector 连接器所在的服务器 IP，Port 为运行端口（默认 8888）。之后确保 IoTDB 已经启动，点击“Save & Test”，出现“Data Source is working”提示表示配置成功。
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/github/51664842-554bf280-1ff5-11e9-97d2-54eebe0b2ca1.png">

#### 操作 Grafana

进入 Grafana 可视化页面后，可以选择添加时间序列，如下图。您也可以按照 Grafana 官方文档进行相应的操作，详情可参看 Grafana 官方文档：http://docs.grafana.org/guides/getting_started/。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/github/51664878-6e54a380-1ff5-11e9-9718-4d0e24627fa8.png">

### 配置 grafana

```
# IoTDB 的 IP 和端口
spring.datasource.url=jdbc:iotdb://127.0.0.1:6667/
spring.datasource.username=root
spring.datasource.password=root
spring.datasource.driver-class-name=org.apache.iotdb.jdbc.IoTDBDriver
server.port=8888
# Use this value to set timestamp precision as "ms", "us" or "ns", which must to be same with the timestamp
# precision of Apache IoTDB engine.
timestamp_precision=ms

# 是否开启降采样
isDownSampling=true
# 默认采样 interval
interval=1m
# 用于对连续数据 (int, long, float, double) 进行降采样的聚合函数
# COUNT, FIRST_VALUE, LAST_VALUE, MAX_TIME, MAX_VALUE, AVG, MIN_TIME, MIN_VALUE, NOW, SUM
continuous_data_function=AVG
# 用于对离散数据 (boolean, string) 进行降采样的聚合函数
# COUNT, FIRST_VALUE, LAST_VALUE, MAX_TIME, MIN_TIME, NOW
discrete_data_function=LAST_VALUE
```

其中 interval 具体配置信息如下

<1h: no sampling

1h~1d : intervals = 1m

1d~30d:intervals = 1h

\>30d：intervals = 1d

配置完后，请重新运行 war 包

```
java -jar iotdb-grafana-connector-{version}.war
```
