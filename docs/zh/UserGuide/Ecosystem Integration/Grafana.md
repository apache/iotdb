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

# IoTDB-Grafana

## 概览

- IoTDB-Grafana
  - Grafana的安装与部署
    - 安装
    - simple-json-datasource数据源插件安装
    - 启动Grafana
  - IoTDB安装
  - IoTDB-Grafana连接器安装
    - 启动IoTDB-Grafana
  - 使用Grafana
    - 添加IoTDB数据源
    - 操作Grafana

<!-- /TOC -->

Grafana是开源的指标量监测和可视化工具，可用于展示时序数据和应用程序运行分析。Grafana支持Graphite，InfluxDB等国际主流时序数据库作为数据源。在IoTDB项目中，我们开发了Grafana展现IoTDB中时序数据的连接器IoTDB-Grafana，为您提供使用Grafana展示IoTDB数据库中的时序数据的可视化方法。

## Grafana的安装与部署

### 安装

* Grafana组件下载地址：https://grafana.com/grafana/download
* 版本 >= 4.4.1

### simple-json-datasource数据源插件安装

* 插件名称: simple-json-datasource
* 下载地址: https://github.com/grafana/simple-json-datasource

具体下载方法是：到Grafana的插件目录中：`{Grafana文件目录}\data\plugins\`（Windows系统，启动Grafana后会自动创建`data\plugins`目录）或`/var/lib/grafana/plugins` （Linux系统，plugins目录需要手动创建）或`/usr/local/var/lib/grafana/plugins`（MacOS系统，具体位置参看使用`brew install`安装Grafana后命令行给出的位置提示。

执行下面的命令：

```
Shell > git clone https://github.com/grafana/simple-json-datasource.git
```
然后重启Grafana服务器，在浏览器中登录Grafana，在“Add data source”页面中“Type”选项出现“SimpleJson”即为安装成功。

### 启动Grafana
进入Grafana的安装目录，使用以下命令启动Grafana：
* Windows系统：
```
Shell > bin\grafana-server.exe
```
* Linux系统：
```
Shell > sudo service grafana-server start
```
* MacOS系统：
```
Shell > grafana-server --config=/usr/local/etc/grafana/grafana.ini --homepath /usr/local/share/grafana cfg:default.paths.logs=/usr/local/var/log/grafana cfg:default.paths.data=/usr/local/var/lib/grafana cfg:default.paths.plugins=/usr/local/var/lib/grafana/plugins
```
更多安装详情，请点[这里](https://grafana.com/docs/grafana/latest/installation/)

## IoTDB安装

参见[https://github.com/apache/iotdb](https://github.com/apache/iotdb)

## IoTDB-Grafana连接器安装

```shell
git clone https://github.com/apache/iotdb.git
```

### 启动IoTDB-Grafana

#### 方案一（适合开发者）

导入整个项目，maven依赖安装完后，直接运行`iotdb/grafana/rc/main/java/org/apache/iotdb/web/grafana`目录下`TsfileWebDemoApplication.java`，这个grafana连接器采用springboot开发

#### 方案二（适合使用者）

```shell
cd iotdb
mvn clean package -pl grafana -am -Dmaven.test.skip=true
cd grafana/target
java -jar iotdb-grafana-{version}.war
  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 :: Spring Boot ::        (v1.5.4.RELEASE)
...
```

如果您需要配置属性，将`grafana/src/main/resources/application.properties`移动到war包同级目录下（`grafana/target`）

## 使用Grafana

Grafana以网页的dashboard形式为您展示数据，在使用时请您打开浏览器，访问http://\<ip\>:\<port\>

默认地址为http://localhost:3000/

注：IP为您的Grafana所在的服务器IP，Port为Grafana的运行端口（默认3000）。默认登录的用户名和密码都是“admin”。

### 添加IoTDB数据源

点击左上角的“Grafana”图标，选择`Data Source`选项，然后再点击`Add data source`。
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51664777-2766ae00-1ff5-11e9-9d2f-7489f8ccbfc2.png">

在编辑数据源的时候，`Type`一栏选择`Simplejson`，`URL`一栏填写http://\<ip\>:\<port\>，IP为您的IoTDB-Grafana连接器所在的服务器IP，Port为运行端口（默认8888）。之后确保IoTDB已经启动，点击“Save & Test”，出现“Data Source is working”提示表示配置成功。
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51664842-554bf280-1ff5-11e9-97d2-54eebe0b2ca1.png">

### 操作Grafana

进入Grafana可视化页面后，可以选择添加时间序列，如图 6.9。您也可以按照Grafana官方文档进行相应的操作，详情可参看Grafana官方文档：http://docs.grafana.org/guides/getting_started/。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51664878-6e54a380-1ff5-11e9-9718-4d0e24627fa8.png">

## 配置grafana

```
# IoTDB的IP和端口
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
# 默认采样interval
interval=1m
# 用于对连续数据(int, long, float, double)进行降采样的聚合函数
# COUNT, FIRST_VALUE, LAST_VALUE, MAX_TIME, MAX_VALUE, AVG, MIN_TIME, MIN_VALUE, NOW, SUM
continuous_data_function=AVG
# 用于对离散数据(boolean, string)进行降采样的聚合函数
# COUNT, FIRST_VALUE, LAST_VALUE, MAX_TIME, MIN_TIME, NOW
discrete_data_function=LAST_VALUE
```

其中interval具体配置信息如下

<1h: no sampling

1h~1d : intervals = 1m

1d~30d:intervals = 1h

\>30d：intervals = 1d

配置完后，请重新运行war包

```
java -jar iotdb-grafana-{version}.war
```

