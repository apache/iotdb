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

## Zeppelin-IoTDB

### Zeppelin 简介

Apache Zeppelin 是一个基于网页的交互式数据分析系统。用户可以通过 Zeppelin 连接数据源并使用 SQL、Scala 等进行交互式操作。操作可以保存为文档（类似于 Jupyter）。Zeppelin 支持多种数据源，包括 Spark、ElasticSearch、Cassandra 和 InfluxDB 等等。现在，IoTDB 已经支持使用 Zeppelin 进行操作。样例如下：

![iotdb-note-snapshot](https://alioss.timecho.com/docs/img/github/102752947-520a3e80-43a5-11eb-8fb1-8fac471c8c7e.png)

### Zeppelin-IoTDB 解释器

#### 系统环境需求

| IoTDB 版本 |   Java 版本   | Zeppelin 版本 |
| :--------: | :-----------: | :-----------: |
| >=`0.12.0` | >=`1.8.0_271` |   `>=0.9.0`   |

安装 IoTDB：参考 [快速上手](https://iotdb.apache.org/zh/UserGuide/Master/QuickStart/QuickStart.html). 假设 IoTDB 安装在 `$IoTDB_HOME`.

安装 Zeppelin：
> 方法 1 直接下载：下载 [Zeppelin](https://zeppelin.apache.org/download.html#) 并解压二进制文件。推荐下载 [netinst](http://www.apache.org/dyn/closer.cgi/zeppelin/zeppelin-0.9.0/zeppelin-0.9.0-bin-netinst.tgz) 二进制包，此包由于未编译不相关的 interpreter，因此大小相对较小。
>
> 方法 2 源码编译：参考 [从源码构建 Zeppelin](https://zeppelin.apache.org/docs/latest/setup/basics/how_to_build.html) ，使用命令为 `mvn clean package -pl zeppelin-web,zeppelin-server -am -DskipTests`。

假设 Zeppelin 安装在 `$Zeppelin_HOME`.

#### 编译解释器

运行如下命令编译 IoTDB Zeppelin 解释器。

```shell
cd $IoTDB_HOME
mvn clean package -pl zeppelin-interpreter -am -DskipTests -P get-jar-with-dependencies
```

编译后的解释器位于如下目录：

```shell
$IoTDB_HOME/zeppelin-interpreter/target/zeppelin-{version}-SNAPSHOT-jar-with-dependencies.jar
```

#### 安装解释器

当你编译好了解释器，在 Zeppelin 的解释器目录下创建一个新的文件夹`iotdb`，并将 IoTDB 解释器放入其中。

```shell
cd $IoTDB_HOME
mkdir -p $Zeppelin_HOME/interpreter/iotdb
cp $IoTDB_HOME/zeppelin-interpreter/target/zeppelin-{version}-SNAPSHOT-jar-with-dependencies.jar $Zeppelin_HOME/interpreter/iotdb
```

#### 启动 Zeppelin 和 IoTDB

进入 `$Zeppelin_HOME` 并运行 Zeppelin：

```shell
# Unix/OS X
> ./bin/zeppelin-daemon.sh start

# Windows
> .\bin\zeppelin.cmd
```

进入 `$IoTDB_HOME` 并运行 IoTDB：

```shell
# Unix/OS X
> nohup sbin/start-server.sh >/dev/null 2>&1 &
or
> nohup sbin/start-server.sh -c <conf_path> -rpc_port <rpc_port> >/dev/null 2>&1 &

# Windows
> sbin\start-server.bat -c <conf_path> -rpc_port <rpc_port>
```

### 使用 Zeppelin-IoTDB 解释器

当 Zeppelin 启动后，访问 [http://127.0.0.1:8080/](http://127.0.0.1:8080/)

通过如下步骤创建一个新的笔记本页面：

1. 点击 `Create new node` 按钮
2. 设置笔记本名
3. 选择解释器为 iotdb

现在可以开始使用 Zeppelin 操作 IoTDB 了。

![iotdb-create-note](https://alioss.timecho.com/docs/img/github/102752945-5171a800-43a5-11eb-8614-53b3276a3ce2.png)

我们提供了一些简单的 SQL 来展示 Zeppelin-IoTDB 解释器的使用：

```sql
CREATE DATABASE root.ln.wf01.wt01;
CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN;
CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN;
CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN;

INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
VALUES (1, 1.1, false, 11);

INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
VALUES (2, 2.2, true, 22);

INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
VALUES (3, 3.3, false, 33);

INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
VALUES (4, 4.4, false, 44);

INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
VALUES (5, 5.5, false, 55);

SELECT *
FROM root.ln.wf01.wt01
WHERE time >= 1
	AND time <= 6;
```

样例如下：

![iotdb-note-snapshot2](https://alioss.timecho.com/docs/img/github/102752948-52a2d500-43a5-11eb-9156-0c55667eb4cd.png)

用户也可以参考 [[1]](https://zeppelin.apache.org/docs/0.9.0/usage/display_system/basic.html) 编写更丰富多彩的文档。

以上样例放置于 `$IoTDB_HOME/zeppelin-interpreter/Zeppelin-IoTDB-Demo.zpln`

### 解释器配置项

进入页面 [http://127.0.0.1:8080/#/interpreter](http://127.0.0.1:8080/#/interpreter) 并配置 IoTDB 的连接参数：

![iotdb-configuration](https://alioss.timecho.com/docs/img/github/102752940-50407b00-43a5-11eb-94fb-3e3be222183c.png)

可配置参数默认值和解释如下：

| 属性                         | 默认值    | 描述                             |
| ---------------------------- | --------- | -------------------------------- |
| iotdb.host                   | 127.0.0.1 | IoTDB 主机名                     |
| iotdb.port                   | 6667      | IoTDB 端口                       |
| iotdb.username               | root      | 用户名                           |
| iotdb.password               | root      | 密码                             |
| iotdb.fetchSize              | 10000     | 查询结果分批次返回时，每一批数量 |
| iotdb.zoneId                 |           | 时区 ID                           |
| iotdb.enable.rpc.compression | FALSE     | 是否允许 rpc 压缩                  |
| iotdb.time.display.type      | default   | 时间戳的展示格式                 |
