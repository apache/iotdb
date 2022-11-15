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

## 节点管理

### 查看子路径

```
SHOW CHILD PATHS pathPattern ? limitClause
```

可以查看此路径模式所匹配的所有路径的下一层的所有路径，即pathPattern.*所匹配的路径。

查询结果集的大小默认为 10000000，如需查询更多信息，请使用```limit```和```offset```。

示例：

* 查询 root.ln 的下一层：show child paths root.ln

```
+------------+
| child paths|
+------------+
|root.ln.wf01|
|root.ln.wf02|
+------------+
```

* 查询形如 root.xx.xx.xx 的路径：show child paths root.\*.\*

```
+---------------+
|    child paths|
+---------------+
|root.ln.wf01.s1|
|root.ln.wf02.s2|
+---------------+
```

### 查看子节点

```
SHOW CHILD NODES pathPattern ? limitClause
```

可以查看此路径模式所匹配的节点的下一层的所有节点。

查询结果集的大小默认为 10000000，如需查询更多信息，请使用```limit```和```offset```。

示例：

* 查询 root 的下一层：show child nodes root

```
+------------+
| child nodes|
+------------+
|          ln|
+------------+
```

* 查询 root.ln 的下一层 ：show child nodes root.ln

```
+------------+
| child nodes|
+------------+
|        wf01|
|        wf02|
+------------+
```

### 统计节点数

IoTDB 支持使用`COUNT NODES <PathPattern> LEVEL=<INTEGER>`来统计当前 Metadata
 树下满足某路径模式的路径中指定层级的节点个数。这条语句可以用来统计带有特定采样点的设备数。例如：

```
IoTDB > COUNT NODES root.** LEVEL=2
IoTDB > COUNT NODES root.ln.** LEVEL=2
IoTDB > COUNT NODES root.ln.wf01.* LEVEL=3
IoTDB > COUNT NODES root.**.temperature LEVEL=3
```

对于上面提到的例子和 Metadata Tree，你可以获得如下结果：

```
+-----+
|count|
+-----+
|    4|
+-----+
Total line number = 1
It costs 0.003s

+-----+
|count|
+-----+
|    2|
+-----+
Total line number = 1
It costs 0.002s

+-----+
|count|
+-----+
|    1|
+-----+
Total line number = 1
It costs 0.002s

+-----+
|count|
+-----+
|    2|
+-----+
Total line number = 1
It costs 0.002s
```

> 注意：时间序列的路径只是过滤条件，与 level 的定义无关。

### 查看设备

* SHOW DEVICES pathPattern? (WITH STORAGE GROUP)? limitClause? #showDevices

查询结果集的大小默认为 10000000，如需查询更多信息，请使用```limit```和```offset```。

与 `Show Timeseries` 相似，IoTDB 目前也支持两种方式查看设备。

* `SHOW DEVICES` 语句显示当前所有的设备信息，等价于 `SHOW DEVICES root.**`。
* `SHOW DEVICES <PathPattern>` 语句规定了 `PathPattern`，返回给定的路径模式所匹配的设备信息。

SQL 语句如下所示：

```
IoTDB> show devices
IoTDB> show devices root.ln.**
```

你可以获得如下数据：

```
+-------------------+---------+
|            devices|isAligned|
+-------------------+---------+
|  root.ln.wf01.wt01|    false|
|  root.ln.wf02.wt02|    false|
|root.sgcc.wf03.wt01|    false|
|    root.turbine.d1|    false|
+-------------------+---------+
Total line number = 4
It costs 0.002s

+-----------------+---------+
|          devices|isAligned|
+-----------------+---------+
|root.ln.wf01.wt01|    false|
|root.ln.wf02.wt02|    false|
+-----------------+---------+
Total line number = 2
It costs 0.001s
```

其中，`isAligned`表示该设备下的时间序列是否对齐。

查看设备及其存储组信息，可以使用 `SHOW DEVICES WITH STORAGE GROUP` 语句。

* `SHOW DEVICES WITH STORAGE GROUP` 语句显示当前所有的设备信息和其所在的存储组，等价于 `SHOW DEVICES root.**`。
* `SHOW DEVICES <PathPattern> WITH STORAGE GROUP` 语句规定了 `PathPattern`，返回给定的路径模式所匹配的设备信息和其所在的存储组。

SQL 语句如下所示：

```
IoTDB> show devices with storage group
IoTDB> show devices root.ln.** with storage group
```

你可以获得如下数据：

```
+-------------------+-------------+---------+
|            devices|storage group|isAligned|
+-------------------+-------------+---------+
|  root.ln.wf01.wt01|      root.ln|    false|
|  root.ln.wf02.wt02|      root.ln|    false|
|root.sgcc.wf03.wt01|    root.sgcc|    false|
|    root.turbine.d1| root.turbine|    false|
+-------------------+-------------+---------+
Total line number = 4
It costs 0.003s

+-----------------+-------------+---------+
|          devices|storage group|isAligned|
+-----------------+-------------+---------+
|root.ln.wf01.wt01|      root.ln|    false|
|root.ln.wf02.wt02|      root.ln|    false|
+-----------------+-------------+---------+
Total line number = 2
It costs 0.001s
```
