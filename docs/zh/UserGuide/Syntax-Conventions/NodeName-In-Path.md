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

# 路径结点名

路径结点名是特殊的标识符，其还可以是通配符 \* 或 \*\*。在创建时间序列时，各层级的路径结点名不能为通配符 \* 或 \*\*。在查询语句中，可以用通配符 \* 或 \*\* 来表示路径结点名，以匹配一层或多层路径。

## 通配符

`*`在路径中表示一层。例如`root.vehicle.*.sensor1`代表的是以`root.vehicle`为前缀，以`sensor1`为后缀，层次等于 4 层的路径。

`**`在路径中表示是（`*`）+，即为一层或多层`*`。例如`root.vehicle.device1.**`代表的是`root.vehicle.device1.*`, `root.vehicle.device1.*.*`, `root.vehicle.device1.*.*.*`等所有以`root.vehicle.device1`为前缀路径的大于等于 4 层的路径；`root.vehicle.**.sensor1`代表的是以`root.vehicle`为前缀，以`sensor1`为后缀，层次大于等于 4 层的路径。

由于通配符 * 在查询表达式中也可以表示乘法符号，下述例子用于帮助您区分两种情况：

```SQL
# 创建时间序列 root.sg.`a*b`
create timeseries root.sg.`a*b` with datatype=FLOAT,encoding=PLAIN;
# 请注意，如标识符部分所述，a*b包含特殊字符，需要用``括起来使用
# create timeseries root.sg.a*b with datatype=FLOAT,encoding=PLAIN 是错误用法

# 创建时间序列 root.sg.a
create timeseries root.sg.a with datatype=FLOAT,encoding=PLAIN;

# 创建时间序列 root.sg.b
create timeseries root.sg.b with datatype=FLOAT,encoding=PLAIN;

# 查询时间序列 root.sg.`a*b`
select `a*b` from root.sg
# 其结果集表头为
|Time|root.sg.a*b|

# 查询时间序列 root.sg.a 和 root.sg.b的乘积
select a*b from root.sg
# 其结果集表头为
|Time|root.sg.a * root.sg.b|
```

## 标识符

路径结点名不为通配符时，使用方法和标识符一致。**在 SQL 中需要使用反引号引用的路径结点，在结果集中也会用反引号引起。**

需要使用反引号进行引用的部分特殊情况示例：

- 创建时间序列时，如下情况需要使用反引号对特殊节点名进行引用：

```SQL
# 路径结点名中包含特殊字符，时间序列各结点为["root","sg","www.`baidu.com"]
create timeseries root.sg.`www.``baidu.com`.a with datatype=FLOAT,encoding=PLAIN;

# 路径结点名为实数
create timeseries root.sg.`111` with datatype=FLOAT,encoding=PLAIN;
```

依次执行示例中语句后，执行 show timeseries，结果如下：

```SQL
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                 timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
|            root.sg.`111`.a| null|      root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|
|root.sg.`www.``baidu.com`.a| null|      root.sg|   FLOAT|   PLAIN|     SNAPPY|null|      null|
+---------------------------+-----+-------------+--------+--------+-----------+----+----------+
```

- 插入数据时，如下情况需要使用反引号对特殊节点名进行引用：

```SQL
# 路径结点名中包含特殊字符
insert into root.sg.`www.``baidu.com`(timestamp, a) values(1, 2);

# 路径结点名为实数
insert into root.sg(timestamp, `111`) values (1, 2);
```

- 查询数据时，如下情况需要使用反引号对特殊节点名进行引用：

```SQL
# 路径结点名中包含特殊字符
select a from root.sg.`www.``baidu.com`;

# 路径结点名为实数
select `111` from root.sg
```

结果集分别为：

```SQL
# select a from root.sg.`www.``baidu.com` 结果集
+-----------------------------+---------------------------+
|                         Time|root.sg.`www.``baidu.com`.a|
+-----------------------------+---------------------------+
|1970-01-01T08:00:00.001+08:00|                        2.0|
+-----------------------------+---------------------------+

# select `111` from root.sg 结果集
+-----------------------------+-------------+
|                         Time|root.sg.`111`|
+-----------------------------+-------------+
|1970-01-01T08:00:00.001+08:00|          2.0|
+-----------------------------+-------------+
```
