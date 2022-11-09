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

# 存储组管理

存储组（Storage Group）可以被视为关系数据库中的Database。

## 创建存储组

我们可以根据存储模型建立相应的存储组。创建存储组支持两种 SQL 语句，如下所示：

```
IoTDB > CREATE DATABASE root.ln
IoTDB > create storage group root.sgcc
```

根据以上两条 SQL 语句，我们可以创建出两个存储组。

需要注意的是，存储组的父子节点都不能再设置存储组。例如在已经有`root.ln`和`root.sgcc`这两个存储组的情况下，创建`root.ln.wf01`存储组是不可行的。系统将给出相应的错误提示，如下所示：

```
IoTDB> CREATE DATABASE root.ln.wf01
Msg: 300: root.ln has already been created as database.
IoTDB> create storage group root.ln.wf01
Msg: 300: root.ln has already been created as database.
```
存储组节点名只支持中英文字符、数字、下划线的组合，如果想设置为纯数字或者包含其他字符，需要用反引号(``)把存储组名称引起来。

还需注意，如果在 Windows 系统上部署，存储组名是大小写不敏感的。例如同时创建`root.ln` 和 `root.LN` 是不被允许的。

## 查看存储组

在存储组创建后，我们可以使用 [SHOW DATABASES](../Reference/SQL-Reference.md) 语句和 [SHOW DATABASES \<PathPattern>](../Reference/SQL-Reference.md) 来查看存储组，SQL 语句如下所示：

```
IoTDB> show databases
IoTDB> show databases root.*
IoTDB> show databases root.**
```

执行结果为：

```
+-------------+----+-------------------------+-----------------------+-----------------------+
|storage group| ttl|schema_replication_factor|data_replication_factor|time_partition_interval|
+-------------+----+-------------------------+-----------------------+-----------------------+
|    root.sgcc|null|                        2|                      2|                 604800|
|      root.ln|null|                        2|                      2|                 604800|
+-------------+----+-------------------------+-----------------------+-----------------------+
Total line number = 2
It costs 0.060s
```

## 删除存储组

用户可以使用`DELETE STORAGE GROUP <PathPattern>`语句删除该路径模式匹配的所有的存储组。在删除的过程中，需要注意的是存储组的数据也会被删除。

```
IoTDB > DELETE STORAGE GROUP root.ln
IoTDB > DELETE STORAGE GROUP root.sgcc
// 删除所有数据，时间序列以及存储组
IoTDB > DELETE STORAGE GROUP root.**
```

## 统计存储组数量

用户可以使用`COUNT STORAGE GROUP <PathPattern>`语句统计存储组的数量，允许指定`PathPattern` 用来统计匹配该`PathPattern` 的存储组的数量

SQL 语句如下所示：

```
IoTDB> show databases
IoTDB> count storage group
IoTDB> count storage group root.*
IoTDB> count storage group root.sgcc.*
IoTDB> count storage group root.sgcc
```

执行结果为：

```
+-------------+
|storage group|
+-------------+
|    root.sgcc|
| root.turbine|
|      root.ln|
+-------------+
Total line number = 3
It costs 0.003s

+-------------+
|storage group|
+-------------+
|            3|
+-------------+
Total line number = 1
It costs 0.003s

+-------------+
|storage group|
+-------------+
|            3|
+-------------+
Total line number = 1
It costs 0.002s

+-------------+
|storage group|
+-------------+
|            0|
+-------------+
Total line number = 1
It costs 0.002s

+-------------+
|storage group|
+-------------+
|            1|
+-------------+
Total line number = 1
It costs 0.002s
```
