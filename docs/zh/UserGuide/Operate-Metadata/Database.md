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

# 数据库管理

数据库（Database）可以被视为关系数据库中的Database。

## 创建数据库

我们可以根据存储模型建立相应的数据库。如下所示：

```
IoTDB > CREATE DATABASE root.ln
```

需要注意的是，database 的父子节点都不能再设置 database。例如在已经有`root.ln`和`root.sgcc`这两个 database 的情况下，创建`root.ln.wf01` database 是不可行的。系统将给出相应的错误提示，如下所示：

```
IoTDB> CREATE DATABASE root.ln.wf01
Msg: 300: root.ln has already been created as database.
```
Database 节点名只支持中英文字符、数字、下划线的组合，如果想设置为纯数字或者包含其他字符，需要用反引号(``)把 database 名称引起来。

还需注意，如果在 Windows 系统上部署，database 名是大小写不敏感的。例如同时创建`root.ln` 和 `root.LN` 是不被允许的。

## 查看数据库

在 database 创建后，我们可以使用 [SHOW DATABASES](../Reference/SQL-Reference.md) 语句和 [SHOW DATABASES \<PathPattern>](../Reference/SQL-Reference.md) 来查看 database，SQL 语句如下所示：

```
IoTDB> show databases
IoTDB> show databases root.*
IoTDB> show databases root.**
```

执行结果为：

```
+-------------+----+-------------------------+-----------------------+-----------------------+
|     database| ttl|schema_replication_factor|data_replication_factor|time_partition_interval|
+-------------+----+-------------------------+-----------------------+-----------------------+
|    root.sgcc|null|                        2|                      2|                 604800|
|      root.ln|null|                        2|                      2|                 604800|
+-------------+----+-------------------------+-----------------------+-----------------------+
Total line number = 2
It costs 0.060s
```

## 删除数据库

用户可以使用`DELETE DATABASE <PathPattern>`语句删除该路径模式匹配的所有的数据库。在删除的过程中，需要注意的是数据库的数据也会被删除。

```
IoTDB > DELETE DATABASE root.ln
IoTDB > DELETE DATABASE root.sgcc
// 删除所有数据，时间序列以及数据库
IoTDB > DELETE DATABASE root.**
```

## 统计数据库数量

用户可以使用`COUNT DATABASES <PathPattern>`语句统计数据库的数量，允许指定`PathPattern` 用来统计匹配该`PathPattern` 的数据库的数量

SQL 语句如下所示：

```
IoTDB> show databases
IoTDB> count databases
IoTDB> count databases root.*
IoTDB> count databases root.sgcc.*
IoTDB> count databases root.sgcc
```

执行结果为：

```
+-------------+
|     database|
+-------------+
|    root.sgcc|
| root.turbine|
|      root.ln|
+-------------+
Total line number = 3
It costs 0.003s

+-------------+
|     Database|
+-------------+
|            3|
+-------------+
Total line number = 1
It costs 0.003s

+-------------+
|     Database|
+-------------+
|            3|
+-------------+
Total line number = 1
It costs 0.002s

+-------------+
|     Database|
+-------------+
|            0|
+-------------+
Total line number = 1
It costs 0.002s

+-------------+
|     database|
+-------------+
|            1|
+-------------+
Total line number = 1
It costs 0.002s
```
