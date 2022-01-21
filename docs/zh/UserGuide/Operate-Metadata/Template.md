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

# 元数据模板

IoTDB 支持元数据模板功能，实现同类型不同实体的物理量元数据共享，减少元数据内存占用，同时简化同类型实体的管理。

注：以下语句中的 `schema` 关键字可以省略。

## 创建元数据模板

创建元数据模板的 SQL 语法如下：

```sql
CREATE SCHEMA? TEMPLATE <templateName> ALIGNED '(' templateMeasurementClause [',' templateMeasurementClause]+ ')'

templateMeasurementClause
    : <measurementId> <attributeClauses> #单个物理量
    | <deviceId> ALIGNED '(' <measurementId> <attributeClauses> [',' <measurementId> <attributeClauses>]+ ')'  #一组对齐的物理量
    ;
```

**示例1：** 创建包含两个非对齐序列的元数据模板

```shell
IoTDB> create schema template temp1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

**示例2：** 创建包含一组对齐序列的元数据模板

```shell
IoTDB> create schema template temp2 aligned (lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla)
```

**示例3：** 创建混合对齐序列和非对齐序列的元数据模板

```shell
IoTDB> create schema template temp3 (GPS aligned (lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla compression=SNAPPY), status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

其中，`GPS` 设备下的物理量 `lat` 和 `lon` 是对齐的。

## 挂载元数据模板

挂载元数据模板的 SQL 语句如下所示：

```shell
IoTDB> set schema template temp1 to root.ln.wf01
```

挂载好元数据模板后，即可进行数据的写入。例如存储组为 root.ln，模板 temp1 被挂载到了节点 root.ln.wf01，那么可直接向时间序列（如root.ln.wf01.GPS.lat和root.ln.wf01.status）写入时间序列数据，该时间序列已可被当作正常创建的序列使用。

**注意**：在插入数据之前，模板定义的时间序列不会被创建。可以使用如下SQL语句在插入数据前创建时间序列：

```shell
IoTDB> create timeseries of schema template on root.ln.wf01
```

**示例：** 执行以下语句
```shell
set schema template temp1 to root.sg1.d1
set schema template temp2 to root.sg1.d2
set schema template temp3 to root.sg1.d3
create timeseries of schema template on root.sg1.d1
create timeseries of schema template on root.sg1.d2
create timeseries of schema template on root.sg1.d3
```

查看此时的时间序列：
```sql
show timeseries root.sg1.**
```

```shell
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+
|             timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.sg1.d1.temperature| null|     root.sg1|   FLOAT|     RLE|     SNAPPY|null|      null|
|     root.sg1.d1.status| null|     root.sg1| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
|        root.sg1.d2.lon| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|
|        root.sg1.d2.lat| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|
|    root.sg1.d3.GPS.lon| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|
|    root.sg1.d3.GPS.lat| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|
|     root.sg1.d3.status| null|     root.sg1| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+
```

查看此时的设备：
```sql
show devices root.sg1.**
```

```shell
+---------------+---------+
|        devices|isAligned|
+---------------+---------+
|    root.sg1.d1|    false|
|    root.sg1.d2|     true|
|    root.sg1.d3|    false|
|root.sg1.d3.GPS|     true|
+---------------+---------+
```

## 查看元数据模板

- 查看所有元数据模板

SQL 语句如下所示：

```shell
IoTDB> show schema templates
```

执行结果如下：
```shell
+-------------+
|template name|
+-------------+
|        temp2|
|        temp3|
|        temp1|
+-------------+
```

- 查看某个元数据模板下的物理量

SQL 语句如下所示：

```shell
IoTDB> show nodes in schema template temp3
```

执行结果如下：
```shell
+-----------+--------+--------+-----------+
|child nodes|dataType|encoding|compression|
+-----------+--------+--------+-----------+
|    GPS.lon|   FLOAT| GORILLA|     SNAPPY|
|    GPS.lat|   FLOAT| GORILLA|     SNAPPY|
|     status| BOOLEAN|   PLAIN|     SNAPPY|
+-----------+--------+--------+-----------+
```

- 查看挂载了某个元数据模板的路径前缀

```shell
IoTDB> show paths set schema template temp1
```

执行结果如下：
```shell
+------------+
| child paths|
+------------+
|root.ln.wf01|
+------------+
```

- 查看使用了某个元数据模板（即序列已创建）的路径前缀

```shell
IoTDB> show paths using schema template temp1
```

执行结果如下：
```shell
+------------+
| child paths|
+------------+
|root.ln.wf01|
+------------+
```

## 卸载元数据模板

卸载元数据模板的 SQL 语句如下所示：

```shell
IoTDB> unset schema template temp1 from root.beijing
```

**注意**：目前不支持从曾经使用模板插入数据后（即使数据已被删除）的实体中卸载模板。

## 删除元数据模板

删除元数据模板的 SQL 语句如下所示：

```shell
IoTDB> drop schema template temp1
```

**注意**：不支持删除已经挂载的模板。
