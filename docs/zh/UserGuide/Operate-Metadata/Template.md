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

## 元数据模板

IoTDB 支持元数据模板功能，实现同类型不同实体的物理量元数据共享，减少元数据内存占用，同时简化同类型实体的管理。

注：以下语句中的 `schema` 关键字可以省略。

### 创建元数据模板

创建元数据模板的 SQL 语法如下：

```sql
CREATE SCHEMA TEMPLATE <templateName> ALIGNED? '(' <measurementId> <attributeClauses> [',' <measurementId> <attributeClauses>]+ ')'
```

**示例1：** 创建包含两个非对齐序列的元数据模板

```shell
IoTDB> create schema template t1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)
```

**示例2：** 创建包含一组对齐序列的元数据模板

```shell
IoTDB> create schema template t2 aligned (lat FLOAT encoding=Gorilla, lon FLOAT encoding=Gorilla)
```

其中，物理量 `lat` 和 `lon` 是对齐的。

### 挂载元数据模板

元数据模板在创建后，需执行挂载操作，方可用于相应路径下的序列创建与数据写入。

**挂载模板前，需确保相关数据库已经创建。**

**推荐将模板挂载在 database 节点上，不建议将模板挂载到 database 上层的节点上。**

挂载元数据模板的 SQL 语句如下所示：

```shell
IoTDB> set schema template t1 to root.sg1.d1
```

### 激活元数据模板

挂载好元数据模板后，且系统开启自动注册序列功能的情况下，即可直接进行数据的写入。例如 database 为 root.sg1，模板 t1 被挂载到了节点 root.sg1.d1，那么可直接向时间序列（如 root.sg1.d1.temperature 和 root.sg1.d1.status）写入时间序列数据，该时间序列已可被当作正常创建的序列使用。

**注意**：在插入数据之前或系统未开启自动注册序列功能，模板定义的时间序列不会被创建。可以使用如下SQL语句在插入数据前创建时间序列即激活模板：

```shell
IoTDB> create timeseries of schema template on root.sg1.d1
```

**示例：** 执行以下语句
```shell
IoTDB> set schema template t1 to root.sg1.d1
IoTDB> set schema template t2 to root.sg1.d2
IoTDB> create timeseries of schema template on root.sg1.d1
IoTDB> create timeseries of schema template on root.sg1.d2
```

查看此时的时间序列：
```sql
show timeseries root.sg1.**
```

```shell
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+--------+-------------------+
|             timeseries|alias|     database|dataType|encoding|compression|tags|attributes|deadband|deadband parameters|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+--------+-------------------+
|root.sg1.d1.temperature| null|     root.sg1|   FLOAT|     RLE|     SNAPPY|null|      null|    null|               null|
|     root.sg1.d1.status| null|     root.sg1| BOOLEAN|   PLAIN|     SNAPPY|null|      null|    null|               null|
|        root.sg1.d2.lon| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|    null|               null|
|        root.sg1.d2.lat| null|     root.sg1|   FLOAT| GORILLA|     SNAPPY|null|      null|    null|               null|
+-----------------------+-----+-------------+--------+--------+-----------+----+----------+--------+-------------------+
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
+---------------+---------+
```

### 查看元数据模板

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
|           t2|
|           t1|
+-------------+
```

- 查看某个元数据模板下的物理量

SQL 语句如下所示：

```shell
IoTDB> show nodes in schema template t1
```

执行结果如下：
```shell
+-----------+--------+--------+-----------+
|child nodes|dataType|encoding|compression|
+-----------+--------+--------+-----------+
|temperature|   FLOAT|     RLE|     SNAPPY|
|     status| BOOLEAN|   PLAIN|     SNAPPY|
+-----------+--------+--------+-----------+
```

- 查看挂载了某个元数据模板的路径

```shell
IoTDB> show paths set schema template t1
```

执行结果如下：
```shell
+-----------+
|child paths|
+-----------+
|root.sg1.d1|
+-----------+
```

- 查看使用了某个元数据模板的路径（即模板在该路径上已激活，序列已创建）

```shell
IoTDB> show paths using schema template t1
```

执行结果如下：
```shell
+-----------+
|child paths|
+-----------+
|root.sg1.d1|
+-----------+
```

### 解除元数据模板

若需删除模板表示的某一组时间序列，可采用解除模板操作，SQL语句如下所示：

```shell
IoTDB> delete timeseries of schema template t1 from root.sg1.d1
```

或

```shell
IoTDB> deactivate schema template t1 from root.sg1.d1
```

解除操作支持批量处理，SQL语句如下所示：

```shell
IoTDB> delete timeseries of schema template t1 from root.sg1.*, root.sg2.*
```

或

```shell
IoTDB> deactivate schema template t1 from root.sg1.*, root.sg2.*
```

若解除命令不指定模板名称，则会将给定路径涉及的所有模板使用情况均解除。

### 卸载元数据模板

卸载元数据模板的 SQL 语句如下所示：

```shell
IoTDB> unset schema template t1 from root.sg1.d1
```

**注意**：不支持卸载仍处于激活状态的模板，需保证执行卸载操作前解除对该模板的所有使用，即删除所有该模板表示的序列。

### 删除元数据模板

删除元数据模板的 SQL 语句如下所示：

```shell
IoTDB> drop schema template t1
```

**注意**：不支持删除已经挂载的模板，需在删除操作前保证该模板卸载成功。
