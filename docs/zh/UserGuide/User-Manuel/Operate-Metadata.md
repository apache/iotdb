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

# 元数据操作
## 数据库管理

数据库（Database）可以被视为关系数据库中的Database。

### 创建数据库

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

### 查看数据库

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

### 删除数据库

用户可以使用`DELETE DATABASE <PathPattern>`语句删除该路径模式匹配的所有的数据库。在删除的过程中，需要注意的是数据库的数据也会被删除。

```
IoTDB > DELETE DATABASE root.ln
IoTDB > DELETE DATABASE root.sgcc
// 删除所有数据，时间序列以及数据库
IoTDB > DELETE DATABASE root.**
```

### 统计数据库数量

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

### TTL

IoTDB 支持对 database 级别设置数据存活时间（TTL），这使得 IoTDB 可以定期、自动地删除一定时间之前的数据。合理使用 TTL
可以帮助您控制 IoTDB 占用的总磁盘空间以避免出现磁盘写满等异常。并且，随着文件数量的增多，查询性能往往随之下降，
内存占用也会有所提高。及时地删除一些较老的文件有助于使查询性能维持在一个较高的水平和减少内存资源的占用。

TTL的默认单位为毫秒，如果配置文件中的时间精度修改为其他单位，设置ttl时仍然使用毫秒单位。

#### 设置 TTL

设置 TTL 的 SQL 语句如下所示：
```
IoTDB> set ttl to root.ln 3600000
```
这个例子表示在`root.ln`数据库中，只有3600000毫秒，即最近一个小时的数据将会保存，旧数据会被移除或不可见。
```
IoTDB> set ttl to root.sgcc.** 3600000
```
支持给某一路径下的 database 设置TTL，这个例子表示`root.sgcc`路径下的所有 database 设置TTL。
```
IoTDB> set ttl to root.** 3600000
```
表示给所有 database 设置TTL。

#### 取消 TTL

取消 TTL 的 SQL 语句如下所示：

```
IoTDB> unset ttl to root.ln
```

取消设置 TTL 后， database `root.ln`中所有的数据都会被保存。
```
IoTDB> unset ttl to root.sgcc.**
```

取消设置`root.sgcc`路径下的所有 database 的 TTL 。
```
IoTDB> unset ttl to root.**
```

取消设置所有 database 的 TTL 。

#### 显示 TTL

显示 TTL 的 SQL 语句如下所示：

```
IoTDB> SHOW ALL TTL
IoTDB> SHOW TTL ON StorageGroupNames
```

SHOW ALL TTL 这个例子会给出所有 database 的 TTL。
SHOW TTL ON root.ln,root.sgcc,root.DB 这个例子会显示指定的三个 database 的 TTL。
注意：没有设置 TTL 的 database 的 TTL 将显示为 null。

```
IoTDB> show all ttl
+-------------+-------+
|     database|ttl(ms)|
+-------------+-------+
|      root.ln|3600000|
|    root.sgcc|   null|
|      root.DB|3600000|
+-------------+-------+
```



### 设置异构数据库（进阶操作）

在熟悉 IoTDB 元数据建模的前提下，用户可以在 IoTDB 中设置异构的数据库，以便应对不同的生产需求。

目前支持的数据库异构参数有：

| 参数名                       | 参数类型    | 参数描述                      |
|---------------------------|---------|---------------------------|
| TTL                       | Long    | 数据库的 TTL                  |
| SCHEMA_REPLICATION_FACTOR | Integer | 数据库的元数据副本数                |
| DATA_REPLICATION_FACTOR   | Integer | 数据库的数据副本数                 |
| SCHEMA_REGION_GROUP_NUM   | Integer | 数据库的 SchemaRegionGroup 数量 |
| DATA_REGION_GROUP_NUM     | Integer | 数据库的 DataRegionGroup 数量   |

用户在配置异构参数时需要注意以下三点：
+ TTL 和 TIME_PARTITION_INTERVAL 必须为正整数。
+ SCHEMA_REPLICATION_FACTOR 和 DATA_REPLICATION_FACTOR 必须小于等于已部署的 DataNode 数量。
+ SCHEMA_REGION_GROUP_NUM 和 DATA_REGION_GROUP_NUM 的功能与 iotdb-common.properties 配置文件中的 
`schema_region_group_extension_policy` 和 `data_region_group_extension_policy` 参数相关，以 DATA_REGION_GROUP_NUM 为例：
若设置 `data_region_group_extension_policy=CUSTOM`，则 DATA_REGION_GROUP_NUM 将作为 Database 拥有的 DataRegionGroup 的数量；
若设置 `data_region_group_extension_policy=AUTO`，则 DATA_REGION_GROUP_NUM 将作为 Database 拥有的 DataRegionGroup 的配额下界，即当该 Database 开始写入数据时，将至少拥有此数量的 DataRegionGroup。

用户可以在创建 Database 时设置任意异构参数，或在单机/分布式 IoTDB 运行时调整部分异构参数。

#### 创建 Database 时设置异构参数

用户可以在创建 Database 时设置上述任意异构参数，SQL 语句如下所示：

```
CREATE DATABASE prefixPath (WITH databaseAttributeClause (COMMA? databaseAttributeClause)*)?
```

例如：
```
CREATE DATABASE root.db WITH SCHEMA_REPLICATION_FACTOR=1, DATA_REPLICATION_FACTOR=3, SCHEMA_REGION_GROUP_NUM=1, DATA_REGION_GROUP_NUM=2;
```

#### 运行时调整异构参数

用户可以在 IoTDB 运行时调整部分异构参数，SQL 语句如下所示：

```
ALTER DATABASE prefixPath WITH databaseAttributeClause (COMMA? databaseAttributeClause)*
```

例如：
```
ALTER DATABASE root.db WITH SCHEMA_REGION_GROUP_NUM=1, DATA_REGION_GROUP_NUM=2;
```

注意，运行时只能调整下列异构参数：
+ SCHEMA_REGION_GROUP_NUM
+ DATA_REGION_GROUP_NUM

#### 查看异构数据库

用户可以查询每个 Database 的具体异构配置，SQL 语句如下所示：

```
SHOW DATABASES DETAILS prefixPath?
```

例如：

```
IoTDB> SHOW DATABASES DETAILS
+--------+--------+-----------------------+---------------------+---------------------+--------------------+-----------------------+-----------------------+------------------+---------------------+---------------------+
|Database|     TTL|SchemaReplicationFactor|DataReplicationFactor|TimePartitionInterval|SchemaRegionGroupNum|MinSchemaRegionGroupNum|MaxSchemaRegionGroupNum|DataRegionGroupNum|MinDataRegionGroupNum|MaxDataRegionGroupNum|
+--------+--------+-----------------------+---------------------+---------------------+--------------------+-----------------------+-----------------------+------------------+---------------------+---------------------+
|root.db1|    null|                      1|                    3|            604800000|                   0|                      1|                      1|                 0|                    2|                    2|
|root.db2|86400000|                      1|                    1|            604800000|                   0|                      1|                      1|                 0|                    2|                    2|
|root.db3|    null|                      1|                    1|            604800000|                   0|                      1|                      1|                 0|                    2|                    2|
+--------+--------+-----------------------+---------------------+---------------------+--------------------+-----------------------+-----------------------+------------------+---------------------+---------------------+
Total line number = 3
It costs 0.058s
```

各列查询结果依次为：
+ 数据库名称
+ 数据库的 TTL
+ 数据库的元数据副本数
+ 数据库的数据副本数
+ 数据库的时间分区间隔
+ 数据库当前拥有的 SchemaRegionGroup 数量
+ 数据库需要拥有的最小 SchemaRegionGroup 数量
+ 数据库允许拥有的最大 SchemaRegionGroup 数量
+ 数据库当前拥有的 DataRegionGroup 数量
+ 数据库需要拥有的最小 DataRegionGroup 数量
+ 数据库允许拥有的最大 DataRegionGroup 数量


## 元数据模板管理

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

**模板挂载路径下禁止创建普通序列，已创建了普通序列的前缀路径上不允许挂载模板。**

挂载元数据模板的 SQL 语句如下所示：

```shell
IoTDB> set schema template t1 to root.sg1.d1
```

### 激活元数据模板

挂载好元数据模板后，且系统开启自动注册序列功能的情况下，即可直接进行数据的写入。例如 database 为 root.sg1，模板 t1 被挂载到了节点 root.sg1.d1，那么可直接向时间序列（如 root.sg1.d1.temperature 和 root.sg1.d1.status）写入时间序列数据，该时间序列已可被当作正常创建的序列使用。

**注意**：在插入数据之前或系统未开启自动注册序列功能，模板定义的时间序列不会被创建。可以使用如下SQL语句在插入数据前创建时间序列即激活模板：

```shell
IoTDB> create timeseries using schema template on root.sg1.d1
```

**示例：** 执行以下语句
```shell
IoTDB> set schema template t1 to root.sg1.d1
IoTDB> set schema template t2 to root.sg1.d2
IoTDB> create timeseries using schema template on root.sg1.d1
IoTDB> create timeseries using schema template on root.sg1.d2
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

### 修改元数据模板

在需要新增物理量的场景中，可以通过修改元数据模板来给所有已激活该模板的设备新增物理量。

修改元数据模板的 SQL 语句如下所示：

```shell
IoTDB> alter schema template t1 add (speed FLOAT encoding=RLE, FLOAT TEXT encoding=PLAIN compression=SNAPPY)
```

**向已挂载模板的路径下的设备中写入数据，若写入请求中的物理量不在模板中，将自动扩展模板。**


## 时间序列管理

### 创建时间序列

根据建立的数据模型，我们可以分别在两个存储组中创建相应的时间序列。创建时间序列的 SQL 语句如下所示：

```
IoTDB > create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE
IoTDB > create timeseries root.ln.wf02.wt02.hardware with datatype=TEXT,encoding=PLAIN
IoTDB > create timeseries root.ln.wf02.wt02.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.temperature with datatype=FLOAT,encoding=RLE
```

从 v0.13 起，可以使用简化版的 SQL 语句创建时间序列：

```
IoTDB > create timeseries root.ln.wf01.wt01.status BOOLEAN encoding=PLAIN
IoTDB > create timeseries root.ln.wf01.wt01.temperature FLOAT encoding=RLE
IoTDB > create timeseries root.ln.wf02.wt02.hardware TEXT encoding=PLAIN
IoTDB > create timeseries root.ln.wf02.wt02.status BOOLEAN encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.status BOOLEAN encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.temperature FLOAT encoding=RLE
```

需要注意的是，当创建时间序列时指定的编码方式与数据类型不对应时，系统会给出相应的错误提示，如下所示：
```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
error: encoding TS_2DIFF does not support BOOLEAN
```

详细的数据类型与编码方式的对应列表请参见 [编码方式](../Basic-Concept/Encoding.md)。

### 创建对齐时间序列

创建一组对齐时间序列的SQL语句如下所示：

```
IoTDB> CREATE ALIGNED TIMESERIES root.ln.wf01.GPS(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT encoding=PLAIN compressor=SNAPPY) 
```

一组对齐序列中的序列可以有不同的数据类型、编码方式以及压缩方式。

对齐的时间序列也支持设置别名、标签、属性。

### 删除时间序列

我们可以使用`(DELETE | DROP) TimeSeries <PathPattern>`语句来删除我们之前创建的时间序列。SQL 语句如下所示：

```
IoTDB> delete timeseries root.ln.wf01.wt01.status
IoTDB> delete timeseries root.ln.wf01.wt01.temperature, root.ln.wf02.wt02.hardware
IoTDB> delete timeseries root.ln.wf02.*
IoTDB> drop timeseries root.ln.wf02.*
```

### 查看时间序列

* SHOW LATEST? TIMESERIES pathPattern? timeseriesWhereClause? limitClause?

  SHOW TIMESERIES 中可以有四种可选的子句，查询结果为这些时间序列的所有信息

时间序列信息具体包括：时间序列路径名，database，Measurement 别名，数据类型，编码方式，压缩方式，属性和标签。

示例：

* SHOW TIMESERIES

  展示系统中所有的时间序列信息

* SHOW TIMESERIES <`Path`>

  返回给定路径的下的所有时间序列信息。其中 `Path` 需要为一个时间序列路径或路径模式。例如，分别查看`root`路径和`root.ln`路径下的时间序列，SQL 语句如下所示：

```
IoTDB> show timeseries root.**
IoTDB> show timeseries root.ln.**
```

执行结果分别为：

```
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
|                     timeseries|   alias|     database|dataType|encoding|compression|                                       tags|                                              attributes|deadband|deadband parameters|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
|root.sgcc.wf03.wt01.temperature|    null|    root.sgcc|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|    null|               null|
|     root.sgcc.wf03.wt01.status|    null|    root.sgcc| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|    null|               null|
|             root.turbine.d1.s1|newAlias| root.turbine|   FLOAT|     RLE|     SNAPPY|{"newTag1":"newV1","tag4":"v4","tag3":"v3"}|{"attr2":"v2","attr1":"newV1","attr4":"v4","attr3":"v3"}|    null|               null|
|     root.ln.wf02.wt02.hardware|    null|      root.ln|    TEXT|   PLAIN|     SNAPPY|                                       null|                                                    null|    null|               null|
|       root.ln.wf02.wt02.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|    null|               null|
|  root.ln.wf01.wt01.temperature|    null|      root.ln|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|    null|               null|
|       root.ln.wf01.wt01.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|    null|               null|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
Total line number = 7
It costs 0.016s

+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+--------+-------------------+
|                   timeseries|alias|     database|dataType|encoding|compression|tags|attributes|deadband|deadband parameters|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+--------+-------------------+
|   root.ln.wf02.wt02.hardware| null|      root.ln|    TEXT|   PLAIN|     SNAPPY|null|      null|    null|               null|
|     root.ln.wf02.wt02.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|    null|               null|
|root.ln.wf01.wt01.temperature| null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|    null|               null|
|     root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|    null|               null|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+--------+-------------------+
Total line number = 4
It costs 0.004s
```

* SHOW TIMESERIES LIMIT INT OFFSET INT

  只返回从指定下标开始的结果，最大返回条数被 LIMIT 限制，用于分页查询。例如：

```
show timeseries root.ln.** limit 10 offset 10
```

* SHOW TIMESERIES WHERE TIMESERIES contains 'containStr'

  对查询结果集根据 timeseries 名称进行字符串模糊匹配过滤。例如：

```
show timeseries root.ln.** where timeseries contains 'wf01.wt'
```

执行结果为：

```
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
|                     timeseries|   alias|     database|dataType|encoding|compression|                                       tags|                                              attributes|deadband|deadband parameters|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
|  root.ln.wf01.wt01.temperature|    null|      root.ln|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|    null|               null|
|       root.ln.wf01.wt01.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|    null|               null|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
Total line number = 2
It costs 0.016s
```

* SHOW TIMESERIES WHERE DataType=type

  对查询结果集根据时间序列数据类型进行过滤。例如：

```
show timeseries root.ln.** where dataType=FLOAT
```

执行结果为:

```
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
|                     timeseries|   alias|     database|dataType|encoding|compression|                                       tags|                                              attributes|deadband|deadband parameters|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
|root.sgcc.wf03.wt01.temperature|    null|    root.sgcc|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|    null|               null|
|             root.turbine.d1.s1|newAlias| root.turbine|   FLOAT|     RLE|     SNAPPY|{"newTag1":"newV1","tag4":"v4","tag3":"v3"}|{"attr2":"v2","attr1":"newV1","attr4":"v4","attr3":"v3"}|    null|               null|
|  root.ln.wf01.wt01.temperature|    null|      root.ln|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|    null|               null|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
Total line number = 3
It costs 0.016s

```


* SHOW LATEST TIMESERIES

  表示查询出的时间序列需要按照最近插入时间戳降序排列


需要注意的是，当查询路径不存在时，系统会返回 0 条时间序列。

### 统计时间序列总数

IoTDB 支持使用`COUNT TIMESERIES<Path>`来统计一条路径中的时间序列个数。SQL 语句如下所示：

* 可以通过 `WHERE` 条件对时间序列名称进行字符串模糊匹配，语法为： `COUNT TIMESERIES <Path> WHERE TIMESERIES contains 'containStr'` 。
* 可以通过 `WHERE` 条件对时间序列数据类型进行过滤，语法为： `COUNT TIMESERIES <Path> WHERE DataType=<DataType>'`。
* 可以通过 `WHERE` 条件对标签点进行过滤，语法为： `COUNT TIMESERIES <Path> WHERE TAGS(key)='value'` 或 `COUNT TIMESERIES <Path> WHERE TAGS(key) contains 'value'`。
* 可以通过定义`LEVEL`来统计指定层级下的时间序列个数。这条语句可以用来统计每一个设备下的传感器数量，语法为：`COUNT TIMESERIES <Path> GROUP BY LEVEL=<INTEGER>`。

```
IoTDB > COUNT TIMESERIES root.**
IoTDB > COUNT TIMESERIES root.ln.**
IoTDB > COUNT TIMESERIES root.ln.*.*.status
IoTDB > COUNT TIMESERIES root.ln.wf01.wt01.status
IoTDB > COUNT TIMESERIES root.** WHERE TIMESERIES contains 'sgcc' 
IoTDB > COUNT TIMESERIES root.** WHERE DATATYPE = INT64
IoTDB > COUNT TIMESERIES root.** WHERE TAGS(unit) contains 'c' 
IoTDB > COUNT TIMESERIES root.** WHERE TAGS(unit) = 'c' 
IoTDB > COUNT TIMESERIES root.** WHERE TIMESERIES contains 'sgcc' group by level = 1
```

例如有如下时间序列（可以使用`show timeseries`展示所有时间序列）：

```
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
|                     timeseries|   alias|     database|dataType|encoding|compression|                                       tags|                                              attributes|deadband|deadband parameters|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
|root.sgcc.wf03.wt01.temperature|    null|    root.sgcc|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|    null|               null|
|     root.sgcc.wf03.wt01.status|    null|    root.sgcc| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|    null|               null|
|             root.turbine.d1.s1|newAlias| root.turbine|   FLOAT|     RLE|     SNAPPY|{"newTag1":"newV1","tag4":"v4","tag3":"v3"}|{"attr2":"v2","attr1":"newV1","attr4":"v4","attr3":"v3"}|    null|               null|
|     root.ln.wf02.wt02.hardware|    null|      root.ln|    TEXT|   PLAIN|     SNAPPY|                               {"unit":"c"}|                                                    null|    null|               null|
|       root.ln.wf02.wt02.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                    {"description":"test1"}|                                                    null|    null|               null|
|  root.ln.wf01.wt01.temperature|    null|      root.ln|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|    null|               null|
|       root.ln.wf01.wt01.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|    null|               null|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+--------+-------------------+
Total line number = 7
It costs 0.004s
```

那么 Metadata Tree 如下所示：

<img style="width:100%; max-width:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/github/69792176-1718f400-1201-11ea-861a-1a83c07ca144.jpg">

可以看到，`root`被定义为`LEVEL=0`。那么当你输入如下语句时：

```
IoTDB > COUNT TIMESERIES root.** GROUP BY LEVEL=1
IoTDB > COUNT TIMESERIES root.ln.** GROUP BY LEVEL=2
IoTDB > COUNT TIMESERIES root.ln.wf01.* GROUP BY LEVEL=2
```

你将得到以下结果：

```
IoTDB> COUNT TIMESERIES root.** GROUP BY LEVEL=1
+------------+-----------------+
|      column|count(timeseries)|
+------------+-----------------+
|   root.sgcc|                2|
|root.turbine|                1|
|     root.ln|                4|
+------------+-----------------+
Total line number = 3
It costs 0.002s

IoTDB > COUNT TIMESERIES root.ln.** GROUP BY LEVEL=2
+------------+-----------------+
|      column|count(timeseries)|
+------------+-----------------+
|root.ln.wf02|                2|
|root.ln.wf01|                2|
+------------+-----------------+
Total line number = 2
It costs 0.002s

IoTDB > COUNT TIMESERIES root.ln.wf01.* GROUP BY LEVEL=2
+------------+-----------------+
|      column|count(timeseries)|
+------------+-----------------+
|root.ln.wf01|                2|
+------------+-----------------+
Total line number = 1
It costs 0.002s
```

> 注意：时间序列的路径只是过滤条件，与 level 的定义无关。

### 标签点管理

我们可以在创建时间序列的时候，为它添加别名和额外的标签和属性信息。

标签和属性的区别在于：

* 标签可以用来查询时间序列路径，会在内存中维护标点到时间序列路径的倒排索引：标签 -> 时间序列路径
* 属性只能用时间序列路径来查询：时间序列路径 -> 属性

所用到的扩展的创建时间序列的 SQL 语句如下所示：
```
create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)
```

括号里的`temprature`是`s1`这个传感器的别名。
我们可以在任何用到`s1`的地方，将其用`temprature`代替，这两者是等价的。

> IoTDB 同时支持在查询语句中 [使用 AS 函数](../Reference/SQL-Reference.md#数据管理语句) 设置别名。二者的区别在于：AS 函数设置的别名用于替代整条时间序列名，且是临时的，不与时间序列绑定；而上文中的别名只作为传感器的别名，与其绑定且可与原传感器名等价使用。

> 注意：额外的标签和属性信息总的大小不能超过`tag_attribute_total_size`.

 * 标签点属性更新
创建时间序列后，我们也可以对其原有的标签点属性进行更新，主要有以下六种更新方式：
* 重命名标签或属性
```
ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO newTag1
```
* 重新设置标签或属性的值
```
ALTER timeseries root.turbine.d1.s1 SET newTag1=newV1, attr1=newV1
```
* 删除已经存在的标签或属性
```
ALTER timeseries root.turbine.d1.s1 DROP tag1, tag2
```
* 添加新的标签
```
ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3=v3, tag4=v4
```
* 添加新的属性
```
ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES attr3=v3, attr4=v4
```
* 更新插入别名，标签和属性
> 如果该别名，标签或属性原来不存在，则插入，否则，用新值更新原来的旧值
```
ALTER timeseries root.turbine.d1.s1 UPSERT ALIAS=newAlias TAGS(tag2=newV2, tag3=v3) ATTRIBUTES(attr3=v3, attr4=v4)
```

* 使用标签作为过滤条件查询时间序列，使用 TAGS(tagKey) 来标识作为过滤条件的标签
```
SHOW TIMESERIES (<`PathPattern`>)? timeseriesWhereClause
```

返回给定路径的下的所有满足条件的时间序列信息，SQL 语句如下所示：

```
ALTER timeseries root.ln.wf02.wt02.hardware ADD TAGS unit=c
ALTER timeseries root.ln.wf02.wt02.status ADD TAGS description=test1
show timeseries root.ln.** where TAGS(unit)='c'
show timeseries root.ln.** where TAGS(description) contains 'test1'
```

执行结果分别为：

```
+--------------------------+-----+-------------+--------+--------+-----------+------------+----------+--------+-------------------+
|                timeseries|alias|     database|dataType|encoding|compression|        tags|attributes|deadband|deadband parameters|
+--------------------------+-----+-------------+--------+--------+-----------+------------+----------+--------+-------------------+
|root.ln.wf02.wt02.hardware| null|      root.ln|    TEXT|   PLAIN|     SNAPPY|{"unit":"c"}|      null|    null|               null|
+--------------------------+-----+-------------+--------+--------+-----------+------------+----------+--------+-------------------+
Total line number = 1
It costs 0.005s

+------------------------+-----+-------------+--------+--------+-----------+-----------------------+----------+--------+-------------------+
|              timeseries|alias|     database|dataType|encoding|compression|                   tags|attributes|deadband|deadband parameters|
+------------------------+-----+-------------+--------+--------+-----------+-----------------------+----------+--------+-------------------+
|root.ln.wf02.wt02.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|{"description":"test1"}|      null|    null|               null|
+------------------------+-----+-------------+--------+--------+-----------+-----------------------+----------+--------+-------------------+
Total line number = 1
It costs 0.004s
```

- 使用标签作为过滤条件统计时间序列数量

```
COUNT TIMESERIES (<`PathPattern`>)? timeseriesWhereClause
COUNT TIMESERIES (<`PathPattern`>)? timeseriesWhereClause GROUP BY LEVEL=<INTEGER>
```

返回给定路径的下的所有满足条件的时间序列的数量，SQL 语句如下所示：

```
count timeseries
count timeseries root.** where TAGS(unit)='c'
count timeseries root.** where TAGS(unit)='c' group by level = 2
```

执行结果分别为：

```
IoTDB> count timeseries
+-----------------+
|count(timeseries)|
+-----------------+
|                6|
+-----------------+
Total line number = 1
It costs 0.019s
IoTDB> count timeseries root.** where TAGS(unit)='c'
+-----------------+
|count(timeseries)|
+-----------------+
|                2|
+-----------------+
Total line number = 1
It costs 0.020s
IoTDB> count timeseries root.** where TAGS(unit)='c' group by level = 2
+--------------+-----------------+
|        column|count(timeseries)|
+--------------+-----------------+
|  root.ln.wf02|                2|
|  root.ln.wf01|                0|
|root.sgcc.wf03|                0|
+--------------+-----------------+
Total line number = 3
It costs 0.011s
```

> 注意，现在我们只支持一个查询条件，要么是等值条件查询，要么是包含条件查询。当然 where 子句中涉及的必须是标签值，而不能是属性值。

创建对齐时间序列

```
create aligned timeseries root.sg1.d1(s1 INT32 tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2), s2 DOUBLE tags(tag3=v3, tag4=v4) attributes(attr3=v3, attr4=v4))
```

执行结果如下：

```
IoTDB> show timeseries
+--------------+-----+-------------+--------+--------+-----------+-------------------------+---------------------------+--------+-------------------+
|    timeseries|alias|     database|dataType|encoding|compression|                     tags|                 attributes|deadband|deadband parameters|
+--------------+-----+-------------+--------+--------+-----------+-------------------------+---------------------------+--------+-------------------+
|root.sg1.d1.s1| null|     root.sg1|   INT32|     RLE|     SNAPPY|{"tag1":"v1","tag2":"v2"}|{"attr2":"v2","attr1":"v1"}|    null|               null|
|root.sg1.d1.s2| null|     root.sg1|  DOUBLE| GORILLA|     SNAPPY|{"tag4":"v4","tag3":"v3"}|{"attr4":"v4","attr3":"v3"}|    null|               null|
+--------------+-----+-------------+--------+--------+-----------+-------------------------+---------------------------+--------+-------------------+
```

支持查询：

```
IoTDB> show timeseries where TAGS(tag1)='v1'
+--------------+-----+-------------+--------+--------+-----------+-------------------------+---------------------------+--------+-------------------+
|    timeseries|alias|     database|dataType|encoding|compression|                     tags|                 attributes|deadband|deadband parameters|
+--------------+-----+-------------+--------+--------+-----------+-------------------------+---------------------------+--------+-------------------+
|root.sg1.d1.s1| null|     root.sg1|   INT32|     RLE|     SNAPPY|{"tag1":"v1","tag2":"v2"}|{"attr2":"v2","attr1":"v1"}|    null|               null|
+--------------+-----+-------------+--------+--------+-----------+-------------------------+---------------------------+--------+-------------------+
```

上述对时间序列标签、属性的更新等操作都支持。


## 路径查询

### 查看路径的所有子路径

```
SHOW CHILD PATHS pathPattern
```

可以查看此路径模式所匹配的所有路径的下一层的所有路径和它对应的节点类型，即pathPattern.*所匹配的路径及其节点类型。

节点类型：ROOT -> SG INTERNAL -> DATABASE -> INTERNAL -> DEVICE -> TIMESERIES

示例：

* 查询 root.ln 的下一层：show child paths root.ln

```
+------------+----------+
| child paths|node types|
+------------+----------+
|root.ln.wf01|  INTERNAL|
|root.ln.wf02|  INTERNAL|
+------------+----------+
Total line number = 2
It costs 0.002s
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

### 查看路径的下一级节点

```
SHOW CHILD NODES pathPattern
```

可以查看此路径模式所匹配的节点的下一层的所有节点。

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
+------------+
|count(nodes)|
+------------+
|           4|
+------------+
Total line number = 1
It costs 0.003s

+------------+
|count(nodes)|
+------------+
|           2|
+------------+
Total line number = 1
It costs 0.002s

+------------+
|count(nodes)|
+------------+
|           1|
+------------+
Total line number = 1
It costs 0.002s

+------------+
|count(nodes)|
+------------+
|           2|
+------------+
Total line number = 1
It costs 0.002s
```

> 注意：时间序列的路径只是过滤条件，与 level 的定义无关。

### 查看设备

* SHOW DEVICES pathPattern? (WITH DATABASE)? devicesWhereClause? limitClause? 

与 `Show Timeseries` 相似，IoTDB 目前也支持两种方式查看设备。

* `SHOW DEVICES` 语句显示当前所有的设备信息，等价于 `SHOW DEVICES root.**`。
* `SHOW DEVICES <PathPattern>` 语句规定了 `PathPattern`，返回给定的路径模式所匹配的设备信息。
* `WHERE` 条件中可以使用 `DEVICE contains 'xxx'`，根据 device 名称进行模糊查询。

SQL 语句如下所示：

```
IoTDB> show devices
IoTDB> show devices root.ln.**
IoTDB> show devices root.ln.** where device contains 't'
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

查看设备及其 database 信息，可以使用 `SHOW DEVICES WITH DATABASE` 语句。

* `SHOW DEVICES WITH DATABASE` 语句显示当前所有的设备信息和其所在的 database，等价于 `SHOW DEVICES root.**`。
* `SHOW DEVICES <PathPattern> WITH DATABASE` 语句规定了 `PathPattern`，返回给定的路径模式所匹配的设备信息和其所在的 database。

SQL 语句如下所示：

```
IoTDB> show devices with database
IoTDB> show devices root.ln.** with database
```

你可以获得如下数据：

```
+-------------------+-------------+---------+
|            devices|     database|isAligned|
+-------------------+-------------+---------+
|  root.ln.wf01.wt01|      root.ln|    false|
|  root.ln.wf02.wt02|      root.ln|    false|
|root.sgcc.wf03.wt01|    root.sgcc|    false|
|    root.turbine.d1| root.turbine|    false|
+-------------------+-------------+---------+
Total line number = 4
It costs 0.003s

+-----------------+-------------+---------+
|          devices|     database|isAligned|
+-----------------+-------------+---------+
|root.ln.wf01.wt01|      root.ln|    false|
|root.ln.wf02.wt02|      root.ln|    false|
+-----------------+-------------+---------+
Total line number = 2
It costs 0.001s
```

### 统计设备数量

* COUNT DEVICES \<PathPattern\>

上述语句用于统计设备的数量，同时允许指定`PathPattern` 用于统计匹配该`PathPattern` 的设备数量

SQL 语句如下所示：

```
IoTDB> show devices
IoTDB> count devices
IoTDB> count devices root.ln.**
```

你可以获得如下数据：

```
+-------------------+---------+
|            devices|isAligned|
+-------------------+---------+
|root.sgcc.wf03.wt03|    false|
|    root.turbine.d1|    false|
|  root.ln.wf02.wt02|    false|
|  root.ln.wf01.wt01|    false|
+-------------------+---------+
Total line number = 4
It costs 0.024s

+--------------+
|count(devices)|
+--------------+
|             4|
+--------------+
Total line number = 1
It costs 0.004s

+--------------+
|count(devices)|
+--------------+
|             2|
+--------------+
Total line number = 1
It costs 0.004s
```
