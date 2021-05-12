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

# IoTDB-SQL 语言

## 数据定义语言（DDL）

### 存储组管理

#### 创建存储组

我们可以根据存储模型建立相应的存储组。创建存储组的SQL语句如下所示：

```
IoTDB > set storage group to root.ln
IoTDB > set storage group to root.sgcc
```

根据以上两条SQL语句，我们可以创建出两个存储组。

需要注意的是，存储组的父子节点都不能再设置存储组。例如在已经有`root.ln`和`root.sgcc`这两个存储组的情况下，创建`root.ln.wf01`存储组是不可行的。系统将给出相应的错误提示，如下所示：

```
IoTDB> set storage group to root.ln.wf01
Msg: 300: root.ln has already been set to storage group.
```
存储组节点名只支持中英文字符、数字、下划线和中划线的组合。

还需注意，如果在Windows系统上部署，存储组名是大小写不敏感的。例如同时创建`root.ln` 和 `root.LN` 是不被允许的。

#### 查看存储组

在存储组创建后，我们可以使用[SHOW STORAGE GROUP](../Appendix/SQL-Reference.md)语句和[SHOW STORAGE GROUP \<PrefixPath>](../Appendix/SQL-Reference.md)来查看存储组，SQL语句如下所示：

```
IoTDB> show storage group
IoTDB> show storage group root.ln
```

执行结果为：

```
+-------------+
|storage group|
+-------------+
|    root.sgcc|
|      root.ln|
+-------------+
Total line number = 2
It costs 0.060s
```

#### 删除存储组

用户可以使用`DELETE STORAGE GROUP <PrefixPath>`语句删除该前缀路径下所有的存储组。在删除的过程中，需要注意的是存储组的数据也会被删除。

```
IoTDB > DELETE STORAGE GROUP root.ln
IoTDB > DELETE STORAGE GROUP root.sgcc
// 删除所有数据，时间序列以及存储组
IoTDB > DELETE STORAGE GROUP root.*
```
### 时间序列管理

#### 创建时间序列

根据建立的数据模型，我们可以分别在两个存储组中创建相应的时间序列。创建时间序列的SQL语句如下所示：

```
IoTDB > create timeseries root.ln.wf01.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.ln.wf01.wt01.temperature with datatype=FLOAT,encoding=RLE
IoTDB > create timeseries root.ln.wf02.wt02.hardware with datatype=TEXT,encoding=PLAIN
IoTDB > create timeseries root.ln.wf02.wt02.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.status with datatype=BOOLEAN,encoding=PLAIN
IoTDB > create timeseries root.sgcc.wf03.wt01.temperature with datatype=FLOAT,encoding=RLE
```

我们也可以创建**对齐**时间序列：

```
IoTDB > create aligned timeseries root.sg.d1.(s1 FLOAT, s2 INT32)
IoTDB > create aligned timeseries root.sg.d1.(s3 FLOAT, s4 INT32) with encoding=(RLE, Grollia), compression=SNAPPY
```

注意：对齐时间序列必须拥有相同的压缩方式。

需要注意的是，当创建时间序列时指定的编码方式与数据类型不对应时，系统会给出相应的错误提示，如下所示：
```
IoTDB> create timeseries root.ln.wf02.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=TS_2DIFF
error: encoding TS_2DIFF does not support BOOLEAN
```

详细的数据类型与编码方式的对应列表请参见[编码方式](../Data-Concept/Encoding.md)。

### 创建和挂载设备模板
```

IoTDB > set storage group root.beijing

// 创建设备模板
IoTDB > create device template temp1(
  (s1 INT32 with encoding=Gorilla, compression=SNAPPY),
  (s2 FLOAT with encoding=RLE, compression=SNAPPY)
 )

// 将设备模板挂载到root.beijing存储组上
IoTDB > set device template temp1 to root.beijing

```

#### 删除时间序列

我们可以使用`DELETE TimeSeries <PrefixPath>`语句来删除我们之前创建的时间序列。SQL语句如下所示：

```
IoTDB> delete timeseries root.ln.wf01.wt01.status
IoTDB> delete timeseries root.ln.wf01.wt01.temperature, root.ln.wf02.wt02.hardware
IoTDB> delete timeseries root.ln.wf02.*
```

对于**对齐**时间序列，我们可以通过括号来显式地删除整组序列：

```
IoTDB > delete timeseries root.sg.d1.(s1,s2)
```

注意：目前暂不支持删除部分对齐时间序列。

```
IoTDB > delete timeseries root.sg.d1.s1
error: Not support deleting part of aligned timeseies!
```


#### 查看时间序列

* SHOW LATEST? TIMESERIES prefixPath? showWhereClause? limitClause?

  SHOW TIMESERIES 中可以有四种可选的子句，查询结果为这些时间序列的所有信息

时间序列信息具体包括：时间序列路径名，存储组，Measurement别名，数据类型，编码方式，压缩方式，属性和标签。

示例：

* SHOW TIMESERIES

  展示系统中所有的时间序列信息

* SHOW TIMESERIES <`Path`>

  返回给定路径的下的所有时间序列信息。其中 `Path` 需要为一个前缀路径、带星路径或时间序列路径。例如，分别查看`root`路径和`root.ln`路径下的时间序列，SQL语句如下所示：

```
IoTDB> show timeseries root
IoTDB> show timeseries root.ln
```

执行结果分别为：

```
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
|                     timeseries|   alias|storage group|dataType|encoding|compression|                                       tags|                                              attributes|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
|root.sgcc.wf03.wt01.temperature|    null|    root.sgcc|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|
|     root.sgcc.wf03.wt01.status|    null|    root.sgcc| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
|             root.turbine.d1.s1|newAlias| root.turbine|   FLOAT|     RLE|     SNAPPY|{"newTag1":"newV1","tag4":"v4","tag3":"v3"}|{"attr2":"v2","attr1":"newV1","attr4":"v4","attr3":"v3"}|
|     root.ln.wf02.wt02.hardware|    null|      root.ln|    TEXT|   PLAIN|     SNAPPY|                                       null|                                                    null|
|       root.ln.wf02.wt02.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
|  root.ln.wf01.wt01.temperature|    null|      root.ln|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|
|       root.ln.wf01.wt01.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
Total line number = 7
It costs 0.016s

+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                   timeseries|alias|storage group|dataType|encoding|compression|tags|attributes|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|   root.ln.wf02.wt02.hardware| null|      root.ln|    TEXT|   PLAIN|     SNAPPY|null|      null|
|     root.ln.wf02.wt02.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
|root.ln.wf01.wt01.temperature| null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|
|     root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 4
It costs 0.004s
```

* SHOW TIMESERIES (<`PrefixPath`>)? WhereClause 
  
  返回给定路径的下的所有满足条件的时间序列信息，SQL语句如下所示：

```
ALTER timeseries root.ln.wf02.wt02.hardware ADD TAGS unit=c
ALTER timeseries root.ln.wf02.wt02.status ADD TAGS description=test1
show timeseries root.ln where unit=c
show timeseries root.ln where description contains 'test1'
```

执行结果分别为：

```
+--------------------------+-----+-------------+--------+--------+-----------+------------+----------+
|                timeseries|alias|storage group|dataType|encoding|compression|        tags|attributes|
+--------------------------+-----+-------------+--------+--------+-----------+------------+----------+
|root.ln.wf02.wt02.hardware| null|      root.ln|    TEXT|   PLAIN|     SNAPPY|{"unit":"c"}|      null|
+--------------------------+-----+-------------+--------+--------+-----------+------------+----------+
Total line number = 1
It costs 0.005s

+------------------------+-----+-------------+--------+--------+-----------+-----------------------+----------+
|              timeseries|alias|storage group|dataType|encoding|compression|                   tags|attributes|
+------------------------+-----+-------------+--------+--------+-----------+-----------------------+----------+
|root.ln.wf02.wt02.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|{"description":"test1"}|      null|
+------------------------+-----+-------------+--------+--------+-----------+-----------------------+----------+
Total line number = 1
It costs 0.004s
```

> 注意，现在我们只支持一个查询条件，要么是等值条件查询，要么是包含条件查询。当然where子句中涉及的必须是标签值，而不能是属性值。

* SHOW TIMESERIES LIMIT INT OFFSET INT

  只返回从指定下标开始的结果，最大返回条数被 LIMIT 限制，用于分页查询

* SHOW LATEST TIMESERIES

  表示查询出的时间序列需要按照最近插入时间戳降序排列
  
需要注意的是，当查询路径不存在时，系统会返回0条时间序列。

#### 统计时间序列总数

IoTDB支持使用`COUNT TIMESERIES<Path>`来统计一条路径中的时间序列个数。SQL语句如下所示：
```
IoTDB > COUNT TIMESERIES root
IoTDB > COUNT TIMESERIES root.ln
IoTDB > COUNT TIMESERIES root.ln.*.*.status
IoTDB > COUNT TIMESERIES root.ln.wf01.wt01.status
```

除此之外，还可以通过定义`LEVEL`来统计指定层级下的时间序列个数。这条语句可以用来统计每一个设备下的传感器数量，语法为：`COUNT TIMESERIES <Path> GROUP BY LEVEL=<INTEGER>`。

例如有如下时间序列（可以使用`show timeseries`展示所有时间序列）：

```
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
|                     timeseries|   alias|storage group|dataType|encoding|compression|                                       tags|                                              attributes|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
|root.sgcc.wf03.wt01.temperature|    null|    root.sgcc|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|
|     root.sgcc.wf03.wt01.status|    null|    root.sgcc| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
|             root.turbine.d1.s1|newAlias| root.turbine|   FLOAT|     RLE|     SNAPPY|{"newTag1":"newV1","tag4":"v4","tag3":"v3"}|{"attr2":"v2","attr1":"newV1","attr4":"v4","attr3":"v3"}|
|     root.ln.wf02.wt02.hardware|    null|      root.ln|    TEXT|   PLAIN|     SNAPPY|                               {"unit":"c"}|                                                    null|
|       root.ln.wf02.wt02.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                    {"description":"test1"}|                                                    null|
|  root.ln.wf01.wt01.temperature|    null|      root.ln|   FLOAT|     RLE|     SNAPPY|                                       null|                                                    null|
|       root.ln.wf01.wt01.status|    null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|                                       null|                                                    null|
+-------------------------------+--------+-------------+--------+--------+-----------+-------------------------------------------+--------------------------------------------------------+
Total line number = 7
It costs 0.004s
```

那么Metadata Tree如下所示：

<img style="width:100%; max-width:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/69792176-1718f400-1201-11ea-861a-1a83c07ca144.jpg">

可以看到，`root`被定义为`LEVEL=0`。那么当你输入如下语句时：

```
IoTDB > COUNT TIMESERIES root GROUP BY LEVEL=1
IoTDB > COUNT TIMESERIES root.ln GROUP BY LEVEL=2
IoTDB > COUNT TIMESERIES root.ln.wf01 GROUP BY LEVEL=2
```

你将得到以下结果：

```
IoTDB> COUNT TIMESERIES root GROUP BY LEVEL=1
+---------+-----+
|   column|count|
+---------+-----+
|  root.ln|    4|
|root.sgcc|    2|
+---------+-----+
Total line number = 2
It costs 0.103s
IoTDB > COUNT TIMESERIES root.ln GROUP BY LEVEL=2
+--------------+-----+
|        column|count|
+--------------+-----+
|  root.ln.wf02|    1|
|  root.ln.wf01|    3|
+--------------+-----+
Total line number = 2
It costs 0.003s
IoTDB > COUNT TIMESERIES root.ln.wf01 GROUP BY LEVEL=2
+--------------+-----+
|        column|count|
+--------------+-----+
|  root.ln.wf01|    4|
+--------------+-----+
Total line number = 1
It costs 0.001s
```

> 注意：时间序列的路径只是过滤条件，与level的定义无关。

#### 标签点管理

我们可以在创建时间序列的时候，为它添加别名和额外的标签和属性信息。
所用到的扩展的创建时间序列的SQL语句如下所示：
```
create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)
```

括号里的`temprature`是`s1`这个传感器的别名。
我们可以在任何用到`s1`的地方，将其用`temprature`代替，这两者是等价的。

> IoTDB 同时支持在查询语句中[使用AS函数](../Appendix/DML-Data-Manipulation%20Language.md)设置别名。二者的区别在于：AS 函数设置的别名用于替代整条时间序列名，且是临时的，不与时间序列绑定；而上文中的别名只作为传感器的别名，与其绑定且可与原传感器名等价使用。

标签和属性的唯一差别在于，我们为标签信息在内存中维护了一个倒排索引，所以可以在`show timeseries`的条件语句中使用标签作为查询条件，你将会在下一节看到具体查询内容。

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

### 节点管理

#### 查看子路径

```
SHOW CHILD PATHS prefixPath
```

可以查看此前缀路径的下一层的所有路径，前缀路径允许使用 * 通配符。

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

#### 查看子节点

```
SHOW CHILD NODES prefixPath
```

可以查看此前缀路径的下一层的所有节点。

示例：

* 查询 root 的下一层：show child nodes root

```
+------------+
| child nodes|
+------------+
|          ln|
+------------+
```

* 查询 root.vehicle的下一层 ：show child nodes root.ln

```
+------------+
| child nodes|
+------------+
|        wf01|
|        wf02|
+------------+
```

#### 统计节点数

IoTDB支持使用`COUNT NODES <PrefixPath> LEVEL=<INTEGER>`来统计当前Metadata树下指定层级的节点个数，这条语句可以用来统计设备数。例如：

```
IoTDB > COUNT NODES root LEVEL=2
IoTDB > COUNT NODES root.ln LEVEL=2
IoTDB > COUNT NODES root.ln.wf01 LEVEL=3
```

对于上面提到的例子和Metadata Tree，你可以获得如下结果：

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
```

> 注意：时间序列的路径只是过滤条件，与level的定义无关。
其中`PrefixPath`可以包含`*`，但是`*`及其后的所有节点将被忽略，仅在`*`前的前缀路径有效。


#### 查看设备

* SHOW DEVICES prefixPath? (WITH STORAGE GROUP)? limitClause? #showDevices

与 `Show Timeseries` 相似，IoTDB 目前也支持两种方式查看设备。

* `SHOW DEVICES` 语句显示当前所有的设备信息，等价于 `SHOW DEVICES root`。
* `SHOW DEVICES <PrefixPath>` 语句规定了 `PrefixPath`，返回在给定的前缀路径下的设备信息。

SQL语句如下所示：

```
IoTDB> show devices
IoTDB> show devices root.ln
```

你可以获得如下数据：

```
+-------------------+
|            devices|
+-------------------+
|  root.ln.wf01.wt01|
|  root.ln.wf02.wt02|
|root.sgcc.wf03.wt01|
|    root.turbine.d1|
+-------------------+
Total line number = 4
It costs 0.002s

+-----------------+
|          devices|
+-----------------+
|root.ln.wf01.wt01|
|root.ln.wf02.wt02|
+-----------------+
Total line number = 2
It costs 0.001s
```

查看设备及其存储组信息，可以使用 `SHOW DEVICES WITH STORAGE GROUP` 语句。

* `SHOW DEVICES WITH STORAGE GROUP` 语句显示当前所有的设备信息和其所在的存储组，等价于 `SHOW DEVICES root`。
* `SHOW DEVICES <PrefixPath> WITH STORAGE GROUP` 语句规定了 `PrefixPath`，返回在给定的前缀路径下的设备信息和其所在的存储组。

SQL语句如下所示：

```
IoTDB> show devices with storage group
IoTDB> show devices root.ln with storage group
```

你可以获得如下数据：

```
+-------------------+-------------+
|            devices|storage group|
+-------------------+-------------+
|  root.ln.wf01.wt01|      root.ln|
|  root.ln.wf02.wt02|      root.ln|
|root.sgcc.wf03.wt01|    root.sgcc|
|    root.turbine.d1| root.turbine|
+-------------------+-------------+
Total line number = 4
It costs 0.003s

+-----------------+-------------+
|          devices|storage group|
+-----------------+-------------+
|root.ln.wf01.wt01|      root.ln|
|root.ln.wf02.wt02|      root.ln|
+-----------------+-------------+
Total line number = 2
It costs 0.001s
```

### 数据存活时间（TTL）

IoTDB支持对存储组级别设置数据存活时间（TTL），这使得IoTDB可以定期、自动地删除一定时间之前的数据。合理使用TTL
可以帮助您控制IoTDB占用的总磁盘空间以避免出现磁盘写满等异常。并且，随着文件数量的增多，查询性能往往随之下降,
内存占用也会有所提高。及时地删除一些较老的文件有助于使查询性能维持在一个较高的水平和减少内存资源的占用。

#### 设置 TTL

设置TTL的SQL语句如下所示：
```
IoTDB> set ttl to root.ln 3600000
```
这个例子表示在`root.ln`存储组中，只有最近一个小时的数据将会保存，旧数据会被移除或不可见。

#### 取消 TTL

取消TTL的SQL语句如下所示：

```
IoTDB> unset ttl to root.ln
```

取消设置TTL后，存储组`root.ln`中所有的数据都会被保存。

#### 显示 TTL

显示TTL的SQL语句如下所示：

```
IoTDB> SHOW ALL TTL
IoTDB> SHOW TTL ON StorageGroupNames
```

SHOW ALL TTL这个例子会给出所有存储组的TTL。
SHOW TTL ON root.group1,root.group2,root.group3这个例子会显示指定的三个存储组的TTL。
注意: 没有设置TTL的存储组的TTL将显示为null。


