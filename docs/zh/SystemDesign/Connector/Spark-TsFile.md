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

# Spark TsFile 连接器

## 设计目的

* 使用 Spark SQL 读取指定 TsFile 的数据，以 Spark DataFrame 的形式返回给客户端

* 使用 Spark DataFrame 中的数据生成 TsFile

## 支持格式
宽表结构：TsFile 原生格式，IoTDB 原生路径格式

| time | root.ln.wf02.wt02.temperature | root.ln.wf02.wt02.status | root.ln.wf02.wt02.hardware | root.ln.wf01.wt01.temperature | root.ln.wf01.wt01.status | root.ln.wf01.wt01.hardware |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|--------------------------|----------------------------|
|    1 | null                          | true                     | null                       | 2.2                           | true                     | null                       |
|    2 | null                          | false                    | aaa                        | 2.2                           | null                     | null                       |
|    3 | null                          | null                     | null                       | 2.1                           | true                     | null                       |
|    4 | null                          | true                     | bbb                        | null                          | null                     | null                       |
|    5 | null                          | null                     | null                       | null                          | false                    | null                       |
|    6 | null                          | null                     | ccc                        | null                          | null                     | null                       |

窄表结构: 关系型数据库模式，IoTDB align by device格式

| time | device_name                   | status                   | hardware                   | temperature |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|
|    1 | root.ln.wf02.wt01             | true                     | null                       | 2.2                           |
|    1 | root.ln.wf02.wt02             | true                     | null                       | null                          |
|    2 | root.ln.wf02.wt01             | null                     | null                       | 2.2                           |
|    2 | root.ln.wf02.wt02             | false                    | aaa                        | null                          |
|    3 | root.ln.wf02.wt01             | true                     | null                       | 2.1                           |
|    4 | root.ln.wf02.wt02             | true                     | bbb                        | null                          |
|    5 | root.ln.wf02.wt01             | false                    | null                       | null                          |
|    6 | root.ln.wf02.wt02             | null                     | ccc                        | null                          |

## 查询流程步骤

#### 1. 表结构推断和生成

该步骤是为了使DataFrame的表结构与需要查询的 TsFile 的表结构匹配

主要逻辑在 src/main/scala/org/apache/iotdb/spark/tsfile/DefaultSource.scala 中的 inferSchema 函数

#### 2. SQL解析

该步骤目的是为了将用户 SQL 语句转化为 TsFile 原生的查询表达式

主要逻辑在 src/main/scala/org/apache/iotdb/spark/tsfile/DefaultSource.scala 中的 buildReader 函数

SQL解析分宽表结构与窄表结构

#### 3. 宽表结构

宽表结构的SQL解析主要逻辑在 src/main/scala/org/apache/iotdb/spark/tsfile/WideConverter.scala 中

该结构与 TsFile 原生查询结构基本相同，不需要特殊处理，直接将SQL语句转化为相应查询表达式即可

#### 4. 窄表结构

宽表结构的SQL解析主要逻辑在 src/main/scala/org/apache/iotdb/spark/tsfile/NarrowConverter.scala中

首先我们根据查询的schema确定要查询的时间序列，仅在tsfile中查询那些sql中存在的时间序列
```
requiredSchema.foreach((field: StructField) => {
  if (field.name != QueryConstant.RESERVED_TIME
    && field.name != NarrowConverter.DEVICE_NAME) {
    measurementNames += field.name
  }
})
```

SQL转化为表达式后，由于窄表结构与 TsFile 原生查询结构不同，需要先将表达式转化为与 device 有关的析取表达式
，才可以转化为对 TsFile 的查询，转化代码在src/main/java/org/apache/iotdb/spark/tsfile/qp中

例子：
```
select time, device_name, s1 from tsfile_table where time > 1588953600000 and time < 1589040000000 and device_name = 'root.group1.d1'
```
此时仅查询时间序列root.group1.d1.s1，条件表达式为[time > 1588953600000] and [time < 1589040000000]

#### 5. 查询实际执行

实际数据查询执行由 TsFile 原生组件完成，参见：

* [Tsfile原生查询流程](../TsFile/Read.md)

## 写入步骤流程

写入主要是将 Dataframe 结构中的数据转化为 TsFile 的 RowRecord，使用 TsFile Writer 进行写入

#### 宽表结构

其主要转化代码在如下两个文件中：

* src/main/scala/org/apache/iotdb/spark/tsfile/WideConverter.scala 负责结构转化

* src/main/scala/org/apache/iotdb/spark/tsfile/WideTsFileOutputWriter.scala 负责匹配 spark 接口与执行写入，会调用上一个文件中的结构转化功能

#### 窄表结构

其主要转化代码在如下两个文件中：

* src/main/scala/org/apache/iotdb/spark/tsfile/NarrowConverter.scala 负责结构转化

* src/main/scala/org/apache/iotdb/spark/tsfile/NarrowTsFileOutputWriter.scala 负责匹配 spark 接口与执行写入，会调用上一个文件中的结构转化功能

