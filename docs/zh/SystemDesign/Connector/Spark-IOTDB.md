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

# Spark IOTDB 连接器

## 设计目的

* 使用 Spark SQL 读取 IOTDB 的数据，以 Spark DataFrame 的形式返回给客户端

## 核心思想
由于 IOTDB 具有解析和执行 SQL 的能力，故该部分可以直接将 SQL 转发给 IOTDB 进程执行，将数据拿到后转换为 RDD 即可

## 执行流程
#### 1. 入口

* src/main/scala/org/apache/iotdb/spark/db/DefaultSource.scala

#### 2. 构建 Relation
Relation 主要保存了 RDD 的元信息，比如列名字，分区策略等，调用 Relation 的 buildScan 方法可以创建 RDD

* src/main/scala/org/apache/iotdb/spark/db/IoTDBRelation.scala

#### 3. 构建 RDD
RDD 中执行对 IOTDB 的 SQL 请求，保存游标

* src/main/scala/org/apache/iotdb/spark/db/IoTDBRDD.scala 中的 compute 方法

#### 4. 迭代 RDD
由于 Spark 懒加载机制，用户遍历 RDD 时才具体调用 RDD 的迭代，也就是 IOTDB 的 fetch Result

* src/main/scala/org/apache/iotdb/spark/db/IoTDBRDD.scala 中的 getNext 方法

## 宽窄表结构转换
宽表结构：IOTDB 原生路径格式

| time | root.ln.wf02.wt02.temperature | root.ln.wf02.wt02.status | root.ln.wf02.wt02.hardware | root.ln.wf01.wt01.temperature | root.ln.wf01.wt01.status | root.ln.wf01.wt01.hardware |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|--------------------------|----------------------------|
|    1 | null                          | true                     | null                       | 2.2                           | true                     | null                       |
|    2 | null                          | false                    | aaa                        | 2.2                           | null                     | null                       |
|    3 | null                          | null                     | null                       | 2.1                           | true                     | null                       |
|    4 | null                          | true                     | bbb                        | null                          | null                     | null                       |
|    5 | null                          | null                     | null                       | null                          | false                    | null                       |
|    6 | null                          | null                     | ccc                        | null                          | null                     | null                       |

窄表结构：关系型数据库模式，IOTDB align by device 格式

| time | device_name                   | status                   | hardware                   | temperature |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|
|    1 | root.ln.wf02.wt01             | true                     | null                       | 2.2                           | 
|    1 | root.ln.wf02.wt02             | true                     | null                       | null                          | 
|    2 | root.ln.wf02.wt01             | null                     | null                       | 2.2                          |                 
|    2 | root.ln.wf02.wt02             | false                    | aaa                        | null                           |                   
|    3 | root.ln.wf02.wt01             | true                     | null                       | 2.1                           |                 
|    4 | root.ln.wf02.wt02             | true                     | bbb                        | null                          |                  
|    5 | root.ln.wf02.wt01             | false                    | null                       | null                          |                   
|    6 | root.ln.wf02.wt02             | null                     | ccc                        | null                          |                   

由于 IOTDB 查询到的数据默认为宽表结构，所以需要宽窄表转换，有如下两个实现方法

#### 1. 使用 IOTDB 的 group by device 语句
这种方式可以直接拿到窄表结构，计算由 IOTDB 完成

#### 2. 使用 Transformer
可以使用 Transformer 进行宽窄表之间的转换，计算由 Spark 完成

* src/main/scala/org/apache/iotdb/spark/db/Transformer.scala

宽表转窄表使用了遍历 device 列表，生成对应的窄表，在 union 起来的策略，并行性较好（无 shuffle）

窄表转宽表使用了基于 timestamp 的 join 操作，有 shuffle，可能存在潜在的性能问题