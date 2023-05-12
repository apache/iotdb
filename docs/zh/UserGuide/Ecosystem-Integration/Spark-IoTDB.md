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

# Spark-IoTDB 用户手册

## 版本支持

支持的 Spark 与 Scala 版本如下：

| Spark 版本       | Scala 版本     |
|----------------|--------------|
| `2.4.0-latest` | `2.11, 2.12` |

## 注意事项

1. 当前版本的 `spark-iotdb-connector` 支持 `2.11` 与 `2.12` 两个版本的 Scala，暂不支持 `2.13` 版本。
2. `spark-iotdb-connector` 支持在 Java、Scala 版本的 Spark 与 PySpark 中使用。

## 部署

`spark-iotdb-connector` 总共有两个使用场景，分别为 IDE 开发与 spark-shell 调试。

### IDE 开发

在 IDE 开发时，只需要在 `pom.xml` 文件中添加以下依赖即可：

``` xml
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <!-- spark-iotdb-connector_2.11 or spark-iotdb-connector_2.13 -->
      <artifactId>spark-iotdb-connector_${scala.version}</artifactId>
      <version>${iotdb.version}</version>
    </dependency>
```

### `spark-shell` 调试

如果需要在 `spark-shell` 中使用 `spark-iotdb-connetcor`，需要先在官网下载 `with-dependencies` 版本的 jar 包。然后再将 Jar 包拷贝到 `${SPARK_HOME}/jars` 目录中即可。
执行以下命令即可：

```shell
cp spark-iotdb-connector_${scala.version}-${iotdb.version}.jar $SPARK_HOME/jars/
```

## 使用

### 参数

| 参数           | 描述                                             | 默认值  | 使用范围       | 能否为空  |
|--------------|------------------------------------------------|------|------------|-------|
| url          | 指定 IoTDB 的 JDBC 的 URL                          | null | read、write | false |
| user         | IoTDB 的用户名                                     | root | read、write | true  |
| password     | IoTDB 的密码                                      | root | read、write | true  |
| sql          | 用于指定查询的 SQL 语句                                 | null | read       | true  |
| numPartition | 在 read 中用于指定 DataFrame 的分区数，在 write 中用于设置写入并发数 | 1    | read、write | true  |
| lowerBound   | 查询的起始时间戳（包含）                                   | 0    | read       | true  |
| upperBound   | 查询的结束时间戳（包含）                                   | 0    | read       | true  |

### 从 IoTDB 读取数据

以下是一个示例，演示如何从 IoTDB 中读取数据成为 DataFrame。

```scala
import org.apache.iotdb.spark.db._

val df = spark.read.format("org.apache.iotdb.spark.db")
  .option("user", "root")
  .option("password", "root")
  .option("url", "jdbc:iotdb://127.0.0.1:6667/")
  .option("sql", "select ** from root") // 查询 SQL
  .option("lowerBound", "0") // 时间戳下界
  .option("upperBound", "100000000") // 时间戳上界
  .option("numPartition", "5") // 分区数
  .load

df.printSchema()

df.show()
```

### 将数据写入 IoTDB

以下是一个示例，演示如何将数据写入 IoTDB。

```scala
// 构造窄表数据
val df = spark.createDataFrame(List(
  (1L, "root.test.d0", 1, 1L, 1.0F, 1.0D, true, "hello"),
  (2L, "root.test.d0", 2, 2L, 2.0F, 2.0D, false, "world")))

val dfWithColumn = df.withColumnRenamed("_1", "Time")
  .withColumnRenamed("_2", "Device")
  .withColumnRenamed("_3", "s0")
  .withColumnRenamed("_4", "s1")
  .withColumnRenamed("_5", "s2")
  .withColumnRenamed("_6", "s3")
  .withColumnRenamed("_7", "s4")
  .withColumnRenamed("_8", "s5")

// 写入窄表数据
dfWithColumn
  .write
  .format("org.apache.iotdb.spark.db")
  .option("url", "jdbc:iotdb://127.0.0.1:6667/")
  .save

// 构造宽表数据
val df = spark.createDataFrame(List(
  (1L, 1, 1L, 1.0F, 1.0D, true, "hello"),
  (2L, 2, 2L, 2.0F, 2.0D, false, "world")))

val dfWithColumn = df.withColumnRenamed("_1", "Time")
  .withColumnRenamed("_2", "root.test.d0.s0")
  .withColumnRenamed("_3", "root.test.d0.s1")
  .withColumnRenamed("_4", "root.test.d0.s2")
  .withColumnRenamed("_5", "root.test.d0.s3")
  .withColumnRenamed("_6", "root.test.d0.s4")
  .withColumnRenamed("_7", "root.test.d0.s5")

// 写入宽表数据
dfWithColumn.write.format("org.apache.iotdb.spark.db")
  .option("url", "jdbc:iotdb://127.0.0.1:6667/")
  .option("numPartition", "10")
  .save
```

### 宽表与窄表转换

以下是如何转换宽表与窄表的示例：

* 从宽到窄

```scala
import org.apache.iotdb.spark.db._

val wide_df = spark.read.format("org.apache.iotdb.spark.db").option("url", "jdbc:iotdb://127.0.0.1:6667/").option("sql", "select * from root.** where time < 1100 and time > 1000").load
val narrow_df = Transformer.toNarrowForm(spark, wide_df)
```

* 从窄到宽

```scala
import org.apache.iotdb.spark.db._

val wide_df = Transformer.toWideForm(spark, narrow_df)
```

## 宽表与窄表

以下 TsFile 结构为例：TsFile 模式中有三个度量：状态，温度和硬件。 这三种测量的基本信息如下：

| 名称  | 类型      | 编码    |
|-----|---------|-------|
| 状态  | Boolean | PLAIN |
| 温度  | Float   | RLE   |
| 硬件  | Text    | PLAIN |

TsFile 中的现有数据如下：

* `d1:root.ln.wf01.wt01`
* `d2:root.ln.wf02.wt02`

| time | d1.status | time | d1.temperature | time | d2.hardware | time | d2.status |
|------|-----------|------|----------------|------|-------------|------|-----------|
| 1    | True      | 1    | 2.2            | 2    | "aaa"       | 1    | True      |
| 3    | True      | 2    | 2.2            | 4    | "bbb"       | 2    | False     |
| 5    | False     | 3    | 2.1            | 6    | "ccc"       | 4    | True      |

宽（默认）表形式如下：

| Time | root.ln.wf02.wt02.temperature | root.ln.wf02.wt02.status | root.ln.wf02.wt02.hardware | root.ln.wf01.wt01.temperature | root.ln.wf01.wt01.status | root.ln.wf01.wt01.hardware |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|--------------------------|----------------------------|
| 1    | null                          | true                     | null                       | 2.2                           | true                     | null                       |
| 2    | null                          | false                    | aaa                        | 2.2                           | null                     | null                       |
| 3    | null                          | null                     | null                       | 2.1                           | true                     | null                       |
| 4    | null                          | true                     | bbb                        | null                          | null                     | null                       |
| 5    | null                          | null                     | null                       | null                          | false                    | null                       |
| 6    | null                          | null                     | ccc                        | null                          | null                     | null                       |

你还可以使用窄表形式，如下所示：

| Time | Device            | status | hardware | temperature |
|------|-------------------|--------|----------|-------------|
| 1    | root.ln.wf02.wt01 | true   | null     | 2.2         |
| 1    | root.ln.wf02.wt02 | true   | null     | null        |
| 2    | root.ln.wf02.wt01 | null   | null     | 2.2         |
| 2    | root.ln.wf02.wt02 | false  | aaa      | null        |
| 3    | root.ln.wf02.wt01 | true   | null     | 2.1         |
| 4    | root.ln.wf02.wt02 | true   | bbb      | null        |
| 5    | root.ln.wf02.wt01 | false  | null     | null        |
| 6    | root.ln.wf02.wt02 | null   | ccc      | null        |
