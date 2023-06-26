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

# Spark-IoTDB User Guide

## Supported Versions

Supported versions of Spark and Scala are as follows:

| Spark Version  | Scala Version |
|----------------|---------------|
| `2.4.0-latest` | `2.11, 2.12`  |

## Precautions

1. The current version of `spark-iotdb-connector` supports Scala `2.11` and `2.12`, but not `2.13`.
2. `spark-iotdb-connector` supports usage in Spark for both Java, Scala, and PySpark.

## Deployment

`spark-iotdb-connector` has two use cases: IDE development and `spark-shell` debugging.

### IDE Development

For IDE development, simply add the following dependency to the `pom.xml` file:

``` xml
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <!-- spark-iotdb-connector_2.11 or spark-iotdb-connector_2.13 -->
      <artifactId>spark-iotdb-connector_${scala.version}</artifactId>
      <version>${iotdb.version}</version>
    </dependency>
```

### `spark-shell` Debugging

To use `spark-iotdb-connector` in `spark-shell`, you need to download the `with-dependencies` version of the jar package
from the official website. After that, copy the jar package to the `${SPARK_HOME}/jars` directory.
Simply execute the following command:

```shell
cp spark-iotdb-connector_${scala.version}-${iotdb.version}.jar $SPARK_HOME/jars/
```

## Usage

### Parameters

| Parameter    | Description                                                                                                  | Default Value | Scope       | Can be Empty |
|--------------|--------------------------------------------------------------------------------------------------------------|---------------|-------------|--------------|
| url          | Specifies the JDBC URL of IoTDB                                                                              | null          | read, write | false        |
| user         | The username of IoTDB                                                                                        | root          | read, write | true         |
| password     | The password of IoTDB                                                                                        | root          | read, write | true         |
| sql          | Specifies the SQL statement for querying                                                                     | null          | read        | true         |
| numPartition | Specifies the partition number of the DataFrame when in read, and the write concurrency number when in write | 1             | read, write | true         |
| lowerBound   | The start timestamp of the query (inclusive)                                                                 | 0             | read        | true         |
| upperBound   | The end timestamp of the query (inclusive)                                                                   | 0             | read        | true         |

### Reading Data from IoTDB

Here is an example that demonstrates how to read data from IoTDB into a DataFrame:

```scala
import org.apache.iotdb.spark.db._

val df = spark.read.format("org.apache.iotdb.spark.db")
  .option("user", "root")
  .option("password", "root")
  .option("url", "jdbc:iotdb://127.0.0.1:6667/")
  .option("sql", "select ** from root") // query SQL
  .option("lowerBound", "0") // lower timestamp bound
  .option("upperBound", "100000000") // upper timestamp bound
  .option("numPartition", "5") // number of partitions
  .load

df.printSchema()

df.show()
```

### Writing Data to IoTDB

Here is an example that demonstrates how to write data to IoTDB:

```scala
// Construct narrow table data
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

// Write narrow table data
dfWithColumn
  .write
  .format("org.apache.iotdb.spark.db")
  .option("url", "jdbc:iotdb://127.0.0.1:6667/")
  .save

// Construct wide table data
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

// Write wide table data
dfWithColumn.write.format("org.apache.iotdb.spark.db")
  .option("url", "jdbc:iotdb://127.0.0.1:6667/")
  .option("numPartition", "10")
  .save
```

### Wide and Narrow Table Conversion

Here are examples of how to convert between wide and narrow tables:

* From wide to narrow

```scala
import org.apache.iotdb.spark.db._

val wide_df = spark.read.format("org.apache.iotdb.spark.db").option("url", "jdbc:iotdb://127.0.0.1:6667/").option("sql", "select * from root.** where time < 1100 and time > 1000").load
val narrow_df = Transformer.toNarrowForm(spark, wide_df)
```

* From narrow to wide

```scala
import org.apache.iotdb.spark.db._

val wide_df = Transformer.toWideForm(spark, narrow_df)
```

## Wide and Narrow Tables

Using the TsFile structure as an example: there are three measurements in the TsFile pattern,
namely `Status`, `Temperature`, and `Hardware`. The basic information for each of these three measurements is as
follows:

| Name        | Type    | Encoding |
|-------------|---------|----------|
| Status      | Boolean | PLAIN    |
| Temperature | Float   | RLE      |
| Hardware    | Text    | PLAIN    |

The existing data in the TsFile is as follows:

* `d1:root.ln.wf01.wt01`
* `d2:root.ln.wf02.wt02`

| time | d1.status | time | d1.temperature | time | d2.hardware | time | d2.status |
|------|-----------|------|----------------|------|-------------|------|-----------|
| 1    | True      | 1    | 2.2            | 2    | "aaa"       | 1    | True      |
| 3    | True      | 2    | 2.2            | 4    | "bbb"       | 2    | False     |
| 5    | False     | 3    | 2.1            | 6    | "ccc"       | 4    | True      |

The wide (default) table form is as follows:

| Time | root.ln.wf02.wt02.temperature | root.ln.wf02.wt02.status | root.ln.wf02.wt02.hardware | root.ln.wf01.wt01.temperature | root.ln.wf01.wt01.status | root.ln.wf01.wt01.hardware |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|--------------------------|----------------------------|
| 1    | null                          | true                     | null                       | 2.2                           | true                     | null                       |
| 2    | null                          | false                    | aaa                        | 2.2                           | null                     | null                       |
| 3    | null                          | null                     | null                       | 2.1                           | true                     | null                       |
| 4    | null                          | true                     | bbb                        | null                          | null                     | null                       |
| 5    | null                          | null                     | null                       | null                          | false                    | null                       |
| 6    | null                          | null                     | ccc                        | null                          | null                     | null                       |

You can also use the narrow table format as shown below:

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