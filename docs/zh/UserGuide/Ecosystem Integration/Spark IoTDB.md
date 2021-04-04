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

## Spark-IoTDB

### 版本

Spark和Java所需的版本如下：

| Spark Version | Scala Version | Java Version | TsFile   |
| ------------- | ------------- | ------------ | -------- |
| `2.4.3`       | `2.11`        | `1.8`        | `0.12.0` |

### 安装

mvn clean scala:compile compile install

#### Maven依赖

```
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>spark-iotdb-connector</artifactId>
      <version>0.10.0</version>
    </dependency>
```

#### Spark-shell用户指南

```
spark-shell --jars spark-iotdb-connector-0.12.0.jar,iotdb-jdbc-0.12.0-jar-with-dependencies.jar

import org.apache.iotdb.spark.db._

val df = spark.read.format("org.apache.iotdb.spark.db").option("url","jdbc:iotdb://127.0.0.1:6667/").option("sql","select * from root").load

df.printSchema()

df.show()
```

如果要对rdd进行分区，可以执行以下操作

```
spark-shell --jars spark-iotdb-connector-0.12.0.jar,iotdb-jdbc-0.12.0-jar-with-dependencies.jar

import org.apache.iotdb.spark.db._

val df = spark.read.format("org.apache.iotdb.spark.db").option("url","jdbc:iotdb://127.0.0.1:6667/").option("sql","select * from root").
                        option("lowerBound", [lower bound of time that you want query(include)]).option("upperBound", [upper bound of time that you want query(include)]).
                        option("numPartition", [the partition number you want]).load

df.printSchema()

df.show()
```

#### 模式推断

以下TsFile结构为例：TsFile模式中有三个度量：状态，温度和硬件。 这三种测量的基本信息如下：

|名称|类型|编码|
|--- |--- |--- |
|状态|Boolean|PLAIN|
|温度|Float|RLE|
|硬件|Text|PLAIN|

TsFile中的现有数据如下：

 * d1:root.ln.wf01.wt01
 * d2:root.ln.wf02.wt02

time|d1.status|time|d1.temperature |time	| d2.hardware	|time|d2.status
---- | ---- | ---- | ---- | ---- | ----  | ---- | ---- | ---- 
1|True	|1|2.2|2|"aaa"|1|True
3|True	|2|2.2|4|"bbb"|2|False
5|False|3	|2.1|6	|"ccc"|4|True


宽（默认）表形式如下：

| time | root.ln.wf02.wt02.temperature | root.ln.wf02.wt02.status | root.ln.wf02.wt02.hardware | root.ln.wf01.wt01.temperature | root.ln.wf01.wt01.status | root.ln.wf01.wt01.hardware |
| ---- | ----------------------------- | ------------------------ | -------------------------- | ----------------------------- | ------------------------ | -------------------------- |
| 1    | null                          | true                     | null                       | 2.2                           | true                     | null                       |
| 2    | null                          | false                    | aaa                        | 2.2                           | null                     | null                       |
| 3    | null                          | null                     | null                       | 2.1                           | true                     | null                       |
| 4    | null                          | true                     | bbb                        | null                          | null                     | null                       |
| 5    | null                          | null                     | null                       | null                          | false                    | null                       |
| 6    | null                          | null                     | ccc                        | null                          | null                     | null                       |

你还可以使用窄表形式，如下所示：（您可以参阅第4部分，了解如何使用窄表形式）

| 时间 | 设备名            | 状态  | 硬件 | 温度 |
| ---- | ----------------- | ----- | ---- | ---- |
| 1    | root.ln.wf02.wt01 | true  | null | 2.2  |
| 1    | root.ln.wf02.wt02 | true  | null | null |
| 2    | root.ln.wf02.wt01 | null  | null | 2.2  |
| 2    | root.ln.wf02.wt02 | false | aaa  | null |
| 3    | root.ln.wf02.wt01 | true  | null | 2.1  |
| 4    | root.ln.wf02.wt02 | true  | bbb  | null |
| 5    | root.ln.wf02.wt01 | false | null | null |
| 6    | root.ln.wf02.wt02 | null  | ccc  | null |

#### 在宽和窄表之间转换

   * 从宽到窄

```
import org.apache.iotdb.spark.db._

val wide_df = spark.read.format("org.apache.iotdb.spark.db").option("url", "jdbc:iotdb://127.0.0.1:6667/").option("sql", "select * from root where time < 1100 and time > 1000").load
val narrow_df = Transformer.toNarrowForm(spark, wide_df)
```

   * 从窄到宽

```
import org.apache.iotdb.spark.db._

val wide_df = Transformer.toWideForm(spark, narrow_df)
```

#### Java用户指南

```
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.iotdb.spark.db.*

public class Example {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("Build a DataFrame from Scratch")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("org.apache.iotdb.spark.db")
        .option("url","jdbc:iotdb://127.0.0.1:6667/")
        .option("sql","select * from root").load();

    df.printSchema();

    df.show();
    
    Dataset<Row> narrowTable = Transformer.toNarrowForm(spark, df)
    narrowTable.show()
  }
}
```
