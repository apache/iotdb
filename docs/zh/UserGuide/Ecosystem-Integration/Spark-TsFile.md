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

## Spark-TsFile

### About TsFile-Spark-Connector

TsFile-Spark-Connector 对 Tsfile 类型的外部数据源实现 Spark 的支持。 这使用户可以通过 Spark 读取，写入和查询 Tsfile。

使用此连接器，您可以

- 从本地文件系统或 hdfs 加载单个 TsFile 到 Spark
- 将本地文件系统或 hdfs 中特定目录中的所有文件加载到 Spark 中
- 将数据从 Spark 写入 TsFile

### System Requirements

| Spark Version | Scala Version | Java Version | TsFile   |
| ------------- | ------------- | ------------ | -------- |
| `2.4.3`       | `2.11.8`      | `1.8`        | `1.0.0` |

> 注意：有关如何下载和使用 TsFile 的更多信息，请参见以下链接：https://github.com/apache/iotdb/tree/master/tsfile
> 注意：spark 版本目前仅支持 2.4.3, 其他版本可能存在不适配的问题，目前已知 2.4.7 的版本存在不适配的问题

### 快速开始

#### 本地模式

在本地模式下使用 TsFile-Spark-Connector 启动 Spark：

```
./<spark-shell-path>  --jars  tsfile-spark-connector.jar,tsfile-{version}-jar-with-dependencies.jar,hadoop-tsfile-{version}-jar-with-dependencies.jar
```

- \<spark-shell-path\>是您的 spark-shell 的真实路径。
- 多个 jar 包用逗号分隔，没有任何空格。
- 有关如何获取 TsFile 的信息，请参见 https://github.com/apache/iotdb/tree/master/tsfile。
- 获取到 dependency 包：```mvn clean package -DskipTests -P get-jar-with-dependencies```

#### 分布式模式

在分布式模式下使用 TsFile-Spark-Connector 启动 Spark（即，Spark 集群通过 spark-shell 连接）：

```
. /<spark-shell-path>   --jars  tsfile-spark-connector.jar,tsfile-{version}-jar-with-dependencies.jar,hadoop-tsfile-{version}-jar-with-dependencies.jar  --master spark://ip:7077
```

注意：

- \<spark-shell-path\>是您的 spark-shell 的真实路径。
- 多个 jar 包用逗号分隔，没有任何空格。
- 有关如何获取 TsFile 的信息，请参见 https://github.com/apache/iotdb/tree/master/tsfile。

### 数据类型对应

| TsFile 数据类型 | SparkSQL 数据类型 |
| -------------- | ---------------- |
| BOOLEAN        | BooleanType      |
| INT32          | IntegerType      |
| INT64          | LongType         |
| FLOAT          | FloatType        |
| DOUBLE         | DoubleType       |
| TEXT           | StringType       |

### 模式推断

显示 TsFile 的方式取决于架构。 以以下 TsFile 结构为例：TsFile 模式中有三个度量：状态，温度和硬件。 这三种测量的基本信息如下：

|名称 | 类型 | 编码 |
| ---- | ---- | ---- | 
|状态 | Boolean|PLAIN|
|温度 | Float|RLE|
|硬件 |Text|PLAIN|

TsFile 中的现有数据如下：

 * d1:root.ln.wf01.wt01
 * d2:root.ln.wf02.wt02

| time  | d1.status | time  | d1.temperature | time  | d2.hardware | time  | d2.status |
| :---- | :----     | :---- | :----          | :---- | :----       | :---- | :----     |
|   1   | True	    |   1   |       2.2      |   2   |    "aaa"    |   1   |    True   |
|   3   | True	    |   2   |       2.2      |   4   |    "bbb"    |   2   |    False  |
|   5   | False     |   3	|       2.1      |   6	 |    "ccc"    |   4   |    True   |

相应的 SparkSQL 表如下：

| time | root.ln.wf02.wt02.temperature | root.ln.wf02.wt02.status | root.ln.wf02.wt02.hardware | root.ln.wf01.wt01.temperature | root.ln.wf01.wt01.status | root.ln.wf01.wt01.hardware |
| ---- | ----------------------------- | ------------------------ | -------------------------- | ----------------------------- | ------------------------ | -------------------------- |
| 1    | null                          | true                     | null                       | 2.2                           | true                     | null                       |
| 2    | null                          | false                    | aaa                        | 2.2                           | null                     | null                       |
| 3    | null                          | null                     | null                       | 2.1                           | true                     | null                       |
| 4    | null                          | true                     | bbb                        | null                          | null                     | null                       |
| 5    | null                          | null                     | null                       | null                          | false                    | null                       |
| 6    | null                          | null                     | ccc                        | null                          | null                     | null                       |

您还可以使用如下所示的窄表形式：（您可以参阅第 6 部分，了解如何使用窄表形式）

| time | device_name       | status | hardware | temperature |
| ---- | ----------------- | ------ | -------- | ----------- |
| 1    | root.ln.wf02.wt01 | true   | null     | 2.2         |
| 1    | root.ln.wf02.wt02 | true   | null     | null        |
| 2    | root.ln.wf02.wt01 | null   | null     | 2.2         |
| 2    | root.ln.wf02.wt02 | false  | aaa      | null        |
| 3    | root.ln.wf02.wt01 | true   | null     | 2.1         |
| 4    | root.ln.wf02.wt02 | true   | bbb      | null        |
| 5    | root.ln.wf02.wt01 | false  | null     | null        |
| 6    | root.ln.wf02.wt02 | null   | ccc      | null        |

### Scala API

注意：请记住预先分配必要的读写权限。

 * 示例 1：从本地文件系统读取

```scala
import org.apache.iotdb.spark.tsfile._
val wide_df = spark.read.tsfile("test.tsfile")  
wide_df.show

val narrow_df = spark.read.tsfile("test.tsfile", true)  
narrow_df.show
```

 * 示例 2：从 hadoop 文件系统读取

```scala
import org.apache.iotdb.spark.tsfile._
val wide_df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
wide_df.show

val narrow_df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true)  
narrow_df.show
```

 * 示例 3：从特定目录读取

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/usr/hadoop") 
df.show
```

注 1：现在不支持目录中所有 TsFile 的全局时间排序。

注 2：具有相同名称的度量应具有相同的架构。

 * 示例 4：广泛形式的查询

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
df.createOrReplaceTempView("tsfile_table")
val newDf = spark.sql("select * from tsfile_table where `device_1.sensor_1`>0 and `device_1.sensor_2` < 22")
newDf.show
```

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
df.createOrReplaceTempView("tsfile_table")
val newDf = spark.sql("select count(*) from tsfile_table")
newDf.show
```

 * 示例 5：缩小形式的查询

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true) 
df.createOrReplaceTempView("tsfile_table")
val newDf = spark.sql("select * from tsfile_table where device_name = 'root.ln.wf02.wt02' and temperature > 5")
newDf.show
```

```scala
import org.apache.iotdb.spark.tsfile._
val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true) 
df.createOrReplaceTempView("tsfile_table")
val newDf = spark.sql("select count(*) from tsfile_table")
newDf.show
```

 * 例 6：写宽格式

```scala
// we only support wide_form table to write
import org.apache.iotdb.spark.tsfile._

val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile") 
df.show
df.write.tsfile("hdfs://localhost:9000/output")

val newDf = spark.read.tsfile("hdfs://localhost:9000/output")
newDf.show
```

 * 例 7：写窄格式

```scala
// we only support wide_form table to write
import org.apache.iotdb.spark.tsfile._

val df = spark.read.tsfile("hdfs://localhost:9000/test.tsfile", true) 
df.show
df.write.tsfile("hdfs://localhost:9000/output", true)

val newDf = spark.read.tsfile("hdfs://localhost:9000/output", true)
newDf.show
```

附录 A：模式推断的旧设计

显示 TsFile 的方式与 TsFile Schema 有关。 以以下 TsFile 结构为例：TsFile 架构中有三个度量：状态，温度和硬件。 这三个度量的基本信息如下：

|名称 | 类型 | 编码 |
| ---- | ---- | ---- | 
|状态 | Boolean|PLAIN|
|温度 | Float|RLE|
|硬件|Text|PLAIN|

文件中的现有数据如下：

 * delta_object1: root.ln.wf01.wt01
 * delta_object2: root.ln.wf02.wt02
 * delta_object3: :root.sgcc.wf03.wt01

| time | delta_object1.status | time | delta_object1.temperature | time | delta_object2.hardware | time | delta_object2.status | time | delta_object3.status | time | delta_object3.temperature | 
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
|   1   |  True	|   1   |  2.2  |   2   | "aaa" |   1   | True  |   2   | True  |   3   |  3.3  |
|   3   |  True	|   2   |  2.2  |   4   | "bbb" |   2   | False |   3   | True  |   6   |  6.6  |
|   5   |  False|   3	|  2.1  |   6	| "ccc" |   4   | True  |   4   | True  |   8   |  8.8  |
|   7   |  True |   4   |  2.0  |   8   | "ddd" |   5   | False |   6   | True  |   9   |  9.9  |

有两种显示方法：

 * 默认方式

将创建两列来存储设备的完整路径：time（LongType）和 delta_object（StringType）。

- `time`：时间戳记，LongType
- `delta_object`：Delta_object ID，StringType

接下来，为每个度量创建一列以存储特定数据。 SparkSQL 表结构如下：

|time(LongType)|delta\_object(StringType)|status(BooleanType)|temperature(FloatType)|hardware(StringType)|
|--- |--- |--- |--- |--- |
|1|root.ln.wf01.wt01|True|2.2|null|
|1|root.ln.wf02.wt02|True|null|null|
|2|root.ln.wf01.wt01|null|2.2|null|
|2|root.ln.wf02.wt02|False|null|"aaa"|
|2|root.sgcc.wf03.wt01|True|null|null|
|3|root.ln.wf01.wt01|True|2.1|null|
|3|root.sgcc.wf03.wt01|True|3.3|null|
|4|root.ln.wf01.wt01|null|2.0|null|
|4|root.ln.wf02.wt02|True|null|"bbb"|
|4|root.sgcc.wf03.wt01|True|null|null|
|5|root.ln.wf01.wt01|False|null|null|
|5|root.ln.wf02.wt02|False|null|null|
|5|root.sgcc.wf03.wt01|True|null|null|
|6|root.ln.wf02.wt02|null|null|"ccc"|
|6|root.sgcc.wf03.wt01|null|6.6|null|
|7|root.ln.wf01.wt01|True|null|null|
|8|root.ln.wf02.wt02|null|null|"ddd"|
|8|root.sgcc.wf03.wt01|null|8.8|null|
|9|root.sgcc.wf03.wt01|null|9.9|null|

 * 展开 delta_object 列

通过“。”将设备列展开为多个列，忽略根目录“root”。方便进行更丰富的聚合操作。如果用户想使用这种显示方式，需要在表创建语句中设置参数“delta\_object\_name”（参考本手册 5.1 节中的示例 5)，在本例中，将参数“delta\_object\_name”设置为“root.device.turbine”。路径层的数量必须是一对一的。此时，除了“根”层之外，为设备路径的每一层创建一列。列名是参数中的名称，值是设备相应层的名称。接下来，将为每个度量创建一个列来存储特定的数据。

那么 SparkSQL 表结构如下：

|time(LongType)|group(StringType)|field(StringType)|device(StringType)|status(BooleanType)|temperature(FloatType)|hardware(StringType)|
|--- |--- |--- |--- |--- |--- |--- |
|1|ln|wf01|wt01|True|2.2|null|
|1|ln|wf02|wt02|True|null|null|
|2|ln|wf01|wt01|null|2.2|null|
|2|ln|wf02|wt02|False|null|"aaa"|
|2|sgcc|wf03|wt01|True|null|null|
|3|ln|wf01|wt01|True|2.1|null|
|3|sgcc|wf03|wt01|True|3.3|null|
|4|ln|wf01|wt01|null|2.0|null|
|4|ln|wf02|wt02|True|null|"bbb"|
|4|sgcc|wf03|wt01|True|null|null|
|5|ln|wf01|wt01|False|null|null|
|5|ln|wf02|wt02|False|null|null|
|5|sgcc|wf03|wt01|True|null|null|
|6|ln|wf02|wt02|null|null|"ccc"|
|6|sgcc|wf03|wt01|null|6.6|null|
|7|ln|wf01|wt01|True|null|null|
|8|ln|wf02|wt02|null|null|"ddd"|
|8|sgcc|wf03|wt01|null|8.8|null|
|9|sgcc|wf03|wt01|null|9.9|null|

TsFile-Spark-Connector 可以通过 SparkSQL 在 SparkSQL 中以表的形式显示一个或多个 tsfile。它还允许用户指定一个目录或使用通配符来匹配多个目录。如果有多个 tsfile，那么所有 tsfile 中的度量值的并集将保留在表中，并且具有相同名称的度量值在默认情况下具有相同的数据类型。注意，如果存在名称相同但数据类型不同的情况，TsFile-Spark-Connector 将不能保证结果的正确性。

写入过程是将数据 aframe 写入一个或多个 tsfile。默认情况下，需要包含两个列：time 和 delta_object。其余的列用作测量。如果用户希望将第二个表结构写回 TsFile，可以设置“delta\_object\_name”参数（请参阅本手册 5.1 节的 5.1 节）。

附录 B：旧注

注意：检查 Spark 根目录中的 jar 软件包，并将 libthrift-0.9.2.jar 和 libfb303-0.9.2.jar 分别替换为 libthrift-0.9.1.jar 和 libfb303-0.9.1.jar。
